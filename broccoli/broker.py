import pickle
import redis
from typing import Union, List, Any
from broccoli.utils import cached_property
from broccoli.components import Component
from broccoli.types import Broker, Config, Message


__all__ = ('RedisBrokerComponent', )


class DecodeError(Exception):
    pass


class RedisBroker(Broker):

    NODE_PREFIX = b'node:'
    CONSUMER_PREFIX = b'consumer:'
    STATE_PREFIX = b'state:'
    QUEUE_PREFIX = b'queue:'
    RESULT_PREFIX = b'result:'
    GROUP_NAME = 'group'
    CONSUMERS = b'@consumers'
    QUEUES = b'@queues'

    def __init__(self,
                 broker_url: str,
                 gzip_min_length: int) -> None:
        self.broker_url = broker_url
        self.gzip_min_length = gzip_min_length
        self.consumer_id = None
        self.queues = None

    @cached_property
    def loads(self):
        _loads = pickle.loads
        if self.gzip_min_length > 0:
            from gzip import decompress

            def loads(data):
                try:
                    if data[0] == 0x1f:
                        data = decompress(data)
                    ret = _loads(data)
                except Exception as exc:
                    raise self.DecodeError(str(exc))
                if not isinstance(ret, Message):
                    try:
                        ret = Message(ret)
                    except Exception as exc:
                        raise self.DecodeError(str(exc))
                return ret
        else:
            def loads(data):
                try:
                    ret = _loads(data)
                except Exception as exc:
                    raise self.DecodeError(str(exc))
                if not isinstance(ret, Message):
                    try:
                        ret = Message(ret)
                    except Exception as exc:
                        raise self.DecodeError(str(exc))
                return ret
        return loads

    @cached_property
    def dumps(self):
        _dumps = pickle.dumps
        if self.gzip_min_length > 0:
            from gzip import compress
            gzip_min_length = self.gzip_min_length

            def dumps(data):
                data = _dumps(data, 4)
                if len(data) >= gzip_min_length:
                    data = compress(data)
                return data
        else:
            def dumps(data):
                return _dumps(data, 4)
        return dumps

    @cached_property
    def client(self):
        client = redis.Redis.from_url(self.broker_url)
        client.set_response_callback('CLIENT LIST', self._parse_client_list)
        return client

    @cached_property
    def DecodeError(self):
        return DecodeError

    @cached_property
    def BrokerError(self):
        return (redis.exceptions.ConnectionError,
                redis.exceptions.ResponseError,
                self.DecodeError)

    @staticmethod
    def _parse_client_list(response, **_options):
        clients = []
        for c in response.splitlines():
            clients.append(dict(pair.split(b'=', 1) for pair in c.split(b' ')))
        return clients

    @staticmethod
    def _encode(s: Union[bytes, str]) -> bytes:
        return s if isinstance(s, bytes) else s.encode('ascii')

    def set_node_id(self, node_id: str):
        self.client.client_setname(self.NODE_PREFIX + self._encode(node_id))

    def get_nodes(self):
        prefix = self.NODE_PREFIX
        prefix_len = len(prefix)
        return [
            (int(c[b'age']), c[b'name'][prefix_len:].decode('ascii'))
            for c in self.client.client_list()
            if c[b'name'].startswith(prefix)
        ]

    def get_consumers(self):
        prefix = self.CONSUMER_PREFIX
        prefix_len = len(prefix)
        return [
            c[b'name'][prefix_len:]
            for c in self.client.client_list()
            if c[b'name'].startswith(prefix)
        ]

    def run_gc(self, verbose=False):
        died_consumers = self.client.smembers(self.CONSUMERS) - set(self.get_consumers())
        if not died_consumers:
            return
        if verbose:
            done = []
        else:
            done = None
        p = self.client.pipeline()
        prefix = self.QUEUE_PREFIX
        for queue in self.client.smembers(self.QUEUES):
            queue = prefix + queue
            for consumer in self.client.xinfo_consumers(queue, self.GROUP_NAME):
                if consumer['name'] not in died_consumers:
                    continue
                if consumer['pending']:
                    items = self.client.xreadgroup(self.GROUP_NAME, consumer['name'], {queue: '0-0'},
                                                   count=consumer['pending'])
                    for stream, messages in items:
                        ids = []
                        for msg_id, data in messages:
                            reply_key = b'reply'
                            if reply_key in data and data[reply_key] in died_consumers:
                                if verbose:
                                    done.append(('Reject message %s', data[b'id'].decode('ascii')))
                                continue
                            ids.append(msg_id)
                            p.xadd(stream, data)
                            if verbose:
                                done.append(('Restart message %s', data[b'id'].decode('ascii')))
                        if ids:
                            p.xack(stream, self.GROUP_NAME, *ids)
                            p.xdel(stream, *ids)
                p.xgroup_delconsumer(queue, self.GROUP_NAME, consumer['name'])
                p.execute()

        consumer_prefix = self.CONSUMER_PREFIX
        p.delete(*[consumer_prefix + c for c in died_consumers])
        p.srem(self.CONSUMERS, *died_consumers)
        p.execute()
        if verbose:
            done += [('Remove died consumer %s', c) for c in died_consumers]
        return done

    def setup(self, consumer_id: str, queues: List[str]):
        queues = [self._encode(q) for q in queues]
        for q in queues:
            self.create_queue(self.QUEUE_PREFIX + q)
        self.queues = {self.QUEUE_PREFIX + q: b'>' for q in queues}
        self.consumer_id = self._encode(consumer_id)
        consumer = self.CONSUMER_PREFIX + self.consumer_id
        self.create_queue(consumer)
        self.queues[consumer] = b'>'
        p = self.client.pipeline()
        p.client_setname(consumer)
        p.sadd(self.QUEUES, *queues)
        p.sadd(self.CONSUMERS, self.consumer_id)
        p.execute()

    def close(self):
        pass

    def create_queue(self, name):
        try:
            self.client.xgroup_create(name, self.GROUP_NAME, mkstream=True)
        except redis.ResponseError as exc:
            if not str(exc).startswith('BUSYGROUP'):
                raise

    def get_messages(self, timeout: int = 0, prefetch: int = 1):
        assert self.consumer_id and self.queues, "call setup() first."
        timeout = timeout * 1000
        data = self.client.xreadgroup(self.GROUP_NAME, self.consumer_id, self.queues,
                                      block=timeout, count=prefetch)
        return [
            ((queue, key), self.loads(message[b'data']))
            for queue, messages in data for key, message in messages
        ]

    def ack_message(self, key):
        queue, key = key
        p = self.client.pipeline()
        p.xack(queue, self.GROUP_NAME, key)
        p.xdel(queue, key)
        p.execute()

    def send_message(self, queue: str, message: dict, reply_back: bool = False):
        data = {b'id': self._encode(message['id'])}
        if reply_back:
            data[b'reply'] = self.consumer_id
            message['reply_to'] = self.consumer_id.decode('ascii')
        data[b'data'] = self.dumps(message)
        self.client.xadd(self.QUEUE_PREFIX + self._encode(queue), data)

    def send_reply(self, consumer: str, message: dict):
        data = {b'data': self.dumps(message), b'id': self._encode(message['id'])}
        p = self.client.pipeline()
        consumer = self._encode(consumer)
        p.sadd(self.CONSUMERS, consumer)
        p.xadd(self.CONSUMER_PREFIX + consumer, data)
        p.execute()

    def set_result(self, result_key: str, message: dict, expires_in: int):
        key = self.RESULT_PREFIX + self._encode(result_key)
        p = self.client.pipeline()
        p.rpush(key, self.dumps(message))
        p.expire(key, expires_in)
        p.execute()

    def get_result(self, result_key: str, timeout: int = None):
        key = self.RESULT_PREFIX + self._encode(result_key)
        result = self.client.blpop([key], timeout=timeout)
        if result is None:
            return
        return self.loads(result[1])

    def set_state(self, task_id: str, state: Any):
        self.client.set(self.STATE_PREFIX + self._encode(task_id), self.dumps(state))


class RedisBrokerComponent(Component):

    singleton = True

    def __init__(self,
                 broker_url: str = 'redis://localhost',
                 gzip_min_length: int = 0):
        self.broker_url = broker_url
        self.gzip_min_length = gzip_min_length

    # noinspection PyMethodOverriding
    def resolve(self, config: Config) -> Broker:
        broker_url = config.get('broker_url', self.broker_url)
        gzip_min_length = config.get('gzip_min_length', self.gzip_min_length)
        return RedisBroker(broker_url=broker_url,
                           gzip_min_length=gzip_min_length)
