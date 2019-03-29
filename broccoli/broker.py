import pickle
from logging import Logger
from typing import Union, List, Callable, ByteString
from broccoli.utils import cached_property
from broccoli.components import Component
from broccoli.types import Broker, Router, Config, LoggerService


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
                 gzip_min_length: int,
                 router: Router,
                 logger: Logger) -> None:
        self._broker_url = broker_url
        self._gzip_min_length = gzip_min_length
        self._consumer_id = None
        self._queues = None
        self._router = router
        self._logger = logger

    @cached_property
    def BrokerError(self):
        import redis
        return (redis.exceptions.ConnectionError,
                redis.exceptions.ResponseError)

    def set_node_id(self, node_id: str):
        self._client.client_setname(self.NODE_PREFIX + self._encode(node_id))

    def get_nodes(self):
        prefix = self.NODE_PREFIX
        prefix_len = len(prefix)
        return [
            (int(c[b'age']), c[b'name'][prefix_len:].decode('ascii'))
            for c in self._client.client_list()
            if c[b'name'].startswith(prefix)
        ]

    def setup(self, consumer_id: str, queues: List[str]):
        queues = [self._encode(q) for q in queues]
        for q in queues:
            self._create_queue(self.QUEUE_PREFIX + q)
        self._queues = {self.QUEUE_PREFIX + q: b'>' for q in queues}
        self._consumer_id = self._encode(consumer_id)
        consumer = self.CONSUMER_PREFIX + self._consumer_id
        self._create_queue(consumer)
        self._queues[consumer] = b'>'
        p = self._client.pipeline()
        p.client_setname(consumer)
        p.sadd(self.QUEUES, *queues)
        p.sadd(self.CONSUMERS, self._consumer_id)
        p.execute()

    def close(self):
        pass

    def get_messages(self, timeout: int = 0):
        assert self._consumer_id and self._queues, "call setup() first."
        timeout = int(timeout * 1000)
        data = self._client.xreadgroup(self.GROUP_NAME, self._consumer_id, self._queues,
                                       block=timeout, count=1)
        result = []
        if data:
            for queue, messages in data:
                for key, message in messages:
                    ack_key = (queue, key)
                    message = self._loads(message[b'data'])
                    if message is None:
                        self.ack(ack_key)
                        continue
                    result.append((ack_key, message))
        return result

    def ack(self, key):
        queue, key = key
        p = self._client.pipeline()
        p.xack(queue, self.GROUP_NAME, key)
        p.xdel(queue, key)
        p.execute()

    def send_message(self, message: dict, reply_back: bool = False):
        data = {b'id': self._encode(message['id'])}
        if reply_back:
            data[b'reply'] = self._consumer_id
            message['reply_to'] = self._consumer_id.decode('ascii')
        queue = message.get('queue') or self._router.get_queue(message['task'])
        data[b'data'] = self._dumps(message)
        self._client.xadd(self.QUEUE_PREFIX + self._encode(queue), data)

    def send_reply(self, consumer: str, message: dict):
        data = {b'data': self._dumps(message), b'id': self._encode(message['id'])}
        p = self._client.pipeline()
        consumer = self._encode(consumer)
        p.sadd(self.CONSUMERS, consumer)
        p.xadd(self.CONSUMER_PREFIX + consumer, data)
        p.execute()

    def set_result(self, result_key: str, result: dict, expires_in: int):
        key = self.RESULT_PREFIX + self._encode(result_key)
        p = self._client.pipeline()
        p.rpush(key, self._dumps(result))
        p.expire(key, expires_in)
        p.execute()

    def get_result(self, result_key: str, timeout: int = 0):
        key = self.RESULT_PREFIX + self._encode(result_key)
        result = self._client.blpop([key], timeout=timeout)
        if result is None:
            return
        return self._loads(result[1])

    def set_state(self, task_id: str, state: dict):
        self._client.set(self.STATE_PREFIX + self._encode(task_id), self._dumps(state))

    def run_gc(self):
        died_consumers = self._client.smembers(self.CONSUMERS) - set(self._get_consumers())
        if not died_consumers:
            return
        p = self._client.pipeline()
        prefix = self.QUEUE_PREFIX
        for queue in self._client.smembers(self.QUEUES):
            queue = prefix + queue
            for consumer in self._client.xinfo_consumers(queue, self.GROUP_NAME):
                if consumer['name'] not in died_consumers:
                    continue
                if consumer['pending']:
                    items = self._client.xreadgroup(self.GROUP_NAME, consumer['name'], {queue: '0-0'},
                                                    count=consumer['pending'])
                    for stream, messages in items:
                        ids = []
                        for msg_id, data in messages:
                            reply_key = b'reply'
                            if reply_key in data and data[reply_key] in died_consumers:
                                self._logger.debug('Reject message %s', data[b'id'].decode('ascii'))
                                continue
                            self._logger.debug('Restart message %s', data[b'id'].decode('ascii'))
                            ids.append(msg_id)
                            p.xadd(stream, data)
                        if ids:
                            p.xack(stream, self.GROUP_NAME, *ids)
                            p.xdel(stream, *ids)
                p.xgroup_delconsumer(queue, self.GROUP_NAME, consumer['name'])
                p.execute()

        consumer_prefix = self.CONSUMER_PREFIX
        self._logger.debug('Remove died consumer %s', died_consumers)
        p.delete(*[consumer_prefix + c for c in died_consumers])
        p.srem(self.CONSUMERS, *died_consumers)
        p.execute()

    @cached_property
    def _loads(self) -> Callable[[ByteString], dict]:
        _loads = pickle.loads
        if self._gzip_min_length > 0:
            from gzip import decompress

            def loads(data):
                try:
                    if data[0] == 0x1f:
                        data = decompress(data)
                    return dict(_loads(data))
                except Exception as exc:
                    self._logger.error('Received invalid message: %s', str(exc))
        else:
            def loads(data):
                try:
                    return dict(_loads(data))
                except Exception as exc:
                    self._logger.error('Received invalid message: %s', str(exc))
        return loads

    @cached_property
    def _dumps(self):
        _dumps = pickle.dumps
        if self._gzip_min_length > 0:
            from gzip import compress
            gzip_min_length = self._gzip_min_length

            def dumps(data):
                data = _dumps(dict(data), 4)
                if len(data) >= gzip_min_length:
                    data = compress(data)
                return data
        else:
            def dumps(data):
                return _dumps(dict(data), 4)
        return dumps

    @cached_property
    def _client(self):
        import redis
        client = redis.Redis.from_url(self._broker_url)
        client.set_response_callback('CLIENT LIST', self._parse_client_list)
        return client

    @staticmethod
    def _parse_client_list(response, **_options):
        clients = []
        for c in response.splitlines():
            clients.append(dict(pair.split(b'=', 1) for pair in c.split(b' ')))
        return clients

    @staticmethod
    def _encode(s: Union[bytes, str]) -> bytes:
        return s if isinstance(s, bytes) else s.encode('ascii')

    def _get_consumers(self):
        prefix = self.CONSUMER_PREFIX
        prefix_len = len(prefix)
        return [
            c[b'name'][prefix_len:]
            for c in self._client.client_list()
            if c[b'name'].startswith(prefix)
        ]

    def _create_queue(self, name):
        try:
            self._client.xgroup_create(name, self.GROUP_NAME, mkstream=True)
        except self.BrokerError as exc:
            if not str(exc).startswith('BUSYGROUP'):
                raise


class RedisBrokerComponent(Component):
    singleton = True

    def __init__(self,
                 broker_url: str = 'redis://localhost',
                 gzip_min_length: int = 0):
        self.broker_url = broker_url
        self.gzip_min_length = gzip_min_length

    def resolve(self, router: Router, config: Config, logger_service: LoggerService) -> Broker:
        broker_url = config.get('broker_url', self.broker_url)
        gzip_min_length = config.get('gzip_min_length', self.gzip_min_length)
        logger = logger_service.get_logger('bro.broker')
        return RedisBroker(broker_url=broker_url,
                           gzip_min_length=gzip_min_length,
                           router=router,
                           logger=logger)


BROKER_COMPONENTS = [
    RedisBrokerComponent()
]
