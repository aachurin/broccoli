import typing
import pickle
from . interfaces import Broker, Configurable
from . utils import cached_property
from . exceptions import BrokerError, BrokerResultLocked


class RedisBroker(Configurable,
                  Broker):
    """
    broker_url       Redis server URI.
    result_expires   The result expiration timeout.
    gzip_min_length  Sets the minimum length of a message that will be gzipped.
    """

    broker_url: str
    result_expires: int
    gzip_min_length: int

    def __init__(self, *,
                 broker_url: str = 'redis://localhost',
                 result_expires: int = 3600,
                 gzip_min_length: int = 0) -> None:
        self.dumps = None
        self.loads = None
        self.configure(broker_url, result_expires, gzip_min_length)

    def configure(self,
                  broker_url: str = None,
                  result_expires: int = None,
                  gzip_min_length: int = None):
        if broker_url is not None:
            self.broker_url = broker_url
        if result_expires is not None:
            if result_expires <= 0:
                raise ValueError("result_expires must be greater than zero")
            self.result_expires = result_expires
        if gzip_min_length is not None:
            if gzip_min_length < 0:
                raise ValueError("gzip_min_length must be greater than zero")
            self.gzip_min_length = gzip_min_length
            self.dumps, self.loads = get_encoder(gzip_min_length)

    def get_configuration(self):
        ret = {
            'class': self.__class__.__name__,
            'broker_url': self.broker_url,
            'result_expires': self.result_expires
        }
        if self.gzip_min_length:
            ret['gzip_min_length'] = self.gzip_min_length
        return 'Broker', ret

    @cached_property
    def server(self):
        import redis
        return redis.StrictRedis.from_url(self.broker_url)

    @cached_property
    def errors(self):
        import redis.exceptions
        return redis.exceptions.ConnectionError

    def push_request(self, queue: str, req: typing.Any) -> None:
        queue_key = 'queue.%s' % queue
        try:
            self.server.rpush(queue_key, self.dumps(req))
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def pop_request(self, queues: typing.List[str], timeout: float = 0) -> typing.Any:
        queue_keys = ['queue.%s' % q for q in queues]
        try:
            req = self.server.brpop(queue_keys, timeout)
        except self.errors as e:
            raise BrokerError(str(e)) from None
        if req is not None:
            return self.loads(req[1])
        return None

    def set_state(self, task_id: str, value: str, expires: int = None) -> None:
        state_key = 'state.%s' % task_id
        expires = expires or self.result_expires
        try:
            self.server.setex(state_key, expires, value)
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def get_state(self, task_id: str) -> None:
        state_key = 'state.%s' % task_id
        try:
            return self.server.get(state_key)
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def set_meta(self, task_id: str, value: typing.Any, expires: int = None) -> None:
        value = self.dumps(value)
        meta_key = 'meta.%s' % task_id
        expires = expires or self.result_expires
        try:
            self.server.setex(meta_key, expires, value)
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def get_meta(self, task_id: str) -> typing.Any:
        meta_key = 'meta.%s' % task_id
        try:
            ret = self.server.get(meta_key)
        except self.errors as e:
            raise BrokerError(str(e)) from None
        if ret is not None:
            return self.loads(ret)
        return None

    def push_result(self, task_id: str, value: typing.Any, expires: int = None) -> None:
        value = self.dumps(value)
        result_key = 'result.%s' % task_id
        expires = expires or self.result_expires
        try:
            p = self.server.pipeline().rpush(result_key, value)
            if expires:
                p = p.expires(result_key, expires)
            p.execute()
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def pop_result(self, task_id: str, timeout: float = 0) -> typing.Any:
        result_key = 'result.%s' % task_id
        lock_key = 'lock.%s' % task_id
        if not self.server.setnx(lock_key, '1'):
            raise BrokerResultLocked(task_id)
        try:
            ret = self.server.brpop(result_key, timeout)
        except self.errors as e:
            raise BrokerError(str(e)) from None
        finally:
            self.server.delete(lock_key)
        if ret is not None:
            return self.loads(ret[1])
        return None

    def peek_result(self, task_id: str, timeout: float = None) -> typing.Any:
        result_key = 'result.%s' % task_id
        if timeout is not None:
            try:
                ret = self.server.brpoplpush(result_key, result_key, timeout)
            except self.errors as e:
                raise BrokerError(str(e)) from None
        else:
            try:
                ret = self.server.lindex(result_key, 0)
            except self.errors as e:
                raise BrokerError(str(e)) from None
        if ret is not None:
            return self.loads(ret[1])
        return None


def get_encoder(gzip_min_length: int):
    _dumps = pickle.dumps
    _loads = pickle.loads
    if gzip_min_length > 0:
        from gzip import compress, decompress

        def loads(data):
            if data[0] == 0x1f:
                data = decompress(data)
            return _loads(data)

        def dumps(data):
            data = _dumps(data, 4)
            if len(data) >= gzip_min_length:
                data = compress(data)
            return data
    else:
        def loads(data):
            return _loads(data)

        def dumps(data):
            return _dumps(data, 4)
    return dumps, loads
