import pickle
from typing import List, Optional
from . app import App
from . types import Broker, Configurable, Request, Response
from . utils import cached_property
from . exceptions import BrokerError, BrokerResultLocked


class RedisBroker(Configurable,
                  Broker):
    """
    broker_url       Redis server URI.
    result_expires   The result expiration timeout.
    gzip_min_length  Sets the minimum length of a message that will be gzipped.

    """

    app: App
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

    def send_request(self, queue: str, request: Request, expires: int = None) -> None:
        queue_key = 'queue#' + queue
        location_key = 'location#' + request.id
        data = self.dumps(request)
        try:
            p = self.server.pipeline()
            p.rpush(queue_key, data)
            if expires is not None:
                p.setex(location_key, expires, '?')
            else:
                p.set(location_key, '?')
            p.execute()
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def get_request(self, queues: List[str], timeout: float = 0) -> Optional[Request]:
        queue_keys = [('queue#' + q) for q in queues]
        try:
            data = self.server.brpop(queue_keys, timeout)
        except self.errors as e:
            raise BrokerError(str(e)) from None
        if data is None:
            return None
        request = self.loads(data[1])
        location_key = 'location#' + request.id
        if self.server.set(location_key, self.app.node_id, xx=True):
            return request

    def send_result(self, response: Response, expires: int = None) -> None:
        result_key = 'result#' + response.id
        location_key = 'location#' + response.id
        data = self.dumps(response)
        expires = expires or self.result_expires
        try:
            p = self.server.pipeline()
            p.delete(location_key)
            p.rpush(result_key, data)
            if expires:
                p.expire(result_key, expires)
            p.execute()
        except self.errors as e:
            raise BrokerError(str(e)) from None

    def get_result(self, response_id: str, timeout: float = 0) -> Optional[Response]:
        result_key = 'result#' + response_id
        lock_key = 'lock#' + response_id
        if not self.server.setnx(lock_key, '1'):
            raise BrokerResultLocked(response_id)
        try:
            data = self.server.brpop(result_key, timeout)
        except self.errors as e:
            raise BrokerError(str(e)) from None
        finally:
            self.server.delete(lock_key)
        if data is not None:
            return self.loads(data[1])
        return None

    def peek_result(self, response_id: str, timeout: float = None) -> Optional[Response]:
        result_key = 'result#' + response_id
        if timeout is not None:
            try:
                data = self.server.brpoplpush(result_key, result_key, timeout)
            except self.errors as e:
                raise BrokerError(str(e)) from None
        else:
            try:
                data = self.server.lindex(result_key, 0)
            except self.errors as e:
                raise BrokerError(str(e)) from None
        if data is not None:
            return self.loads(data[1])
        return None

    #     state_key = 'state.%s' % task_id
    #     expires = expires or self.result_expires
    #     try:
    #         self.se    # def set_state(self, task_id: str, value: str, expires: int = None) -> None:rver.setex(state_key, expires, value)
    #     except self.errors as e:
    #         raise BrokerError(str(e)) from None
    #
    # def get_state(self, task_id: str) -> None:
    #     state_key = 'state.%s' % task_id
    #     try:
    #         return self.server.get(state_key)
    #     except self.errors as e:
    #         raise BrokerError(str(e)) from None
    #
    # def set_meta(self, task_id: str, value: typing.Any, expires: int = None) -> None:
    #     value = self.dumps(value)
    #     meta_key = 'meta.%s' % task_id
    #     expires = expires or self.result_expires
    #     try:
    #         self.server.setex(meta_key, expires, value)
    #     except self.errors as e:
    #         raise BrokerError(str(e)) from None
    #
    # def get_meta(self, task_id: str) -> typing.Any:
    #     meta_key = 'meta.%s' % task_id
    #     try:
    #         ret = self.server.get(meta_key)
    #     except self.errors as e:
    #         raise BrokerError(str(e)) from None
    #     if ret is not None:
    #         return self.loads(ret)
    #     return None


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
