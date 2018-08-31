import typing
from uuid import uuid4
from . interfaces import App
from . traceback import Traceback


class State:

    PENDING = 'pending'
    RUNNING = 'running'
    ERROR = 'error'
    DONE = 'done'


Header = typing.NewType('Header', typing.Any)


class Request(typing.NamedTuple):

    id: str
    task: str
    args: typing.Optional[typing.Dict[str, typing.Any]]
    headers: typing.Dict[str, Header]


class Response(typing.NamedTuple):

    id: str
    value: typing.Optional[typing.Any] = None
    exc: typing.Optional[BaseException] = None
    traceback: typing.Optional[str] = None


class Task():

    name: str
    handler: typing.Callable
    headers: dict

    # Tuple of expected exceptions.
    throws = ()

    def __call__(self, *args, **kwargs):
        __log_tb_stop__ = 0
        return self.apply(args, kwargs)

    def delay(self, **kwargs):
        return self.apply(kwargs, async=True)

    def apply(self, args=None, queue=None, async=False, headers=None):
        app = self.app
        args = args or {}
        queue = queue or app.router.get_queue(self.name)
        headers = dict(self.headers, **(headers or ()))
        task_id = uuid4().hex
        request = Request(task_id, self.name, args, headers)
        app.broker.push_request(queue, request)

        if async:
            return AsyncResult(app, task_id)

        result = app.broker.pop_result(task_id)
        return self.unpack_response(result)

    @staticmethod
    def unpack_response(response: Response, raise_exception: bool = True):
        if response.exc is not None:
            if raise_exception:
                tb = Traceback(response.traceback) if response.traceback else None
                raise response.exc from tb
            return response.exc
        return response.value

    def __repr__(self):
        return repr(self.handler)


class AsyncResult():

    __slots__ = ('app', 'task_id')

    def __init__(self, app: App, task_id: str) -> None:
        self.app = app
        self.task_id = task_id

    def get(self, default=None, raise_exception=False) -> typing.Any:
        result = self.app.broker.peek_result(self.task_id)
        if result is None:
            return default
        return Task.unpack_response(result, raise_exception)

    def wait(self):
        result = self.app.broker.pop_result(self.task_id)
        return Task.unpack_response(result)

    def __repr__(self):
        return '%s(task_id=%r)' % (self.__class__.__name__, self.task_id)
