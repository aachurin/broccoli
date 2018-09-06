import typing
from uuid import uuid4
from . types import Request, Response, TaskName


class Task:

    name: TaskName
    handler: typing.Callable
    headers: dict

    # Tuple of expected exceptions.
    throws = ()

    def apply(self, args=None, queue=None, async=False, expires=None, **headers):
        app = self.app
        args = args or {}
        queue = queue or app.router.get_queue(self.name)
        headers = dict(self.headers, **headers)
        task_id = uuid4().hex
        request = Request(task_id, self.name, args, headers)
        app.broker.send_request(queue, request, expires)

        if async:
            return AsyncResult(app, task_id)

        response: Response = app.broker.get_result(task_id)
        return response.get_value()

    def delay(self, **kwargs):
        return self.apply(kwargs, async=True)

    def __call__(self, **kwargs):
        return self.apply(kwargs)

    @staticmethod
    def create_task(func, app, name=None, base=None, **headers):
        name = name or '%s.%s' % (func.__module__, func.__name__)
        base = base or Task

        options = {}

        for key in list(headers.keys()):
            if key.startswith('_'):
                raise TypeError('Invalid @task parameter %r' % key)
            if hasattr(base, key):
                if callable(getattr(base, key)):
                    raise TypeError('Invalid @task parameter %r' % key)
                options[key] = headers.pop(key)

        task = type(func.__name__, (base,), dict({
            'app': app,
            'name': name,
            'handler': staticmethod(func),
            'headers': headers,
            '__doc__': func.__doc__,
            '__module__': func.__module__
        }, **options))()

        try:
            task.__qualname__ = func.__qualname__
        except AttributeError:
            pass

        task.__name__ = func.__name__

        return task

    def __repr__(self):
        return repr(self.handler)


class AsyncResult:

    def __init__(self, app, task_id) -> None:
        self.app = app
        self.task_id = task_id

    def get(self, default=None, raise_exception=False) -> typing.Any:
        response: Response = self.app.broker.peek_result(self.task_id)
        if response is None:
            return default
        return response.get_value(raise_exception)

    def wait(self, raise_exception=False):
        response: Response = self.app.broker.get_result(self.task_id)
        return response.get_value(raise_exception)

    def __repr__(self):
        return '%s(task_id=%r)' % (self.__class__.__name__, self.task_id)
