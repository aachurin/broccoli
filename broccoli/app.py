import time
import typing
from . injector import Injector, ReturnValue
from . interfaces import Broker, Router, Worker, Event, App as BaseApp
from . exceptions import TaskNotFound
from . task import Task, Request, Response
from . traceback import extract_log_tb
from . components import default_components


__all__ = ('App',)


class App(BaseApp):

    tasks: typing.Dict[str, Task]
    task_class = Task

    def __init__(self,
                 broker=None,
                 router=None,
                 worker=None,
                 components=None,
                 hooks=None) -> None:
        if components:
            msg = 'components must be a list of instances of Component.'
            assert all([(not isinstance(component, type) and hasattr(component, 'resolve'))
                        for component in components]), msg
        if hooks:
            msg = 'hooks must be a list.'
            assert isinstance(hooks, (list, tuple)), msg

        self.hooks = hooks or []

        self.check_epoch_time()
        self.init_injector(components)
        self.init_broker(broker)
        self.init_router(router)
        self.init_worker(worker)

        self.on_request = self.get_hooks('on_request')
        self.on_response = self.get_hooks('on_response', reverse=True)

        self.tasks = {}

    @staticmethod
    def check_epoch_time():
        tm = time.gmtime(0)
        msg = "Looks like your epoch time is not 1970-01-01T00:00:00"
        assert ((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec) == (1970, 1, 1, 0, 0, 0)), msg

    def init_injector(self, components=None):
        components = (components or []) + default_components
        initial_components = {
            'app': BaseApp,
            'request': Request,
            'response': Response,
            'event': Event,
            'exc': Exception,
        }
        self.injector = Injector(components, initial_components)

    def init_broker(self, broker: Broker):
        if broker is None:
            from . broker import RedisBroker
            broker = RedisBroker()
        msg = 'broker must be an instance of Broker'
        assert isinstance(broker, Broker), msg
        self.broker = broker

    def init_router(self, router: Router):
        if router is None:
            from . router import SimpleRouter
            router = SimpleRouter()
        msg = 'router must be an instance of Router'
        assert isinstance(router, Router), msg
        self.router = router

    def init_worker(self, worker: Worker):
        if worker is None:
            from . worker import PreforkWorker
            worker = PreforkWorker()
        msg = 'worker must be an instance of Worker'
        assert isinstance(worker, Worker), msg
        self.worker = worker

    def get_hooks(self, name, reverse=False):
        hooks = self.hooks
        if reverse:
            hooks = reversed(hooks)
        return [
            getattr(hook, name) for hook in hooks
            if hasattr(hook, name)
        ]

    def inject(self,
               funcs,
               request: Request = None,
               response: Response = None,
               exc: Exception = None,
               event: dict = None):
        state = {
            'app': self,
            'request': request,
            'response': response,
            'exc': exc,
            'event': event
        }
        return self.injector.run(funcs, state)

    def serve(self):
        return self.inject([self.worker.start])

    def lookup_task(self, name: str) -> Task:
        try:
            return self.tasks[name]
        except KeyError:
            raise TaskNotFound(name) from None

    @staticmethod
    def render_response(request: Request,
                        return_value: ReturnValue) -> Response:
        return Response(id=request.id, value=return_value)

    @staticmethod
    def exception_handler(task: Task,
                          request: Request,
                          exc: Exception) -> Response:
        if task is not None and not isinstance(exc, task.throws):
            tb = extract_log_tb(exc)
            return Response(id=request.id, exc=exc, traceback=tb)
        return Response(id=request.id, exc=exc)

    def __call__(self, request: Request) -> Response:
        state = {
            'app': self,
            'broker': self.broker,
            'request': request,
            'response': None,
            'exc': None,
            'event': None
        }

        try:
            task = self.lookup_task(request.task)
            funcs = (
                self.on_request
                + [task.handler, self.render_response]
                + self.on_response
            )
            return self.injector.run(funcs, state)
        except Exception as exc:
            state['exc'] = exc
            funcs = (
                [self.exception_handler]
                + self.on_response
            )
            return self.injector.run(funcs, state)

    def task(self, *args, **kwargs):
        def create_task_wrapper(func):
            def create_task(name=None, base=None, **headers):
                name = name or '%s.%s' % (func.__module__, func.__name__)
                base = base or self.task_class

                options = {}
                for key in list(headers.keys()):
                    if key.startswith('_'):
                        raise TypeError('Invalid @task parameter %r' % key)
                    if hasattr(base, key):
                        if callable(getattr(base, key)):
                            raise TypeError('Invalid @task parameter %r' % key)
                        options[key] = headers.pop(key)

                task = type(func.__name__, (base,), dict({
                    'app': self,
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

                self.tasks[name] = task
                return task

            return create_task(**kwargs)

        if len(args) == 1:
            if callable(args[0]):
                return create_task_wrapper(*args)
            raise TypeError("argument 1 to @task() must be a callable")

        if args:
            raise TypeError("@task() takes exactly 1 argument")

        return create_task_wrapper
