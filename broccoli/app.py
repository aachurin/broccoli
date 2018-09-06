import re
import time
import uuid
import typing
from . task import Task
from . exceptions import TaskNotFound
from . injector import Injector, ReturnValue
from . traceback import extract_log_tb
from . types import Configurable, Broker, Router, Worker, Event, Request, Response


__all__ = ('App',)


class App(Configurable):
    """
    node_id          Unique node name
    """

    broker: Broker
    router: Router
    worker: Worker
    injector: Injector
    node_id: str

    tasks: typing.Dict[str, Task]
    hooks: list

    def __init__(self, *,
                 node_id=None,
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

        self.check_epoch_time()

        self.hooks = hooks or []
        self.tasks = {}

        self.configure(node_id or str(uuid.uuid4()))

        self.init_injector(components)
        self.init_broker(broker)
        self.init_router(router)
        self.init_worker(worker)

        self.setup()

    @staticmethod
    def check_epoch_time():
        tm = time.gmtime(0)
        msg = "Looks like your epoch time is not 1970-01-01T00:00:00"
        assert ((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec) == (1970, 1, 1, 0, 0, 0)), msg

    def init_injector(self, components=None):
        from . components import DEFAULT_COMPONENTS
        components = (components or []) + DEFAULT_COMPONENTS
        initial = {
            'app': App,
            'request': Request,
            'event': Event,
            'exc': Exception,
        }
        self.injector = Injector(components, initial)

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

    def configure(self,
                  node_id: str = None):
        if node_id is not None:
            if not re.match(r'(?:[a-zA-Z0-9]+[@.\-]?)+$', node_id):
                raise ValueError("invalid node id")
            self.node_id = node_id

    def get_configuration(self):
        ret = {
            'node_id': self.node_id
        }
        return 'Node', ret

    def setup(self):
        self.broker.bind(self)
        self.router.bind(self)
        self.worker.bind(self)

    def get_hooks(self, name, reverse=False):
        hooks = self.hooks
        if reverse:
            hooks = reversed(hooks)
        return [getattr(hook, name) for hook in hooks if hasattr(hook, name)]

    def get_task(self, name: str) -> Task:
        try:
            return self.tasks[name]
        except KeyError:
            raise TaskNotFound(name) from None

    def task(self, *args, **kwargs):
        def create_task_wrapper(func):
            task = Task.create_task(func, self, **kwargs)
            self.tasks[task.name] = task
            return task

        if len(args) == 1:
            if callable(args[0]):
                return create_task_wrapper(*args)
            raise TypeError("argument 1 to @task() must be a callable")

        if args:
            raise TypeError("@task() takes exactly 1 argument")

        return create_task_wrapper

    def serve(self):
        return self.inject((self.worker.start,))

    def inject(self,
               funcs,
               request: Request = None,
               event: Event = None,
               exc: BaseException = None):
        return self.injector.run(funcs, {
            'app': self,
            'request': request,
            'event': event,
            'exc': exc,
        })

    def __call__(self, request: Request) -> Response:
        try:
            task = self.get_task(request.task)
            funcs = (task.handler, self.render_response)
            return self.inject(funcs, request=request)
        except Exception as exc:
            funcs = (self.exception_handler,)
            return self.inject(funcs, request=request, exc=exc)

    @staticmethod
    def render_response(request: Request,
                        return_value: ReturnValue) -> Response:
        return Response(request.id, value=return_value)

    @staticmethod
    def exception_handler(task: Task,
                          request: Request,
                          exc: Exception) -> Response:
        if not isinstance(exc, task.throws):
            tb = extract_log_tb(exc)
            return Response(request.id, exc=exc, traceback=tb)
        return Response(request.id, exc=exc)
