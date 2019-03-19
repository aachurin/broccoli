import time
import typing
import inspect
from datetime import datetime
from broccoli.injector import Injector
from broccoli.components import ReturnValue
from broccoli.task import create_task
from broccoli.result import AsyncResult
from broccoli.types import App, Broker, Router, Logger, Config, Task, Message, Fence
from broccoli.traceback import extract_log_tb
from broccoli.common import components as common_components
from broccoli.utils import cached_property
from broccoli.graph import build_graph


__all__ = ('Broccoli',)


class TaskNotFound(Exception):
    pass


class RejectMessage(Exception):
    pass


class nullfence:

    def __enter__(self):
        pass

    def __exit__(self, *excinfo):
        pass


class Broccoli(App):

    injector: Injector = None
    tasks: typing.Dict[str, Task] = None
    result_class = AsyncResult

    def __init__(self, components=None, settings=None, event_hooks=None) -> None:
        if components:
            msg = 'components must be a list of instances of Component.'
            assert all([(not isinstance(component, type) and hasattr(component, 'resolve'))
                        for component in components]), msg
        if settings is not None:
            msg = 'settings must be a dict.'
            assert isinstance(settings, dict), msg

        if event_hooks:
            msg = 'event_hooks must be a list.'
            assert isinstance(event_hooks, (list, tuple)), msg

        self.init_injector(components or [])
        self.settings = settings
        self.tasks = {}
        self.context = {}
        self.graphs = {}
        self.event_hooks = event_hooks

    @cached_property
    def TaskNotFound(self):
        return TaskNotFound

    @cached_property
    def RejectMessage(self):
        return RejectMessage

    def update_context_and_reset(self, **context):
        context = dict(self.context, **context)
        self.context = context
        self.graphs = {}
        self.injector.clear_cache()

    def init_injector(self, components):
        components = [Injector.ensure_component(c) for c in components]

        def has_component(cls):
            for c in components:
                if inspect.signature(c.resolve).return_annotation is cls:
                    return True
            return False

        if not has_component(Broker):
            from broccoli.broker import RedisBrokerComponent
            components += [RedisBrokerComponent()]

        if not has_component(Router):
            from broccoli.router import SimpleRouterComponent
            components += [SimpleRouterComponent()]

        if not has_component(Logger):
            from broccoli.logger import ConsoleLoggerComponent
            components += [ConsoleLoggerComponent()]

        if not has_component(Config):
            from broccoli.config import ConfigComponent
            components += [ConfigComponent()]

        components += common_components

        initial = {
            'app': App,
            'message': Message,
            'task': Task,
            'exc': Exception,
            'fence': Fence
        }

        self.injector = Injector(components, initial)

    def inject(self, funcs, args=None, kwargs=None, cache=True):
        state = {
            'app': self,
            'message': None,
            'task': None,
            'exc': None,
            'fence': None
        }
        return self.injector.run(
            funcs,
            state=state,
            func_args=args,
            func_kwargs=kwargs,
            cache=cache
        )

    def get_task(self, name: str):
        try:
            return self.tasks[name]
        except KeyError:
            raise self.TaskNotFound(name) from None

    def task(self, *args, **kwargs):
        def create_task_wrapper(func):
            task = create_task(self, func, **kwargs)
            if task.name in self.tasks:
                msg = 'Task with name %r is already registered.'
                raise TypeError(msg % task.name)
            self.tasks[task.name] = task
            return task

        if len(args) == 1:
            if callable(args[0]):
                return create_task_wrapper(*args)
            raise TypeError("Argument 1 to @task() must be a callable")

        if args:
            raise TypeError("@task() takes exactly 1 argument")

        return create_task_wrapper

    def get_event_hooks(self):
        event_hooks = []
        for hook in self.event_hooks:
            event_hooks.append(hook() if isinstance(hook, type) else hook)

        on_message = [
            hook.on_message for hook in event_hooks
            if hasattr(hook, 'on_message')
        ]

        on_reply = [
            hook.on_reply for hook in reversed(event_hooks)
            if hasattr(hook, 'on_reply')
        ]

        on_exception = [
            hook.on_exception for hook in reversed(event_hooks)
            if hasattr(hook, 'on_exception')
        ]

        return on_message, on_reply, on_exception

    def got_reply(self, reply: Message):
        if 'graph_id' not in reply:
            raise self.RejectMessage('no graph_id')
        graph_id = reply['graph_id']
        if graph_id not in self.graphs:
            raise self.RejectMessage('unexpected graph_id')
        graph = self.graphs[graph_id]
        if reply['id'] not in graph:
            raise self.RejectMessage('unexpected reply id')
        graph.set_complete(reply)

    def run_message(self,
                    message: Message,
                    on_complete: typing.Callable = None,
                    on_complete_args=None, *,
                    fence=nullfence()):
        if message.is_reply:
            self.got_reply(message)
        else:
            coro = self.run_message_async(message, on_complete, on_complete_args, fence)
            try:
                coro.send(None).on_complete = coro
            except StopIteration:
                pass

    async def run_message_async(self, message, on_complete, on_complete_args, fence):
        if self.event_hooks is None:
            on_message, on_reply, on_exception = [], [], []
        else:
            on_message, on_reply, on_exception = self.get_event_hooks()

        state = {
            'app': self,
            'message': message,
            'task': None,
            'exc': None,
            'fence': fence
        }

        try:
            __log_tb_start__ = None
            task = self.get_task(message['task'])
            state['task'] = task
            self.injector.run(on_message + [self.on_message], state=state)
            task.func_args_guard(*(message.get('args') or ()), **(message.get('kwargs') or {}))
            graph = build_graph(self, message)
            if graph:
                self.graphs[graph.id] = graph
                try:
                    await graph
                finally:
                    del self.graphs[graph.id]
            funcs = (
                [task.handler, self.render_reply] +
                on_reply +
                [self.on_reply]
            )
            result = self.injector.run(funcs,
                                       func_args=message.get('args'),
                                       func_kwargs=message.get('kwargs'),
                                       state=state)

        except Exception as exc:
            state['exc'] = exc
            result = self.injector.run(
                ([self.render_exception] + on_exception + [self.on_reply]),
                state=state
            )

        on_complete(result, *(on_complete_args or ()))

    def on_message(self, message: Message):
        if 'expires_at' in message:
            expires_at = message['expires_at']
            if isinstance(expires_at, datetime):
                if expires_at > datetime.utcnow():
                    raise self.RejectMessage('Due to expiration time.')
            elif isinstance(expires_at, int):
                if expires_at > time.time():
                    raise self.RejectMessage('Due to expiration time.')

    @staticmethod
    def render_reply(message: Message, return_value: ReturnValue):
        return message.reply(value=return_value)

    def render_exception(self, message: Message, task: Task, exc: Exception):
        if isinstance(exc, task.throws) or isinstance(exc, (self.TaskNotFound, self.RejectMessage)):
            return message.reply(exc=exc)
        traceback = extract_log_tb(exc)
        if traceback:
            return message.reply(exc=exc, traceback=traceback)
        return message.reply(exc=exc)

    @staticmethod
    def on_reply(message: Message, task: Task, reply: ReturnValue):
        if message.get('ignore_result', task.ignore_result):
            reply.pop('result_key', None)
        return reply

    def send_message(self, message: dict, reply_back: bool = False):
        queue = message.get('queue') or self._router.get_queue(message['task'])
        self._broker.send_message(queue, message, reply_back=reply_back)

    def result(self, result_key: str):
        return self.result_class(self, result_key)

    @cached_property
    def _broker(self) -> Broker:
        def get(obj: Broker):
            return obj
        return self.inject([get], cache=False)

    @cached_property
    def _router(self) -> Router:
        def get(obj: Router):
            return obj
        return self.inject([get], cache=False)
