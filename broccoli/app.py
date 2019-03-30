import time
import typing
import inspect
from uuid import uuid4
from broccoli.injector import ASyncInjector
from broccoli.components import ReturnValue
from broccoli.task import create_task
from broccoli.result import AsyncResult
from broccoli.types import App, Broker, Task, Message, Arguments, Fence, TaskLogger
from broccoli.traceback import extract_log_tb
from broccoli.utils import cached_property
from broccoli.graph import Graph
from broccoli.exceptions import Reject
from broccoli.router import ROUTER_COMPONENTS
from broccoli.broker import BROKER_COMPONENTS
from broccoli.logger import LOGGER_COMPONENTS
from broccoli.config import CONFIG_COMPONENTS
from broccoli.arguments import ARGUMENT_COMPONENTS

__all__ = ('Broccoli',)


class nullfence:

    def __enter__(self):
        pass

    def __exit__(self, *excinfo):
        pass


class Broccoli(App):
    _injector: ASyncInjector = None
    _tasks: typing.Dict[str, Task] = None
    result_factory = AsyncResult
    graph_factory = Graph

    def __init__(self, components=None, settings=None) -> None:
        if components:
            msg = 'components must be a list of instances of Component.'
            assert all([(not isinstance(component, type) and hasattr(component, 'resolve'))
                        for component in components]), msg
        if settings is not None:
            msg = 'settings must be a dict.'
            assert isinstance(settings, dict), msg

        self.settings = settings
        self._init_injector(components or [])
        self._tasks = {}
        self._context = {}
        self._graphs = {}

    def set_context(self, **context):
        self._context = dict(self._context, **context)
        self._graphs = {}
        self._injector.clear_cache()

    def get_context(self):
        return self._context

    def set_hooks(self,
                  on_request: typing.Callable = None,
                  on_response: typing.Callable = None):
        if on_request:
            if inspect.iscoroutinefunction(on_request):
                msg = 'Function %r may not be async.'
                raise TypeError(msg % on_request)
            self._on_request_hook = on_request
        elif '_on_request_hook' in self.__dict__:
            del self._on_request_hook
        if on_response:
            if inspect.iscoroutinefunction(on_response):
                msg = 'Function %r may not be async.'
                raise TypeError(msg % on_response)
            self._on_response_hook = on_response
        elif '_on_response_hook' in self.__dict__:
            del self._on_response_hook

    def _init_injector(self, components):
        components = components or []
        components += ROUTER_COMPONENTS
        components += LOGGER_COMPONENTS
        components += BROKER_COMPONENTS
        components += CONFIG_COMPONENTS
        components += ARGUMENT_COMPONENTS

        initial = {
            'app': App,
            'message': Message,
            'args': Arguments,
            'task': Task,
            'exc': Exception,
            'fence': Fence
        }

        self._injector = ASyncInjector(components, initial)

    def inject(self, funcs, args=None, cache=True):
        state = {
            'app': self,
            'message': None,
            'args': args,
            'task': None,
            'exc': None,
            'fence': None
        }
        return self._injector.run(
            funcs,
            state=state,
            cache=cache
        )

    def get_task(self, name: str):
        try:
            return self._tasks[name]
        except KeyError:
            raise Reject('Task %s not found' % name)

    def task(self, *args, **kwargs):
        def create_task_wrapper(func):
            if inspect.iscoroutinefunction(func):
                msg = 'Function %r may not be async.'
                raise TypeError(msg % func)
            task = create_task(self, func, **kwargs)
            if task.name in self._tasks:
                msg = 'Task with name %r is already registered.'
                raise TypeError(msg % task.name)
            self._tasks[task.name] = task
            return task

        if len(args) == 1:
            if callable(args[0]):
                return create_task_wrapper(*args)
            raise TypeError("Argument 1 to @task() must be a callable")

        if args:
            raise TypeError("@task() takes exactly 1 argument")

        return create_task_wrapper

    def send_message(self, message: dict):
        self._broker.send_message(message)

    def result(self, result_key: str):
        return self.result_factory(self, result_key)

    def serve_message(self, message: dict, fence: Fence = None):
        if 'id' not in message:
            raise Reject('no id')

        if 'task' not in message:
            raise Reject('no task')

        if 'reply_id' in message:
            if 'graph_id' not in message:
                raise Reject('no graph_id')
            graph_id = message['graph_id']
            if graph_id not in self._graphs:
                raise Reject('unexpected graph_id')
            graph = self._graphs[graph_id]
            if message['reply_id'] not in graph:
                raise Reject('unexpected reply id')
            graph.run_reply(message)
            return graph.get_pending_messages()
        else:
            coro = self._run_async(message, fence)
            try:
                graph = coro.send(None)
                graph.set_coroutine(coro)
                return graph.get_pending_messages()
            except StopIteration as stop:
                return [stop.value]

    async def _run_async(self, message, fence):
        state = {
            'app': self,
            'message': message,
            'fence': fence,
            'args': None,
            'task': None,
            'exc': None,
            'return_value': None
        }

        try:
            __log_tb_start__ = None
            task = self.get_task(message['task'])
            state['task'] = task
            funcs = (
                self._on_request,
                self._on_request_hook,
                self._build_graph,
                task.handler,
                self._on_response_hook,
                self._on_response,
            )
            return await self._injector.run_async(funcs, state=state)
        except Exception as exc:
            try:
                state['exc'] = exc
                step = state.get('$step', 0)
                if 0 < step < 4:
                    funcs = (self._on_response_hook, self._on_response)
                else:
                    funcs = (self._on_response,)
                return self._injector.run(funcs, state=state)
            except Exception as inner_exc:
                state['exc'] = inner_exc
                return self._injector.run((self._on_response,), state)

    @staticmethod
    def _on_request(message: Message):
        expires_at = message.get('expires_at')
        if expires_at is not None and isinstance(expires_at, (int, float)):
            if expires_at < time.time():
                raise Reject('Due to expiration time.')

    @staticmethod
    def _on_request_hook(ret: ReturnValue):
        return ret

    async def _build_graph(self, task: Task, message: Message) -> Arguments:
        args = ()
        if message.get('subtasks'):
            graph = self.graph_factory(message)
            self._graphs[graph.id] = graph
            try:
                args = await graph
            finally:
                graph.close()
                del self._graphs[graph.id]
        return task.get_arguments(
            *((message.get('args') or ()) + tuple(args)),
            **(message.get('kwargs') or {})
        )

    @staticmethod
    def _on_response_hook(ret: ReturnValue):
        return ret

    @staticmethod
    def _on_response(message: Message,
                     exc: Exception,
                     logger: TaskLogger,
                     return_value: ReturnValue,
                     task: Task):
        reply = {'id': str(uuid4()), 'task': message['task'], 'reply_id': message['id']}
        if 'graph_id' in message:
            reply['graph_id'] = message['graph_id']
        if 'reply_to' in message:
            reply['reply_to'] = message['reply_to']
        if 'result_key' in message:
            if message.get('ignore_result', task.ignore_result):
                reply['result_key'] = None
            else:
                reply['result_key'] = message['result_key']
        if '_context' in message:
            reply['_context'] = message['_context']
        if exc is not None:
            reply['exc'] = exc
            if isinstance(exc, task.throws) or isinstance(exc, Reject):
                logger.error("Task {'id': %r, 'task': %r} raised exception %s: %s",
                             message['id'], message['task'], exc.__class__.__name__, exc)
                return reply
            else:
                traceback = extract_log_tb(exc)
                if traceback:
                    reply['traceback'] = traceback
                logger.error("Task {'id': %r, 'task': %r} raised exception %s: %s\n%s",
                             message['id'], message['task'], exc.__class__.__name__, exc, traceback)
                return reply
        reply['value'] = return_value
        return reply

    @cached_property
    def _broker(self) -> Broker:
        def get(obj: Broker):
            return obj

        return self.inject([get], cache=False)
