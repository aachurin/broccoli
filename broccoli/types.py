import abc
from typing import TypeVar, List, Tuple, Any, Callable
from broccoli.utils import cached_property


__all__ = ('Config', 'AppVar', 'Task', 'Message', 'Fence', 'State', 'Logger', 'Router', 'Broker', 'App')


Config = TypeVar('Config')
Context = TypeVar('Context')
AppVar = TypeVar('AppVar')
Task = TypeVar('Task')
Fence = TypeVar('Fence')
ACK = TypeVar('ACK')


CLOSERS = {'"': '"', "'": "'", '[': ']', '{': '}', '(': ')'}


class Message(dict):

    def __init__(self, task=None, id=None, **options):
        if isinstance(task, dict):
            super().__init__(task)  # dict(d)
            if 'id' not in self:
                raise TypeError('no id')
            if 'task' not in self:
                raise TypeError('no task')
        else:
            super().__init__(self, task=task, id=id, **options)

    def reply(self, **options):
        if 'graph_id' in self:
            options.setdefault('graph_id', self['graph_id'])
        if 'reply_to' in self:
            options.setdefault('reply_to', self['reply_to'])
        elif 'result_key' in self:
            options.setdefault('result_key', self['result_key'])
        return Message(
            id=self['id'],
            task=self['task'],
            type='reply',
            **options
        )

    @cached_property
    def is_reply(self):
        return self.get('type') == 'reply'

    def __str__(self):
        """Short representation"""
        return "{'id': %r, 'task': %r}" % (self['id'], self['task'])

    # noinspection PyDefaultArgument
    def __repr__(self, _closers=CLOSERS):
        """Full representation"""
        ret = []
        for k, v in self.items():
            v = repr(v)
            if len(v) > 100:
                v = v[:100] + ' ...'
                if v[0] in _closers:
                    v += _closers[v[0]]
            ret.append('%r: %s' % (k, v))
        return '{' + ', '.join(ret) + '}'

    def __reduce__(self):
        return self.__class__, (dict(self),)


class State:

    PENDING = 'pending'
    RUNNING = 'running'


class Logger(abc.ABC):

    @abc.abstractmethod
    def info(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def debug(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def critical(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def warning(self, msg: str, *args) -> None:
        raise NotImplementedError


class Router(abc.ABC):

    @abc.abstractmethod
    def get_queue(self, task_name: str) -> str:
        raise NotImplementedError


class Broker(abc.ABC):

    @property
    @abc.abstractmethod
    def DecodeError(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def BrokerError(self):
        raise NotImplementedError

    @abc.abstractmethod
    def set_node_id(self, node_id: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_nodes(self) -> List[Tuple[int, str]]:
        raise NotImplementedError

    @abc.abstractmethod
    def run_gc(self, verbose=False):
        raise NotImplementedError

    @abc.abstractmethod
    def setup(self, consumer_id: str, queues: List[str]):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_messages(self,
                     timeout: int = 0,
                     prefetch: int = 1) -> List[Tuple[ACK, Message]]:
        raise NotImplementedError

    @abc.abstractmethod
    def ack_message(self, key: ACK):
        raise NotImplementedError

    @abc.abstractmethod
    def send_message(self, queue: str, message: dict, reply_back: bool = False):
        raise NotImplementedError

    @abc.abstractmethod
    def send_reply(self, consumer: str, message: dict):
        raise NotImplementedError

    @abc.abstractmethod
    def set_result(self, result_key: str, message: dict, expires_in: int):
        raise NotImplementedError

    @abc.abstractmethod
    def get_result(self, result_key: str, timeout: int = 0):
        raise NotImplementedError

    @abc.abstractmethod
    def set_state(self, task_id: str, state: Any):
        raise NotImplementedError


class App(abc.ABC):

    settings: dict = None
    context: dict = None

    @property
    @abc.abstractmethod
    def RejectMessage(self):
        raise NotImplementedError

    @abc.abstractmethod
    def update_context_and_reset(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def inject(self, funcs, kwargs=None, cache=True):
        raise NotImplementedError

    @abc.abstractmethod
    def run_message(self,
                    message: Message,
                    callback: Callable = None,
                    callback_args=None, *,
                    fence=None):
        raise NotImplementedError

    @abc.abstractmethod
    def send_message(self, message: dict, reply_back: bool = False):
        raise NotImplementedError

    @abc.abstractmethod
    def result(self, result_key: str):
        raise NotImplementedError
