import abc
import logging
from typing import TypeVar, Type, List, Tuple, Any, Callable

__all__ = ('Config', 'Argument', 'Arguments', 'Task', 'Message', 'Fence', 'TaskLogger',
           'State', 'Router', 'LoggerService', 'Broker', 'App')

Config = TypeVar('Config')
Argument = TypeVar('Argument')
Arguments = TypeVar('Arguments')
Message = TypeVar('Message')
Fence = TypeVar('Fence')
TaskLogger = TypeVar('TaskLogger')


CLOSERS = {'"': '"', "'": "'", '[': ']', '{': '}', '(': ')'}


class MsgRepr:
    __slots__ = ('m',)

    def __init__(self, m):
        self.m = m

    def __str__(self):
        """Short representation"""
        return "{'id': %r, 'task': %r}" % (self.m.get('id'), self.m.get('task'))

    # noinspection PyDefaultArgument
    def __repr__(self, _closers=CLOSERS):
        """Full representation"""
        ret = []
        for k, v in self.m.items():
            v = repr(v)
            if len(v) > 100:
                v = v[:100] + ' ...'
                if v[0] in _closers:
                    v += _closers[v[0]]
            ret.append('%r: %s' % (k, v))
        return '{' + ', '.join(ret) + '}'


class State:
    PENDING = 'pending'
    RUNNING = 'running'


class LoggerService(abc.ABC):

    @abc.abstractmethod
    def get_logger(self, name) -> logging.Logger:
        raise NotImplementedError()


class Router(abc.ABC):

    @abc.abstractmethod
    def get_queue(self, task_name: str) -> str:
        raise NotImplementedError()


class Broker(abc.ABC):

    @property
    @abc.abstractmethod
    def BrokerError(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_node_id(self, node_id: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_nodes(self) -> List[Tuple[int, str]]:
        raise NotImplementedError()

    @abc.abstractmethod
    def setup(self, consumer_id: str, queues: List[str]):
        raise NotImplementedError()

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_messages(self,
                     timeout: int = 0):
        raise NotImplementedError()

    @abc.abstractmethod
    def ack(self, key):
        raise NotImplementedError()

    @abc.abstractmethod
    def send_message(self, message: dict, reply_back: bool = False):
        raise NotImplementedError()

    @abc.abstractmethod
    def send_reply(self, consumer: str, message: dict):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_result(self, result_key: str, result: dict, expires_in: int):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_result(self, result_key: str, timeout: int = 0):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_state(self, task_id: str, state: Any):
        raise NotImplementedError()

    @abc.abstractmethod
    def run_gc(self):
        raise NotImplementedError()


class App(abc.ABC):
    settings: dict = None

    @abc.abstractmethod
    def set_hooks(self,
                  on_request: Callable = None,
                  on_response: Callable = None):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_context(self) -> dict:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_context(self, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def inject(self, funcs, args=None, cache=True):
        raise NotImplementedError()

    @abc.abstractmethod
    def serve_message(self, message: dict, fence: Fence = None):
        raise NotImplementedError()

    @abc.abstractmethod
    def send_message(self, message: dict):
        raise NotImplementedError()

    @abc.abstractmethod
    def result(self, result_key: str):
        raise NotImplementedError()


class Task(abc.ABC):
    throws: Tuple[Type[Exception], ...] = ()
    ignore_result: bool = False

    @property
    @abc.abstractmethod
    def handler(self) -> Callable:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def get_arguments(*args, **kwargs) -> dict:
        raise NotImplementedError()
