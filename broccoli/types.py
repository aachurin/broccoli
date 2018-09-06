import abc
from typing import Tuple, Any, Optional, List, Dict, NewType
from . traceback import Traceback


TaskId = NewType('TaskId', str)
TaskName = NewType('TaskName', str)
Header = NewType('Header', Any)


class State:

    PENDING = 'pending'
    RUNNING = 'running'
    ERROR = 'error'
    DONE = 'done'


class Request:

    id: str

    def __init__(self,
                 id: str,
                 task: str,
                 args: Dict[str, Any] = None,
                 headers: Dict[str, Header] = None) -> None:
        self.id = id
        self.task = task
        self.args = args
        self.headers = headers

    def __repr__(self):
        return '%s, args=%s, headers=%s' % (self.task, self.args, self.headers)


class Response:

    id: str

    def __init__(self,
                 id: str,
                 value: Any = None,
                 exc: BaseException = None,
                 traceback: str = None) -> None:
        self.id = id
        self.value = value
        self.exc = exc
        self.traceback = traceback

    def get_value(self, raise_exception: bool = True):
        if self.exc is not None:
            if raise_exception:
                tb = Traceback(self.traceback) if self.traceback else None
                raise self.exc from tb
            return self.exc
        return self.value


class Configurable(abc.ABC):

    @abc.abstractmethod
    def configure(self, **kwargs) -> None:
        raise NotImplementedError

    def get_configuration(self) -> Tuple[str, dict]:
        return '', {}


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

    # noinspection PyPep8Naming
    @abc.abstractmethod
    def setLevel(self, level: int) -> None:
        raise NotImplementedError


class Router(abc.ABC):

    app: object

    def bind(self, app):
        self.app = app

    @abc.abstractmethod
    def get_queue(self, task_name: str) -> str:
        raise NotImplementedError


class Broker(abc.ABC):

    app: object

    def bind(self, app):
        self.app = app

    @abc.abstractmethod
    def send_request(self, queue: str, request: Request, expire: int = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_request(self, queues: List[str], timeout: float = 0) -> Optional[Request]:
        raise NotImplementedError

    @abc.abstractmethod
    def send_result(self, response: Response, expires: int = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_result(self, response_id: str, timeout: float = 0) -> Optional[Response]:
        raise NotImplementedError

    @abc.abstractmethod
    def peek_result(self, response_id: str, timeout: float = None) -> Optional[Response]:
        raise NotImplementedError


class Worker(abc.ABC):

    app: object

    def bind(self, app):
        self.app = app

    @abc.abstractmethod
    def start(self, *args, **kwargs):
        raise NotImplementedError


class Event():
    pass
