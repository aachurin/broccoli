import abc
import typing


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

    @abc.abstractmethod
    def get_queue(self, task_name: str) -> str:
        raise NotImplementedError


class Broker(abc.ABC):

    @abc.abstractmethod
    def push_request(self, queue: str, req: typing.Any) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def pop_request(self, queues: typing.List[str], timeout: float = 0) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def set_state(self, task_id: str, value: str, expires: int = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_state(self, task_id: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def set_meta(self, task_id: str, value: typing.Any, expires: int = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_meta(self, task_id: str) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def push_result(self, task_id: str, value: typing.Any, expires: int = None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def pop_result(self, task_id: str, timeout: float = 0) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def peek_result(self, task_id: str, timeout: float = 0) -> typing.Any:
        raise NotImplementedError


class App(abc.ABC):

    broker: Broker
    router: Router


class Worker(abc.ABC):

    @abc.abstractmethod
    def start(self, *args, **kwargs):
        raise NotImplementedError


class Plugin(abc.ABC):

    @abc.abstractmethod
    def start(self):
        raise NotImplementedError
