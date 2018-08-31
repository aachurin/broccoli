import logging
import inspect
from . injector import Component
from . interfaces import App, Logger
from . task import Task, Request, Header
from . logger import ConsoleLogger


class HeaderComponent(Component):

    def resolve(self,
                parameter: inspect.Parameter,
                request: Request) -> Header:
        name = parameter.name
        if name not in request.headers:
            return None
        return Header(request.headers[name])


class TaskComponent(Component):

    def resolve(self, app: App, request: Request) -> Task:
        return app.lookup_task(request.task)


class ArgComponent(Component):

    default_allowed_argument_types = (int, float, bool, str, list, dict, object, tuple)

    def __init__(self, allowed_argument_types=None):
        self.allowed_argument_types = allowed_argument_types or self.default_allowed_argument_types

    def identity(self, parameter: inspect.Parameter):
        parameter_name = parameter.name.lower()
        return 'argument:' + parameter_name

    def can_handle_parameter(self, parameter: inspect.Parameter):
        return (parameter.annotation is inspect.Signature.empty
                or parameter.annotation in self.allowed_argument_types)

    def resolve(self, parameter: inspect.Parameter, request: Request):
        if parameter.default is parameter.empty:
            try:
                return request.args[parameter.name]
            except KeyError:
                raise TypeError("missing required argument: %r" % parameter.name)
        return request.args.get(parameter.name, parameter.default)


class StandardLoggerComponent(Component):

    def resolve(self, task: Task) -> Logger:
        return logging.getLogger(task.name)


class ConsoleLoggerComponent(Component):

    log_level: int
    _logger: logging.Logger

    def __init__(self, *, log_level: str='INFO') -> None:
        if log_level is not None:
            self.log_level = ConsoleLogger.LEVELS[log_level]

    def resolve(self) -> Logger:
        try:
            return self._logger
        except AttributeError:
            self._logger = ConsoleLogger(self.log_level)
        return self._logger


default_components = [
    HeaderComponent(),
    TaskComponent(),
    ArgComponent(),
    ConsoleLoggerComponent(log_level='INFO')
]
