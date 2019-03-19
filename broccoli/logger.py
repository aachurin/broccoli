import logging
import logging.config
from broccoli.types import Logger, AppVar, Task
from broccoli.utils import get_colorizer, color
from broccoli.components import Component


class ConsoleLogger(Logger):

    debug_color = color.cyan
    info_color = color.light_white
    warning_color = color.yellow
    error_color = color.light_red
    critical_color = color.red

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 40

    LEVELS = {
        'CRITICAL': CRITICAL,
        'ERROR': ERROR,
        'WARNING': WARNING,
        'INFO': INFO,
        'DEBUG': DEBUG,
        'NOTSET': 0,
    }

    colorize = staticmethod(get_colorizer())

    def __init__(self, level, thread_name, task_name, format) -> None:
        self.level = level
        self.thread_name = thread_name
        self.task_name = task_name
        self.log_format = format

    def format(self, level, message):
        return self.log_format.format(
            level=level,
            message=message,
            thread=self.thread_name,
            task=self.task_name
        )

    def debug(self, msg, *args) -> None:
        if self.level <= self.DEBUG:
            msg = msg % args if args else msg
            print(self.colorize(self.format('DEBUG', msg), self.debug_color))

    def info(self, msg, *args) -> None:
        if self.level <= self.INFO:
            msg = msg % args if args else msg
            print(self.colorize(self.format('INFO', msg), self.info_color))

    def warning(self, msg, *args) -> None:
        if self.level <= self.WARNING:
            msg = msg % args if args else msg
            print(self.colorize(self.format('WARNING', msg), self.warning_color))

    def error(self, msg, *args) -> None:
        if self.level <= self.ERROR:
            msg = msg % args if args else msg
            print(self.colorize(self.format('ERROR', msg), self.error_color))

    def critical(self, msg, *args) -> None:
        if self.level <= self.CRITICAL:
            msg = msg % args if args else msg
            print(self.colorize(self.format('CRITICAL', msg), self.critical_color))


class StandardLoggerComponent(Component):

    def __init__(self, logging_config=None):
        if logging_config:
            logging.config.dictConfig(logging_config)

    # noinspection PyMethodOverriding
    def resolve(self, task: Task) -> Logger:
        return logging.getLogger(task.name)


class ConsoleLoggerComponent(Component):

    def __init__(self, *, log_level: str = 'DEBUG', format='[{thread}] {level}|{task}: {message}') -> None:
        self.format = format
        self.log_level = ConsoleLogger.LEVELS[log_level]

    # noinspection PyMethodOverriding
    def resolve(self, thread_name: AppVar = 'main', task: Task = None) -> Logger:
        task_name = task.name if task else 'app'
        return ConsoleLogger(self.log_level, thread_name, task_name, format=self.format)
