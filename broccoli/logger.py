import logging
import logging.config
from broccoli.types import LoggerService, Task, TaskLogger, Config
from broccoli.components import Component
from broccoli.utils import get_colorizer, color


class ColorizedStreamHandler(logging.StreamHandler):
    colorize = staticmethod(get_colorizer())
    colors = {
        logging.DEBUG: color.cyan,
        logging.INFO: color.light_white,
        logging.WARNING: color.yellow,
        logging.ERROR: color.light_red,
        logging.CRITICAL: color.red
    }

    def format(self, record):
        msg = super().format(record)
        fg = self.colors.get(record.levelno)
        if fg is not None:
            msg = self.colorize(msg, fg)
        return msg


class StdLoggerService(LoggerService):

    def __init__(self, config, loglevel):
        if not config:
            config = self.get_default_config(loglevel)
        logging.config.dictConfig(config)

    @staticmethod
    def get_default_config(loglevel):
        return {
            'version': 1,
            'formatters': {
                'default': {
                    'format': '%(asctime)s %(processName)s %(levelname)s [%(name)s]: %(message)s',
                }
            },
            'handlers': {
                'console': {
                    'class': 'broccoli.logger.ColorizedStreamHandler',
                    'level': 'DEBUG',
                    'formatter': 'default'
                },
            },
            'loggers': {
                'bro': {
                    'level': loglevel,
                    'handlers': ['console']
                }
            }
        }

    def get_logger(self, name):
        return logging.getLogger(name)


class LoggerServiceComponent(Component):
    singleton = True

    def __init__(self, config=None):
        self.config = config

    def resolve(self, config: Config) -> LoggerService:
        return StdLoggerService(self.config, config.get('loglevel', 'INFO'))


class TaskLoggerComponent(Component):

    def resolve(self, service: LoggerService, task: Task) -> TaskLogger:
        return service.get_logger('bro.tasks.' + task.name)


LOGGER_COMPONENTS = [
    TaskLoggerComponent(),
    LoggerServiceComponent()
]
