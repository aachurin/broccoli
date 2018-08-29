import time
import logging
from . interfaces import Logger
from . utils import get_colorizer, color


class ConsoleLogger(Logger):

    debug_color = color.silver
    info_color = color.white
    warning_color = color.olive
    error_color = color.maroon
    critical_color = color.red

    LEVELS = {
        'CRITICAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0,
    }

    LEVELS_REVERSE = {
        v: k for k, v in LEVELS.items()
    }

    def __init__(self, level) -> None:
        self.colorize = get_colorizer()
        self.format = self.get_format()
        self.level = level

    @staticmethod
    def get_format():
        def formatter(level, text):
            return '[%.3f %s] - %s' % (
                time.time(), level, text)
        return formatter

    def debug(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('DEBUG', msg),
                            self.debug_color))

    def info(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('INFO', msg),
                            self.info_color))

    def warning(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('WARNING', msg),
                            self.warning_color))

    def error(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('ERROR', msg),
                            self.error_color))

    def critical(self, msg, *args) -> None:
        msg = msg % args if args else msg
        print(self.colorize(self.format('CRITICAL', msg),
                            self.critical_color))

    def setLevel(self, level: int) -> None:
        # noinspection PyUnusedLocal
        def unlogged(msg, *args):
            pass
        for lvl in ('debug', 'info', 'warning', 'error'):
            # noinspection PyProtectedMember
            if level > logging._nameToLevel[lvl.upper()]:
                setattr(self, lvl, unlogged)
            else:
                self.__dict__.pop(lvl, None)
        self.level = level
