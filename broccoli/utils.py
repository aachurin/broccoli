import os
import sys


class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.
    Optional ``name`` argument allows you to make cached properties of other
    methods. (e.g.  url = cached_property(get_absolute_url, name='url') )
    """
    def __init__(self, func, name=None):
        self.func = func
        self.__doc__ = getattr(func, '__doc__')
        self.name = name or func.__name__

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


class color:
    black = 0
    maroon = 1
    green = 2
    olive = 3
    navy = 4
    purple = 5
    tea = 6
    silver = 7
    grey = 8
    red = 9
    lime = 10
    yellow = 11
    blue = 12
    fuchsi = 13
    aqua = 14
    white = 15


def get_colorizer():
    if not sys.stdout.isatty() or os.environ.get('NOCOLORS'):
        return _fake_colorizer
    return _simple_colorizer


def _fake_colorizer(text, _):
    return text


_fake_colorizer.support_colors = False  # type: ignore


def _simple_colorizer(text, color):
    return '\x1b[38;5;%dm%s\x1b[0m' % (color, text)


_simple_colorizer.support_colors = True  # type: ignore
