import os
import re
import sys
import importlib


class default:
    """
    default value wrapper
    """
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value


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


def import_path(path):
    module, attr = path.rsplit('.', 1)
    return getattr(importlib.import_module(module), attr)


def validate(value, msg, type=None, coerce=None,
             min_value=None, max_value=None, regex=None,
             min_length=None, max_length=None):
    if coerce is not None:
        try:
            value = coerce(value)
        except (ValueError, TypeError):
            raise ValueError(msg)
    if type is not None and not isinstance(value, type):
        raise ValueError(msg)
    try:
        if regex is not None and not re.match(regex, str(value)):
            raise ValueError('not match pattern %r' % regex)
        if min_value is not None and value < min_value:
            raise ValueError('minimum value is %s' % min_value)
        if max_value is not None and value > max_value:
            raise ValueError('maximum value is %s' % max_value)
        if min_length is not None and len(value) < min_length:
            raise ValueError('minimum length is %s' % min_length)
        if max_length is not None and len(value) > max_length:
            raise ValueError('maximum length is %s' % max_length)
    except ValueError as exc:
        raise ValueError(msg + ': ' + str(exc))
    except TypeError:
        raise ValueError(msg)
    return value


class color:
    black = '0;30'
    red = '0;31'
    green = '0;32'
    yellow = '0;33'
    blue = '0;34'
    purple = '0;35'
    cyan = '0;36'
    white = '0;37'
    gray = '1;30'
    light_red = '1;31'
    light_green = '1;32'
    light_yellow = '1;33'
    light_blue = '1;34'
    light_purple = '1;35'
    light_cyan = '1;36'
    light_white = '1;37'


def get_colorizer():
    if not sys.stdout.isatty() or os.environ.get('NOCOLORS'):
        return _fake_colorizer
    return _simple_colorizer


def _fake_colorizer(text, _):
    return text


_fake_colorizer.support_colors = False  # type: ignore


def _simple_colorizer(text, fg):
    return '\x1b[%sm%s\x1b[0m' % (fg, text)


_simple_colorizer.support_colors = True  # type: ignore
