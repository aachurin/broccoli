import inspect
import typing
import functools
from uuid import uuid4
from broccoli.types import App, Argument, Task as _Task


class Subtask(dict):
    __slots__ = ('app',)

    def __init__(self, task, *, args=None, kwargs=None, app=None, **options):
        self.app = app
        if isinstance(task, dict):
            super().__init__(task)  # dict(d)
        else:
            super().__init__(
                task=task,
                args=args or (),
                kwargs=kwargs or {},
                **options
            )

    def m(self, args=None, kwargs=None, **options):
        args = (self['args'] + args) if args else self['args']
        kwargs = {**self['kwargs'], **kwargs} if kwargs else self['kwargs']
        msg = {**self, **options, 'args': args, 'kwargs': kwargs}
        assert 'reply_to' not in msg, "can't use reply_to"
        return msg

    def apply(self, args=None, kwargs=None, **options):
        msg = self.m(args=args, kwargs=kwargs, **options)
        if 'result_key' not in msg:
            msg['result_key'] = str(uuid4())
        if 'id' not in msg:
            msg['id'] = str(uuid4())
        self.app.send_message(msg)
        return self.app.result(msg['result_key'])

    def delay(self, *args, **kwargs):
        return self.apply(args=args, kwargs=kwargs)

    def clone(self):
        res = self.__class__(self)
        res.app = self.app
        return res

    def __or__(self, other):
        if not isinstance(other, Subtask):
            msg = 'Subtask expected, got %r'
            raise TypeError(msg % type(other))
        new = other.clone()
        if 'subtasks' not in new:
            new['subtasks'] = (self,)
        else:
            new['subtasks'] += (self,)
        return new

    def __reduce__(self):
        return Subtask, (dict(self),)

    def __repr__(self):
        d = dict(self)
        task = d.pop('task')
        parts = []
        for k in d:
            parts.append('%s=%r' % (k, d[k]))
        return '%s.s(%s)' % (task, ', '.join(parts))


class Task(_Task):
    app: App = None
    name: str
    handler: typing.Callable
    _subtask = Subtask

    def apply(self, args=None, kwargs=None, **options):
        return self.subtask(args, kwargs, **options).apply()

    def delay(self, *args, **kwargs):
        return self.s(*args, *kwargs).delay()

    def s(self, *args, **kwargs):
        return self._subtask(self.name, args=args, kwargs=kwargs, app=self.app)

    def subtask(self, args=None, kwargs=None, **options):
        return self._subtask(self.name, args=args, kwargs=kwargs, **options, app=self.app)

    __call__ = s

    def __repr__(self):
        return repr(self.handler)

    @staticmethod
    def get_arguments(*args, **kwargs):
        raise NotImplementedError()


task_options = ('throws', 'ignore_result')


def create_task(app, func, name=None, **options):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        __log_tb_start__ = None
        return func(*args, **kwargs)

    cdict = {
        'app': app,
        'name': name or '%s.%s' % (func.__module__, func.__name__),
        'handler': staticmethod(wrapper),
        'get_arguments': staticmethod(create_get_arguments(func, app)),
        '__doc__': func.__doc__,
        '__module__': func.__module__
    }

    for k, v in options.items():
        if k not in task_options:
            msg = 'Invalid argument %s'
            raise TypeError(msg % k)
        if v is not None:
            cdict[k] = v

    task = type(func.__name__, (Task,), cdict)()

    try:
        task.__qualname__ = func.__qualname__
    except AttributeError:
        pass

    task.__name__ = func.__name__
    return task


def create_get_arguments(func, app):
    arg_names = []
    defaults = []
    kwdefaults = {}
    no_more_args = False
    signature = inspect.signature(func)
    injector = app._injector
    for param in signature.parameters.values():
        if injector.get_resolved_to(param) is not Argument:
            no_more_args = True
            continue
        if no_more_args:
            msg = 'Argument %s follows dependency on function %s.'
            raise TypeError(msg % (param.name, func.__qualname__))
        if param.kind == param.POSITIONAL_OR_KEYWORD:
            if param.default is not param.empty:
                defaults += [param.default]
        elif param.kind == param.KEYWORD_ONLY:
            if '*' not in arg_names:
                arg_names += ['*']
            if param.default is not param.empty:
                kwdefaults[param.name] = param.default
        elif param.kind == param.VAR_POSITIONAL:
            msg = 'Variadic arguments in form *%s are not allowed.'
            raise TypeError(msg % param.name)
        elif param.kind == param.VAR_KEYWORD:
            msg = 'Keyword arguments in form **%s are not allowed.'
            raise TypeError(msg % param.name)
        else:
            msg = 'Parameter kind %s is not supported'
            raise TypeError(msg % param.kind)
        arg_names += [param.name]

    args = (
        func.__name__,
        ', '.join(arg_names),
        ', '.join(['%r:%s' % (x, x) for x in arg_names if x != '*'])
    )
    code = 'def %s(%s):\n  return {%s}' % args
    dct = {}
    exec(code, dct, dct)

    get_arguments = dct[func.__name__]
    if defaults:
        get_arguments.__defaults__ = tuple(defaults)
    if kwdefaults:
        get_arguments.__kwdefaults__ = kwdefaults
    get_arguments.__module__ = func.__module__

    return get_arguments
