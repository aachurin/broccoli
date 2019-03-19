import inspect
import typing
import functools
from uuid import uuid4
from broccoli import exceptions
from broccoli.types import App


class Signature(dict):

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

    def apply(self, args=None, kwargs=None, **options):
        args = (self['args'] + args) if args else self['args']
        kwargs = {**self['kwargs'], **kwargs} if kwargs else self['kwargs']
        msg = {**self, **options, 'args': args, 'kwargs': kwargs}
        assert 'reply_to' not in msg, "can't use reply_to"
        if 'id' not in msg:
            msg['id'] = str(uuid4())
        if 'result_key' not in msg:
            msg['result_key'] = str(uuid4())
        self.app.send_message(msg)
        return self.app.result(msg['result_key'])

    def delay(self, *args, **kwargs):
        return self.apply(args=args, kwargs=kwargs)

    def __reduce__(self):
        return Signature, (dict(self),)

    def __repr__(self):
        d = dict(self)
        task = d.pop('task')
        parts = []
        for k in d:
            parts.append('%s=%r' % (k, d[k]))
        return '%s.s(%s)' % (task, ', '.join(parts))


class Task:

    app: App = None
    name: str
    handler: typing.Callable

    _signature = Signature

    # Tuple of expected exceptions.
    throws = ()
    ignore_result = False

    def apply(self, args=None, kwargs=None, id=None, result_key=None, **options):
        id = id or str(uuid4())
        result_key = result_key or str(uuid4())
        msg = {'id': id, 'task': self.name, 'result_key': result_key, **options}
        assert 'reply_to' not in msg, "can't use reply_to"
        if args is not None:
            msg['args'] = args
        if kwargs is not None:
            msg['kwargs'] = kwargs
        self.app.send_message(msg)
        return self.app.result(result_key)

    def delay(self, *args, **kwargs):
        return self.apply(args=args, kwargs=kwargs)

    def s(self, *args, **kwargs):
        return self._signature(self.name, args=args, kwargs=kwargs, app=self.app)

    def signature(self, args=None, kwargs=None, **options):
        return self._signature(self.name, args=args, kwargs=kwargs, **options, app=self.app)

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    def __repr__(self):
        return repr(self.handler)

    @staticmethod
    def func_args_guard(*_, **__):
        return (), {}


task_options = ('throws', 'ignore_result')


def create_task(app, func, name=None, **kwargs):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        __log_tb_start__ = None
        return func(*args, **kwargs)

    cdict = {
        'app': app,
        'name': name or '%s.%s' % (func.__module__, func.__name__),
        'handler': staticmethod(wrapper),
        'func_args_guard': staticmethod(create_func_args_guard(func, app)),
        '__doc__': func.__doc__,
        '__module__': func.__module__
    }

    for k, v in kwargs.items():
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


def create_func_args_guard(func, app):
    funcargs = []
    defaults = []
    kwdefaults = {}
    annotations = {}

    no_more_args = False
    var_args = False

    signature = inspect.signature(func)

    if signature.return_annotation is not inspect.Parameter.empty:
        annotations['return'] = signature.return_annotation

    for parameter in signature.parameters.values():

        if app.injector.get_component_class(parameter) is not None:
            no_more_args = True
            continue

        parameter_name = parameter.name

        if no_more_args:
            msg = 'Argument "%s" follows dependency on function "%s".'
            raise exceptions.ConfigurationError(msg % (parameter_name, func.__qualname__))

        if parameter.annotation is not inspect.Parameter.empty:
            annotations[parameter_name] = parameter.annotation

        if parameter.kind == inspect.Parameter.KEYWORD_ONLY and not var_args:
            funcargs.append('*')
            var_args = True

        if parameter.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
            if parameter.default is not inspect.Parameter.empty:
                defaults.append(parameter.default)

        elif parameter.kind == inspect.Parameter.KEYWORD_ONLY:
            if parameter.default is not inspect.Parameter.empty:
                kwdefaults[parameter_name] = parameter.default

        elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            var_args = True
            parameter_name = '*' + parameter_name

        elif parameter.kind == inspect.Parameter.VAR_KEYWORD:
            parameter_name = '**' + parameter_name
        else:
            msg = 'Parameter kind %s is not supported'
            raise TypeError(msg % parameter.kind)

        funcargs.append(parameter_name)

    code = 'def %s(%s):\n  pass'
    code = code % (func.__name__, ', '.join(funcargs))
    dct = {}
    exec(code, dct, dct)
    adapter = dct[func.__name__]

    if defaults:
        adapter.__defaults__ = tuple(defaults)

    if kwdefaults:
        adapter.__kwdefaults__ = kwdefaults

    adapter.__annotations__ = annotations
    adapter.__module__ = func.__module__

    return adapter
