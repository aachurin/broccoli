import re
import sys
import click
import importlib
import inspect
from collections import OrderedDict
from . exceptions import ConfigurationError
from . app import App
from . utils import get_colorizer, color


class Command(click.Command):

    def get_params(self, ctx):
        self.params = ctx.obj.get_options()
        return super().get_params(ctx)


class CLI(click.Group):

    def command(self, *args, **kwargs):
        return super().command(*args, **kwargs, cls=Command)


@click.command(cls=CLI)
@click.option('-A', '--app', default='taskapp.taskapp', help='Application instance')
@click.pass_context
def cli(ctx, app):
    sys.path[0:0] = ['.']

    try:
        module, attr = app.rsplit('.', 1)
    except ValueError:
        click.echo(click.style('✘', fg='red') + ' Invalid application instance "%s"' % app)
        sys.exit(1)

    try:
        module = importlib.import_module(module)
    except ImportError:
        click.echo(click.style('✘', fg='red') + ' Could not load application "%s"' % app)
        raise

    try:
        instance = getattr(module, attr)
    except AttributeError:
        click.echo(click.style('✘', fg='red') + ' Could not load application "%s"' % app)
        sys.exit(1)

    if not isinstance(instance, App):
        click.echo(click.style('✘', fg='red') + ' Invalid application instance "%s"' % app)
        sys.exit(1)

    ctx.obj = Configurator(instance)


@cli.command()
@click.pass_obj
def worker(configurator, **kwargs):
    configurator.apply_config(kwargs)
    configurator.show_startup_info()
    configurator.app.serve()


class Configurator():

    def __init__(self, app):
        self.app = app

    def get_configurable(self):
        return (
            [self.app.router, self.app.broker, self.app.worker] +
            self.app.injector.components
        )

    def get_options(self):
        options = OrderedDict()
        for obj in self.get_configurable():
            if not hasattr(obj, 'configure'):
                continue
            for identity, option in self.get_cli_options(obj):
                if identity in options:
                    if options[identity].type != option.type:
                        msg = 'option "%s" got multiple types'
                        raise ConfigurationError(msg % identity)
                    continue
                options[identity] = option
        return list(options.values())

    def apply_config(self, config):
        for obj in self.get_configurable():
            if not hasattr(obj, 'configure'):
                continue
            parameters = inspect.signature(obj.configure).parameters
            kwargs = {key: config[key] for key in parameters.keys() if key in config}
            try:
                obj.configure(**kwargs)
            except (ValueError, TypeError) as e:
                raise click.ClickException(str(e))

    @staticmethod
    def get_description(obj):
        doc = getattr(obj, '__doc__')

        if doc is None:
            return {}

        param_names = inspect.signature(obj.configure).parameters.keys()
        param_docs = {}
        for param_name in param_names:
            match = re.search(r'^\W*' + param_name + r'\W*(.*)$', doc, re.MULTILINE)
            if match:
                param_docs[param_name] = match.groups()[0]
            else:
                param_docs[param_name] = ''

        return param_docs

    def get_cli_options(self, obj):
        parameter_descriptions = self.get_description(obj)
        parameters = inspect.signature(obj.configure).parameters
        options = []
        for param_name, param in parameters.items():
            description = parameter_descriptions.get(param_name, '')
            annotation = param.annotation
            name = param_name.replace('_', '-')
            if annotation is inspect.Parameter.empty:
                annotation = str
            if issubclass(annotation, (str, int, float, bool)):
                options.append((name,
                                click.Option(('--%s' % name,),
                                             help=description,
                                             type=annotation,
                                             required=False)))
        return options

    def show_startup_info(self):
        index = 0
        indent = 0
        max_height = 16
        colorize = get_colorizer()

        def vshift():
            nonlocal index, indent
            index = 0
            if not lines:
                indent = 0
            else:
                indent = max(l[0] for l in lines) + 1

        def echo(text, endline=True, header=False, color=color.aqua):
            nonlocal index
            if index >= max_height:
                vshift()
            elif header and (index + 1) >= max_height:
                vshift()
            while index >= len(lines):
                lines.append([0, ''])
            line = lines[index]
            if line[0] < indent:
                line[1] += ' ' * (indent - line[0])
                line[0] = indent
            line[0] += len(text) + endline
            line[1] += colorize(text, color) + (' ' if endline else '')
            if endline:
                index += 1

        def echo_kv(key, value, max_width=35):
            key = '. ' + key + ': '
            echo(key, color=color.tea, endline=False)
            if isinstance(value, (list, tuple)):
                value = ', '.join(value)
            value = str(value)
            rest = max_width - len(key)
            indent = ''
            while value:
                echo(indent + value[:rest], color=color.olive)
                value = value[rest:]
                rest = max_width - 4
                indent = '    '

        def echo_conf(conf):
            if not conf:
                echo('. enabled', color=color.tea)
            for key, value in conf.items():
                echo_kv(key, value)

        lines = [
            [0, 0, 0, 0, 0, 0, 0, 233, 233, 58, 58, 149, 107, 149, 149, 106, 106,
                149, 149, 149, 107, 107, 64, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 234, 58, 106, 106, 106, 106, 106, 106, 106, 106, 106, 106,
                106, 106, 106, 106, 106, 106, 106, 106, 149, 64, 235, 0, 0, 0, 0,
                0, 0, 0],
            [0, 0, 235, 70, 106, 106, 106, 106, 106, 106, 106, 70, 106, 64, 106,
                64, 106, 70, 106, 64, 106, 106, 106, 106, 106, 106, 106, 106, 149,
                233, 0, 0, 0, 0],
            [0, 234, 70, 64, 106, 64, 64, 64, 64, 64, 64, 64, 70, 64, 64, 64, 64,
                64, 64, 64, 64, 106, 106, 106, 106, 70, 106, 106, 106, 235, 0, 0,
                0, 0],
            [0, 235, 70, 64, 64, 58, 22, 236, 235, 64, 64, 64, 64, 64, 64, 64, 64,
                64, 64, 64, 64, 64, 70, 70, 64, 70, 70, 70, 106, 106, 106, 232, 0,
                0],
            [0, 237, 64, 64, 64, 22, 237, 58, 234, 236, 106, 236, 22, 58, 236, 58,
                106, 236, 64, 64, 64, 64, 64, 64, 64, 64, 64, 70, 64, 106, 106, 58,
                0, 0],
            [0, 235, 64, 64, 64, 22, 22, 235, 149, 237, 106, 100, 238, 58, 149,
                100, 58, 22, 236, 236, 64, 64, 64, 64, 64, 64, 64, 70, 64, 64, 106,
                106, 234, 0],
            [0, 0, 0, 0, 236, 22, 22, 22, 3, 149, 235, 149, 149, 238, 237, 149,
                236, 149, 149, 236, 236, 3, 22, 237, 236, 236, 235, 235, 235, 22,
                106, 106, 64, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 237, 143, 149, 149, 58, 106, 149, 143, 106,
                58, 149, 149, 3, 58, 3, 106, 100, 235, 22, 22, 22, 22, 2, 70, 238,
                0],
            [0, 0, 0, 0, 0, 0, 0, 235, 149, 149, 149, 149, 106, 149, 149, 149, 149,
                106, 149, 149, 149, 149, 100, 100, 58, 235, 22, 22, 22, 22, 64,
                234, 0, 0],
            [0, 0, 0, 0, 0, 0, 235, 149, 149, 149, 149, 100, 149, 149, 149, 149,
                106, 149, 149, 58, 238, 22, 22, 22, 22, 22, 22, 22, 22, 22, 58,
                235, 0, 0],
            [0, 0, 0, 0, 233, 3, 149, 149, 149, 149, 149, 149, 149, 149, 149, 149,
                149, 237, 0, 0, 0, 0, 0, 0, 237, 234, 234, 234, 234, 234, 0, 0, 0,
                0],
            [0, 0, 0, 235, 58, 235, 149, 149, 149, 149, 149, 149, 149, 149, 149,
                236, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 237, 228, 228, 143, 143, 58, 58, 149, 149, 149, 100, 234, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 58, 228, 228, 228, 234, 236, 149, 235, 0, 0, 0,
                'b', 'r', 'o', 'c', 'c', 'o', 'l', 'i', ' ', 'v', '0', '.', '1',
                0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 232, 237, 235, 58, 0, 0, 0, 0, 0, '(', 's', 't', 'a', 'r', 't',
                'e', 'r', ' ', 'k', 'i', 't', ')', 0, 0, 0, 0]
        ]

        if colorize.support_colors:
            lines = [[len(line), (''.join([colorize(
                        ' ' if c == 0 else (c if type(c) is str else '#'),
                        c if type(c) is int else color.grey
                     ) for c in line]))] for line in lines]
        else:
            lines = []

        print()
        vshift()

        for obj in self.get_configurable():
            if not hasattr(obj, 'get_configuration'):
                continue
            config = obj.get_configuration()
            if hasattr(obj, 'resolve'):
                header = inspect.signature(obj.resolve).return_annotation.__name__
            else:
                header = type(obj).__name__
            echo('[%s]' % header, header=True, color=color.white)
            echo_conf(config)
            echo('')

        print('\n'.join(l[1] for l in lines))
        print()
