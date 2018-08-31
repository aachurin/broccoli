import re
import sys
import click
import typing
import importlib
import inspect
from collections import OrderedDict
from . interfaces import App, Configurable
from . exceptions import ConfigurationError
from . splash import splash


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

    def __init__(self, app: App) -> None:
        self.app = app

    def get_configurables(self) -> typing.List[Configurable]:
        check = (
            [self.app.router, self.app.broker, self.app.worker]
            + self.app.injector.components
            + self.app.hooks
        )
        return [obj for obj in check if isinstance(obj, Configurable)]

    def get_options(self):
        options = OrderedDict()
        for obj in self.get_configurables():
            for identity, option in self.get_cli_options(obj):
                if identity in options:
                    if options[identity].type != option.type:
                        msg = 'option "%s" got multiple types'
                        raise ConfigurationError(msg % identity)
                    continue
                options[identity] = option
        return list(options.values())

    def apply_config(self, config):
        for obj in self.get_configurables():
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
        configs = []
        seen = set()
        for obj in self.get_configurables():
            key, config = obj.get_configuration()
            if key not in seen:
                seen.add(key)
                configs.append((key, config))
        splash(configs)
