import inspect
from broccoli.types import App, AppVar, Context
from broccoli.components import Component, Optional


class AppVarComponent(Component):

    # noinspection PyMethodOverriding
    def resolve(self,
                parameter: inspect.Parameter,
                app: App) -> AppVar:
        name = parameter.name
        if name not in app.context:
            if parameter.default is parameter.empty:
                msg = 'Missing required context var %r'
                raise TypeError(msg % name)
            return parameter.default
        return app.context[name]


class OptionalComponent(Component):

    def can_handle_parameter(self, parameter: inspect.Parameter):
        return (isinstance(parameter.annotation, type)
                and issubclass(parameter.annotation, Optional)
                and parameter.default is not parameter.empty)

    # noinspection PyMethodOverriding
    def resolve(self, parameter: inspect.Parameter):
        return parameter.default


class SimpleContext:
    pass


class ContextComponent(Component):

    def resolve(self) -> Context:
        return SimpleContext()


components = [
    AppVarComponent(),
    ContextComponent(),
    OptionalComponent(),
]
