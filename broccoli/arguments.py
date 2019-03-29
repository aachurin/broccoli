import inspect
from broccoli.types import Arguments, Argument
from broccoli.components import Component


class ArgumentComponent(Component):

    def can_handle_parameter(self, parameter: inspect.Parameter):
        return True

    def resolve(self, args: Arguments, parameter: inspect.Parameter) -> Argument:
        if args is None:
            msg = "Parameter %s is not yet available."
            raise TypeError(msg % parameter.name)
        try:
            return args[parameter.name]
        except KeyError:
            msg = "Parameter named %s is not set."
            raise TypeError(msg % parameter.name)


ARGUMENT_COMPONENTS = [
    ArgumentComponent()
]
