# Based on original code:
# https://github.com/encode/apistar/blob/master/apistar/server/injector.py

import asyncio
import inspect
import typing
from . exceptions import ConfigurationError


class BaseInjector():

    def run(self, func, state):
        raise NotImplementedError()


class Injector(BaseInjector):

    allow_async = False

    def __init__(self, components, initial):
        self.components = components
        self.initial = dict(initial)
        self.reverse_initial = {
            val: key for key, val in initial.items()
        }
        self.resolver_cache = {}

    def resolve_function(self, func, output_name=None, seen_state=None, parent_parameter=None, parent_function=None,
                         set_return=False):
        if seen_state is None:
            seen_state = set(self.initial)

        steps = []
        kwargs = {}
        consts = {}

        signature = inspect.signature(func)

        if output_name is None:
            if signature.return_annotation in self.reverse_initial:
                output_name = self.reverse_initial[signature.return_annotation]
            else:
                output_name = 'return_value'

        for parameter in signature.parameters.values():
            #if parameter.annotation in (int, float)
            if parameter.annotation is ReturnValue:
                kwargs[parameter.name] = 'return_value'
                continue

            # Check if the parameter class exists in 'initial'.
            if parameter.annotation in self.reverse_initial:
                initial_kwarg = self.reverse_initial[parameter.annotation]
                kwargs[parameter.name] = initial_kwarg
                continue

            # The 'Parameter' annotation can be used to get the parameter
            # itself.
            if parameter.annotation is inspect.Parameter:
                consts[parameter.name] = parent_parameter
                continue

            # The 'Function' annotation can be used to get the parent function.
            if parameter.annotation is Function:
                consts[parameter.name] = parent_function
                continue

            # Otherwise, find a component to resolve the parameter.
            for component in self.components:
                if component.can_handle_parameter(parameter):
                    identity = component.identity(parameter)
                    kwargs[parameter.name] = identity
                    if identity not in seen_state:
                        seen_state.add(identity)
                        steps += self.resolve_function(
                            func=component.resolve,
                            output_name=identity,
                            seen_state=seen_state,
                            parent_parameter=parameter,
                            parent_function=func
                        )
                    break
            else:
                msg = 'No component able to handle parameter "%s" on function "%s".'
                raise ConfigurationError(msg % (parameter.name, func.__name__))

        is_async = asyncio.iscoroutinefunction(func)
        if is_async and not self.allow_async:
            msg = 'Function "%s" may not be async.'
            raise ConfigurationError(msg % (func.__name__, ))

        step = (func, is_async, kwargs, consts, output_name, set_return)
        steps.append(step)
        return steps

    def resolve_functions(self, funcs):
        steps = []
        seen_state = set(self.initial)
        for func in funcs:
            func_steps = self.resolve_function(func, seen_state=seen_state, set_return=True)
            steps.extend(func_steps)
        return steps

    def run(self, funcs, state, cache=True):
        funcs = tuple(funcs)
        try:
            steps = self.resolver_cache[funcs]
        except KeyError:
            if not funcs:
                return
            steps = self.resolve_functions(funcs)
            if cache:
                self.resolver_cache[funcs] = steps

        output_name: str
        for func, is_async, kwargs, consts, output_name, set_return in steps:
            func_kwargs = {key: state[val] for key, val in kwargs.items()}
            func_kwargs.update(consts)
            state[output_name] = func(**func_kwargs)
            if set_return:
                state['return_value'] = state[output_name]

        return state[output_name]

    def clear_cache(self, funcs):
        funcs = tuple(funcs)
        try:
            del self.resolver_cache[funcs]
        except KeyError:
            pass


class ASyncInjector(Injector):

    allow_async = True

    async def run_async(self, funcs, state, cache=True):
        funcs = tuple(funcs)
        try:
            steps = self.resolver_cache[funcs]
        except KeyError:
            if not funcs:
                return
            steps = self.resolve_functions(funcs)
            if cache:
                self.resolver_cache[funcs] = steps

        output_name: str
        for func, is_async, kwargs, consts, output_name, set_return in steps:
            func_kwargs = {key: state[val] for key, val in kwargs.items()}
            func_kwargs.update(consts)
            if is_async:
                state[output_name] = await func(**func_kwargs)
            else:
                state[output_name] = func(**func_kwargs)
            if set_return:
                state['return_value'] = state[output_name]

        return state[output_name]


class Component():

    def identity(self, parameter: inspect.Parameter):
        """
        Each component needs a unique identifier string that we use for lookups
        from the `state` dictionary when we run the dependency injection.
        """
        parameter_name = parameter.name.lower()
        annotation_name = parameter.annotation.__name__.lower()

        # If `resolve_parameter` includes `Parameter` then we use an identifier
        # that is additionally parameterized by the parameter name.
        args = inspect.signature(self.resolve).parameters.values()
        if inspect.Parameter in [arg.annotation for arg in args]:
            return annotation_name + ':' + parameter_name

        # Standard case is to use the class name, lowercased.
        return annotation_name

    def can_handle_parameter(self, parameter: inspect.Parameter):
        # Return `True` if this component can handle the given parameter.
        #
        # The default behavior is for components to handle whatever class
        # is used as the return annotation by the `resolve` method.
        #
        # You can override this for more customized styles, for example if you
        # wanted name-based parameter resolution, or if you want to provide
        # a value for a range of different types.
        #
        # Eg. Include the `Request` instance for any parameter named `request`.
        return_annotation = inspect.signature(self.resolve).return_annotation
        if return_annotation is inspect.Signature.empty:
            msg = (
                'Component "%s" must include a return annotation on the '
                '`resolve()` method, or override `can_handle_parameter`'
            )
            raise ConfigurationError(msg % self.__class__.__name__)
        return parameter.annotation is return_annotation

    def resolve(self, *args, **kwargs):
        raise NotImplementedError()


ReturnValue = typing.TypeVar('ReturnValue')
Function = typing.TypeVar('Function')
