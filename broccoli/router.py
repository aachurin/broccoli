import typing
from broccoli.types import Router
from broccoli.components import Component


class SimpleRouter(Router):

    def __init__(self, task_routes, default_queue):
        self.task_routes = task_routes
        self.default_queue = default_queue

    def get_queue(self, task_name) -> str:
        return self.task_routes.get(task_name, self.default_queue)


class SimpleRouterComponent(Component):
    singleton = True

    __slots__ = ('config',)

    def __init__(self,
                 task_routes: typing.Dict[str, str] = None,
                 default_queue: str = 'default') -> None:
        self.config = {
            'task_routes': task_routes or {},
            'default_queue': default_queue
        }

    def resolve(self) -> Router:
        return SimpleRouter(**self.config)


ROUTER_COMPONENTS = [
    SimpleRouterComponent()
]
