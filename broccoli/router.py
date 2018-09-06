import typing
from . types import Configurable, Router


class SimpleRouter(Configurable,
                   Router):
    """
    default_queue  Default task sending queue
    """

    default_queue: str

    def __init__(self,
                 task_routes: typing.Dict[str, str]=None,
                 default_queue: str = 'default'):
        self.task_routes = task_routes or {}
        self.configure(default_queue=default_queue)

    def get_queue(self, task_name) -> str:
        return self.task_routes.get(task_name, self.default_queue)

    def configure(self, default_queue: str = 'default') -> None:
        if default_queue is not None:
            if not default_queue:
                raise ValueError('default_queue is required')
            self.default_queue = default_queue

    def get_configuration(self):
        return 'Router', {
            'class': self.__class__.__name__,
            'default_queue': self.default_queue
        }
