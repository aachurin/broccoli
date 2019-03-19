from broccoli.types import App
from broccoli.traceback import Traceback


class AsyncResult:

    def __init__(self, app: App, result_key: str) -> None:
        self.app = app
        self.result_key = result_key

    def wait(self, raise_exception=True, timeout=None):
        if self.result_key is None:
            msg = 'Task has no result'
            raise TypeError(msg)
        # noinspection PyUnresolvedReferences
        result = self.app._broker.get_result(self.result_key, timeout=timeout)
        if result is None:
            raise TimeoutError()
        elif result.get('exc'):
            if raise_exception:
                tb = Traceback(result['traceback']) if result.get('traceback') else None
                raise result['exc'] from tb
            return result['exc']
        else:
            return result['value']

    def __repr__(self):
        return '%s(result_key=%r)' % (self.__class__.__name__, self.result_key)
