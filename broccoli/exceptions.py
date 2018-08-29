
class ConfigurationError(Exception):
    pass


class TimedOut(Exception):
    pass


class BrokerError(Exception):
    pass


class BrokerResultLocked(Exception):
    pass


class TaskNotFound(Exception):
    pass


class TaskInterrupt(BaseException):
    pass


class WorkerInterrupt(BaseException):
    pass


class WarmShutdown(BaseException):
    def __init__(self, signal):
        self.signal = signal


class ColdShutdown(BaseException):
    def __init__(self, signal):
        self.signal = signal
