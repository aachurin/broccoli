class WorkerInterrupt(BaseException):
    pass


class Shutdown(BaseException):
    pass


class Reject(Exception):
    pass
