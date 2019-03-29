import sys
from traceback import StackSummary


class Traceback(Exception):
    __slots__ = ('tb',)

    def __init__(self, tb):
        self.tb = tb

    def __str__(self):
        return '\n\nTraceback (most recent call last):\n' + self.tb.rstrip()


def walk_tb(tb):
    """Walk a traceback yielding the frame and line number for each frame.
    This will follow tb.tb_next (and thus is in the opposite order to
    walk_stack). Usually used with StackSummary.extract.
    """
    track = False
    result = []
    while tb is not None:
        if track:
            result.append((tb.tb_frame, tb.tb_lineno))
        if '__log_tb_start__' in tb.tb_frame.f_locals:
            result = []
            track = True
        tb = tb.tb_next
    return result


def extract_log_tb(exc=None):
    tb = ''.join(StackSummary.extract(walk_tb(sys.exc_info()[-1])).format())
    if exc.__cause__ is not None and isinstance(exc.__cause__, Traceback):
        tb = exc.__cause__.tb + tb
    return tb
