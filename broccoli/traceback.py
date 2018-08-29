import sys
from traceback import StackSummary


class Traceback(Exception):

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
    while tb is not None:
        if track:
            if '__log_tb_stop__' in tb.tb_frame.f_locals:
                break
            yield (tb.tb_frame, tb.tb_lineno)
        elif '__log_tb_start__' in tb.tb_frame.f_locals:
            track = True
        tb = tb.tb_next


def extract_log_tb(exc=None):
    tb = ''.join(StackSummary.extract(walk_tb(sys.exc_info()[-1])).format())
    if exc.__cause__ is not None and isinstance(exc.__cause__, Traceback):
        tb = exc.__cause__.tb + tb
    return tb
