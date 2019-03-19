import uuid
from broccoli.types import App
from broccoli.task import Signature
from broccoli.traceback import Traceback


class Fut:
    def __await__(self):
        return (yield)


def co_send(coro, value=None):
    try:
        coro.send(value)
    except StopIteration:
        return False
    return True


def co_throw(coro, exc):
    try:
        coro.throw(exc)
    except StopIteration:
        return False
    return True


class Graph(dict):

    signature = Signature
    on_complete = None

    def __init__(self, app: App, graph_id):
        self.app = app
        self.id = graph_id

    def __await__(self):
        return (yield self)

    def set_complete(self, reply):
        waiters = self.pop(reply['id'])
        if 'exc' in reply:
            exc = reply['exc']
            traceback = reply.get('traceback')
            if traceback:
                exc.__cause__ = Traceback(traceback)
            co_throw(waiters[0][0], exc)
        else:
            for coro, key, is_arg in waiters:
                co_send(coro, (key, reply['value'], is_arg))

    def build(self, sig, subtask_id=None, memo=None):
        if memo is None:
            memo = {}
        signature = self.signature
        uuid4 = uuid.uuid4
        args = sig.get('args')
        kwargs = sig.get('kwargs')
        coro = self._await_subtasks(sig, subtask_id)

        wait_args = []
        if args:
            sig['args'] = list(args)
            wait_args += [(key, s, True) for key, s in enumerate(args) if isinstance(s, signature)]
        if kwargs:
            wait_args += [(key, s, False) for key, s in kwargs.items() if isinstance(s, signature)]

        sig['_nargs'] = len(wait_args)
        if wait_args:
            for key, sig, is_arg in wait_args:
                memo_key = id(sig)
                if memo_key in memo:
                    self[memo[memo_key]].append((coro, key, is_arg))
                else:
                    sid = str(uuid4())
                    memo[memo_key] = sid
                    self[sid] = [(coro, key, is_arg)]
                    self.build(sig, sid, memo)

        return co_send(coro)

    async def _await_subtasks(self, sig, subtask_id=None, fut=Fut()):
        nargs = sig.pop('_nargs')
        args = sig.get('args')
        kwargs = sig.get('kwargs')
        while nargs > 0:
            try:
                key, value, is_arg = await fut
            except Exception as exc:
                return co_throw(self.on_complete, exc)
            if is_arg:
                args[key] = value
            else:
                kwargs[key] = value
            nargs -= 1
        if subtask_id is not None:
            msg = dict(sig, id=subtask_id, graph_id=self.id)
            self.app.send_message(msg, reply_back=True)
        elif self.on_complete:
            co_send(self.on_complete, sig)


def build_graph(app, sig, graph=Graph):
    g = graph(app, sig['id'])
    if g.build(sig):
        return g
