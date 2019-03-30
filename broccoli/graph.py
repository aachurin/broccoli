import uuid
from broccoli.task import Subtask
from broccoli.traceback import Traceback


class Fut:
    def __await__(self):
        return (yield)


def co_send(coro, value=None):
    try:
        coro.send(value)
    except StopIteration as stop:
        return stop.value


def co_throw(coro, exc):
    try:
        coro.throw(exc)
    except StopIteration as stop:
        return stop.value


class Graph(dict):
    __slots__ = ('id', '_coro', '_pending')

    S = Subtask

    def __init__(self, message):
        self.id = message['id']
        self._coro = None
        self._pending = []
        self._add_subtasks(message)

    def __await__(self):
        __log_tb_start__ = None
        return (yield self)

    def set_coroutine(self, coro):
        self._coro = coro

    def close(self):
        self._coro = None

    def get_pending_messages(self):
        if self._pending:
            messages, self._pending = self._pending, []
            return messages
        return []

    def run_reply(self, reply):
        if 'exc' in reply:
            exc = reply['exc']
            traceback = reply.get('traceback')
            if traceback:
                exc.__cause__ = Traceback(traceback)
            self._pending.append(co_throw(self._coro, exc))
        else:
            for coro in self.pop(reply['reply_id']):
                args = co_send(coro, reply.get('value'))
            if not self:
                self._pending.append(co_send(self._coro, args))

    def _add_subtasks(self, subtask, subtask_id=None, memo=None):
        if memo is None:
            memo = {}
        subtasks = subtask.get('subtasks')
        if subtasks:
            uuid4 = uuid.uuid4
            coro = self._wait_subtasks(subtask, len(subtasks), subtask_id)
            for subtask in subtasks:
                memo_key = id(subtask)
                if memo_key in memo:
                    self[memo[memo_key]].append(coro)
                else:
                    subtask_id = str(uuid4())
                    memo[memo_key] = subtask_id
                    self[subtask_id] = [coro]
                    if not self._add_subtasks(subtask, subtask_id, memo):
                        self._pending.append(subtask.m(id=subtask_id, graph_id=self.id))
            co_send(coro)
            return True
        return False

    async def _wait_subtasks(self, subtask, nargs, subtask_id=None, fut=Fut()):
        args = []
        while nargs > 0:
            args.append(await fut)
            nargs -= 1
        if subtask_id is not None:
            self._pending.append(subtask.m(args=args, id=subtask_id, graph_id=self.id))
        else:
            return args
