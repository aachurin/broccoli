import uuid
from broccoli.task import Subtask
from broccoli.traceback import Traceback


class Fut:
    def __await__(self):
        return (yield)


class Graph(dict):
    __slots__ = ('id', '_coro', '_pending', 'complete')

    S = Subtask

    def __init__(self, message):
        self.id = message['id']
        self._coro = None
        self._pending = []
        self._build(message)
        self.complete = not self._pending

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
        waiters = self.pop(reply['reply_id'])
        if 'exc' in reply:
            exc = reply['exc']
            traceback = reply.get('traceback')
            if traceback:
                exc.__cause__ = Traceback(traceback)
            try:
                self._coro.throw(exc)
            except StopIteration as ret:
                self._pending.append(ret.value)
        else:
            for coro, key, is_arg in waiters:
                try:
                    coro.send((key, reply.get('value'), is_arg))
                except StopIteration:
                    pass

            if not self:
                try:
                    self._coro.send(None)
                except StopIteration as ret:
                    self._pending.append(ret.value)

    def _build(self, subtask, subtask_id=None, memo=None):
        if memo is None:
            memo = {}

        S = self.S
        uuid4 = uuid.uuid4
        args = subtask.get('args')
        kwargs = subtask.get('kwargs')

        wait_args = []
        if args:
            wait_args += [(key, s, True) for key, s in enumerate(args) if isinstance(s, S)]
        if kwargs:
            wait_args += [(key, s, False) for key, s in kwargs.items() if isinstance(s, S)]

        if wait_args:
            coro = self._await_subtasks(subtask, len(wait_args), subtask_id)
            for key, subtask, is_arg in wait_args:
                memo_key = id(subtask)
                if memo_key in memo:
                    self[memo[memo_key]].append((coro, key, is_arg))
                else:
                    sid = str(uuid4())
                    memo[memo_key] = sid
                    self[sid] = [(coro, key, is_arg)]
                    self._build(subtask, sid, memo)
            coro.send(None)

        elif subtask_id is not None:
            self._pending.append(subtask.m(id=subtask_id, graph_id=self.id))

    async def _await_subtasks(self, subtask, nargs, subtask_id=None, fut=Fut()):
        args = subtask.get('args')
        kwargs = subtask.get('kwargs')
        if args:
            args = subtask['args'] = list(args)

        while nargs > 0:
            key, value, is_arg = await fut
            if is_arg:
                args[key] = value
            else:
                kwargs[key] = value
            nargs -= 1

        if subtask_id is not None:
            self._pending.append(subtask.m(id=subtask_id, graph_id=self.id))
