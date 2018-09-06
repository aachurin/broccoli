import os
import time
import heapq
import typing
import signal
import random
import traceback
import multiprocessing as mp
from multiprocessing.connection import wait
from . app import App
from . types import Configurable, Worker, Logger, Request, Response
from . exceptions import BrokerError, TaskInterrupt, WorkerInterrupt, WarmShutdown, ColdShutdown


class WorkerScheduler():

    heappush = heapq.heappush
    heappop = heapq.heappop

    def __init__(self):
        self.callbacks = []

    def call_in(self, timeout, handler, args=()):
        self.heappush(self.callbacks, (timeout + time.time(), handler, args))

    def call_at(self, at, handler, args=()):
        self.heappush(self.callbacks, (at, handler, args))

    def run_once(self):
        callbacks = self.callbacks
        curtime = time.time()
        while callbacks:
            event_time, handler, args = callbacks[0]
            if event_time > curtime:
                return event_time - curtime
            self.heappop(callbacks)
            handler(*args)


class PreforkWorker(Configurable,
                    Worker):
    """
    concurrency     Worker concurrency.
    queues          Comma separated list of worker queues.
    error_timeout   Worker timeout after an error.
    fetch_timeout   Worker task fetch timeout.
    stop_on_worker_failure  Stop the server if the worker has failed.
    """

    concurrency: int
    error_timeout: float
    fetch_timeout: float
    queues: typing.List[str]
    stop_on_worker_failure: bool

    def __init__(self, *,
                 concurrency: int = 0,
                 error_timeout: float = 5,
                 fetch_timeout: float = 10,
                 queues: str = 'default',
                 stop_on_worker_failure: bool = True) -> None:
        self.configure(
            concurrency,
            error_timeout,
            fetch_timeout,
            queues,
            stop_on_worker_failure
            )

    def configure(self,
                  concurrency: int = None,
                  error_timeout: float = None,
                  fetch_timeout: float = None,
                  queues: str = None,
                  stop_on_worker_failure: bool = None) -> None:
        if concurrency is not None:
            self.concurrency = concurrency if concurrency > 0 else mp.cpu_count()

        if queues is not None:
            if isinstance(queues, str):
                queues = queues.split(',')  # type: ignore
            self.queues = [q.strip() for q in queues if q.strip()]
            if not self.queues:
                raise ValueError('queues is required')

        if error_timeout is not None:
            if error_timeout <= 0:
                raise ValueError('error_timeout has invalid value')
            self.error_timeout = error_timeout

        if fetch_timeout is not None:
            if fetch_timeout <= 0:
                raise ValueError('fetch_timeout has invalid value')
            self.fetch_timeout = fetch_timeout

        if stop_on_worker_failure is not None:
            self.stop_on_worker_failure = stop_on_worker_failure

    def get_configuration(self):
        return 'Worker', {
            'class': self.__class__.__name__,
            'concurrency': self.concurrency,
            'queues': ', '.join(self.queues),
            'error_timeout': self.error_timeout,
            'fetch_timeout': self.fetch_timeout,
            'stop_on_worker_failure': self.stop_on_worker_failure
        }

    @staticmethod
    def start_worker(target, **kwargs):
        parent_conn, child_conn = mp.Pipe(False)
        args = [child_conn]
        p = mp.Process(target=target, args=args, kwargs=kwargs)
        p.start()
        parent_conn.p = p
        return parent_conn

    def start(self, logger: Logger):
        app: App = self.app
        warm_shutdown_started = False
        cold_shutdown_started = False
        stop_on_worker_failure = self.stop_on_worker_failure

        event_hooks = {}
        event_names = ('task_start', 'task_done')
        for event in event_names:
            hooks = app.get_hooks('on_' + event, reverse=(event == 'task_done'))
            if hooks:
                event_hooks[event] = hooks

        def warm_shutdown_handler(signum, _):
            nonlocal warm_shutdown_started
            if warm_shutdown_started:
                if signum == signal.SIGINT:
                    cold_shutdown_handler(signum, None)
            else:
                warm_shutdown_started = True
                raise WarmShutdown(signum)

        def cold_shutdown_handler(signum, _):
            nonlocal cold_shutdown_started
            if not cold_shutdown_started:
                cold_shutdown_started = True
                raise ColdShutdown(signum)

        def start_worker():
            worker = self.start_worker(task_executor, app=app, logger=logger, queues=self.queues,
                                       error_timeout=self.error_timeout, fetch_timeout=self.fetch_timeout)
            workers.append(worker)

        def restart_worker(worker):
            workers.remove(worker)
            if worker.p.exitcode != 0:
                logger.critical('worker[%d] died unexpectedly \u2620', worker.p.pid)
                if stop_on_worker_failure:
                    raise WarmShutdown(signal.SIGTERM)
                else:
                    restart_timeout = 3 + int(random.random() * 5)
                    logger.critical('restart worker[%d] in %d seconds...', worker.p.pid, restart_timeout)
                    scheduler.call_in(restart_timeout, start_worker)
            else:
                logger.info('restart worker[%d]', worker.p.pid)
                scheduler.call_in(0, start_worker)

        def stop_workers(soft=False):
            if not soft:
                logger.warning('cold shutdown started.')
            sig = signal.SIGTERM if soft else signal.SIGKILL
            for worker in workers:
                if worker.p.is_alive():
                    os.kill(worker.p.pid, sig)
                worker.close()
            wait_terminate = workers
            while wait_terminate:
                alive = []
                for worker in wait_terminate:
                    worker.p.join(1)
                    if worker.p.is_alive():
                        alive.append(worker)
                    else:
                        logger.info('worker[%d] stopped', worker.p.pid)
                wait_terminate = alive
            workers.clear()

        scheduler = WorkerScheduler()
        workers: list = []

        for _ in range(self.concurrency):
            start_worker()

        signal.signal(signal.SIGINT, warm_shutdown_handler)
        signal.signal(signal.SIGTERM, warm_shutdown_handler)
        signal.signal(signal.SIGQUIT, cold_shutdown_handler)

        try:
            try:
                while 1:
                    try:
                        wait_timeout = scheduler.run_once()
                        ready = wait(workers, wait_timeout)
                        for w in ready:
                            try:
                                key, event = w.recv()
                            except EOFError:
                                restart_worker(w)
                                continue
                            hooks = event_hooks.get(key)
                            if hooks:
                                event['source'] = w.p
                                event['scheduler'] = scheduler
                                app.inject(hooks, event.get('request'), event)
                    except Exception:
                        logger.critical(traceback.format_exc())
                        raise
            except WarmShutdown as exc:
                logger.warning('warm shutdown started.')
                if exc.signal == signal.SIGINT:
                    logger.warning('hitting Ctrl+C again will terminate all running tasks!')
                stop_workers(soft=True)
        except ColdShutdown:
            stop_workers()


def task_executor(conn,
                  app: App,
                  logger: Logger,
                  queues: typing.List[str],
                  error_timeout: float,
                  fetch_timeout: float):
    broker = app.broker
    task_interrupt = TaskInterrupt
    worker_interrupt = WorkerInterrupt
    broker_error = BrokerError
    can_raise = None
    terminated = False
    pid = os.getpid()

    def task_interrupt_handler(_, __):
        nonlocal can_raise
        if can_raise is task_interrupt:
            can_raise = None
            raise task_interrupt()

    def worker_warm_interrupt_handler(_, __):
        nonlocal terminated, can_raise
        terminated = True
        if can_raise is worker_interrupt:
            can_raise = None
            raise worker_interrupt()

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, worker_warm_interrupt_handler)
    signal.signal(signal.SIGUSR1, task_interrupt_handler)

    def emit(event, **kwargs):
        if not terminated:
            conn.send((event, kwargs))

    logger.info('worker[%d] started', pid)

    sleep_timeout = random.random() * 5

    try:
        while not terminated:
            # can interrupt worker from this point
            can_raise = worker_interrupt
            if sleep_timeout:
                time.sleep(sleep_timeout)
                sleep_timeout = 0.
            try:
                # from this point it's unsafe to stop worker
                can_raise = None
                try:
                    request: Request = broker.get_request(queues, fetch_timeout)
                except broker_error as exc:
                    logger.error('worker[%d]: broker error: %s', pid, str(exc))
                    sleep_timeout = error_timeout
                    continue

                if request is None:
                    continue

                logger.debug('worker[%d]: received task %r', pid, request)
                start_time = time.time()

                truncated_request = Request(request.id, request.task, None, request.headers)

                emit('task_start', request=truncated_request, start_time=start_time)

                try:
                    # from this point we can interrupt the task
                    can_raise = task_interrupt
                    response = app(request)
                    can_raise = None
                except task_interrupt as exc:
                    response = Response(id=request.id, exc=exc)
                finally:
                    emit('task_done', request=truncated_request, running_time=(time.time() - start_time))

                if response.exc is not None:
                    if response.traceback:
                        logger.error('worker[%d]: task %r raised exception:\n%r\n%s',
                                     pid, request, response.exc, ''.join(response.traceback))
                    else:
                        logger.error('worker[%d]: task %r raised exception:\n%r',
                                     pid, request, response.exc)
                else:
                    logger.debug('worker[%d]: task %r, id=%r done', pid, request.task, request.id)

                attempts = 5
                while 1:
                    try:
                        broker.send_result(response, request.headers.get('result_expires'))
                        break
                    except broker_error as exc:
                        logger.error('worker[%d]: broker error: %s', pid, str(exc))
                        if attempts > 0:
                            attempts -= 1
                        elif terminated:
                            raise
                        else:
                            can_raise = worker_interrupt
                        time.sleep(error_timeout)

            except Exception:
                # Something went wrong
                tb = traceback.format_exc()
                logger.critical('worker[%d]: %s', pid, tb)

    except worker_interrupt:
        pass
