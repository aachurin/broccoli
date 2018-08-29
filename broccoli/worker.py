import os
import sys
import time
import heapq
import signal
import random
import traceback
import multiprocessing as mp
from multiprocessing.connection import wait
from typing import List
from . app import App
from . exceptions import BrokerError, TaskInterrupt, WorkerInterrupt, WarmShutdown, ColdShutdown
from . interfaces import Worker, Broker, Logger
from . task import Response


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


class PreforkWorker(Worker):
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
    queues: List[str]
    stop_on_worker_failure: bool

    def __init__(self, *,
                 concurrency: int=0,
                 error_timeout: float=5,
                 fetch_timeout: float=10,
                 queues: str='default',
                 stop_on_worker_failure: bool=True) -> None:
        self.configure(
            concurrency,
            error_timeout,
            fetch_timeout,
            queues,
            stop_on_worker_failure
            )

    def configure(self,
                  concurrency: int=None,
                  error_timeout: float=None,
                  fetch_timeout: float=None,
                  queues: str=None,
                  stop_on_worker_failure: bool=None) -> None:
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
        return {
            'concurrency': self.concurrency,
            'queues': ', '.join(self.queues),
            'error_timeout': self.error_timeout,
            'fetch_timeout': self.fetch_timeout,
            'stop_on_worker_failure': self.stop_on_worker_failure
        }

    @staticmethod
    def start_worker(target, **kwargs):
        parent_conn, child_conn = mp.Pipe()
        args = [child_conn]
        p = mp.Process(target=target, args=args, kwargs=kwargs)
        p.start()
        parent_conn.p = p
        return parent_conn

    def start(self, app: App, broker: Broker, logger: Logger):
        shutdown_started = False
        stop_on_worker_failure = self.stop_on_worker_failure

        event_hooks = {}
        event_names = ('worker_start', 'worker_error', 'broker_error', 'task_start', 'task_done')
        for event in event_names:
            hooks = app.get_event_hooks('on_' + event, reverse=(event == 'task_done'))
            if hooks:
                event_hooks[event] = hooks

        def warm_shutdown_handler(signum, _):
            nonlocal shutdown_started
            if shutdown_started:
                if signum == signal.SIGINT:
                    raise ColdShutdown(signum)
            else:
                shutdown_started = True
                raise WarmShutdown(signum)

        def cold_shutdown_handler(signum, _):
            raise ColdShutdown(signum)

        def start_worker():
            worker = self.start_worker(task_executor, app=app, broker=broker, logger=logger, queues=self.queues,
                                       error_timeout=self.error_timeout, fetch_timeout=self.fetch_timeout,
                                       emit_events=list(event_hooks.keys()))
            workers.append(worker)

        def restart_worker(worker):
            workers.remove(worker)
            if worker.p.exitcode != 0:
                logger.critical('worker-%d died unexpectedly \u2620', worker.p.pid)
                if stop_on_worker_failure:
                    raise WarmShutdown(signal.SIGTERM)
                else:
                    restart_timeout = 3 + int(random.random() * 5)
                    logger.critical('restart worker-%d in %d seconds...', worker.p.pid, restart_timeout)
                    scheduler.call_in(restart_timeout, start_worker)
            else:
                logger.info('restart worker-%d', worker.p.pid)
                scheduler.call_in(0, start_worker)

        def stop_workers(soft=False):
            sig = signal.SIGTERM if soft else signal.SIGQUIT
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
                        logger.info('worker-%d stopped', worker.p.pid)
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
                                event, kwargs = w.recv()
                            except EOFError:
                                restart_worker(w)
                                continue
                            hooks = event_hooks.get(event)
                            if hooks:
                                app.inject(hooks)
                                # for hook in hooks:
                                #     hook(source=w, scheduler=scheduler, **kwargs)
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
                  broker: Broker,
                  logger: Logger,
                  queues: List[str],
                  error_timeout: float,
                  fetch_timeout: float,
                  emit_events: list):
    task_interrupt = TaskInterrupt
    worker_interrupt = WorkerInterrupt
    broker_error = BrokerError
    exception = Exception
    emit_worker_start = 'worker_start' in emit_events
    emit_worker_error = 'worker_error' in emit_events
    emit_broker_error = 'broker_error' in emit_events
    emit_task_start = 'task_start' in emit_events
    emit_task_done = 'task_done' in emit_events
    can_raise = None
    terminated = False
    pid = os.getpid()
    del emit_events

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

    def worker_cold_interrupt_handler(_, __):
        sys.exit(-1)

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, worker_warm_interrupt_handler)
    signal.signal(signal.SIGQUIT, worker_cold_interrupt_handler)
    signal.signal(signal.SIGUSR1, task_interrupt_handler)

    def emit(event, req=None, **kwargs):
        if not terminated:
            if req is not None:
                kwargs['id'] = req.id
                kwargs['task'] = req.task
                kwargs['options'] = req.options
            conn.send((event, kwargs))

    logger.info('worker-%d started', pid)

    if emit_worker_start:
        emit('worker_start')

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
                    request = broker.pop_request(queues, fetch_timeout)
                except broker_error as exc:
                    logger.error('worker-%d: broker error: %s', pid, str(exc))
                    if emit_broker_error:
                        emit('broker_error')
                    sleep_timeout = error_timeout
                    continue

                if request is None:
                    continue

                logger.debug('worker-%d: received task %r', pid, request)
                start_time = time.time()

                if emit_task_start:
                    emit('task_start', request, start_time=start_time)

                response = None

                try:
                    # from this point we can interrupt the task
                    can_raise = task_interrupt
                    response = app(request)
                    can_raise = None
                except task_interrupt as exc:
                    if response is None:
                        logger.info('worker-%d: task %r was interrupted', pid, request)
                        response = Response(id=request.id, exc=exc)
                finally:
                    if emit_task_done:
                        emit('task_done', request, running_time=(time.time() - start_time))

                if response.exc is not None:
                    if response.traceback:
                        logger.error('worker-%d: task %r raised exception: %r\n%s',
                                     pid, request, response.exc, ''.join(response.traceback))
                    else:
                        logger.error('worker-%d: task %r raised exception: %r',
                                     pid, request, response.exc)

                attempts = 5
                while 1:
                    try:
                        broker.push_result(request.id, response,
                                           request.options.get('result_expires'))
                        break
                    except broker_error as exc:
                        logger.error('worker-%d: broker error: %s', pid, str(exc))
                        if emit_broker_error:
                            emit('broker_error')
                        if attempts > 0:
                            attempts -= 1
                        elif terminated:
                            raise
                        else:
                            can_raise = worker_interrupt
                        time.sleep(error_timeout)

            except exception as exc:
                # Something went wrong
                tb = traceback.format_exc()
                logger.critical('worker-%d: %s', pid, tb)
                if emit_worker_error:
                    emit('worker_error', exc=exc, traceback=tb)

    except worker_interrupt:
        pass
