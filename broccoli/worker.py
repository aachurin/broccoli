import os
import sys
import time
import uuid
import heapq
import signal
import random
import traceback
import multiprocessing as mp
from multiprocessing.connection import wait
from broccoli import __version__
from broccoli.utils import get_colorizer, color, default, validate
from broccoli.types import App, Broker, Logger, Config, Context


class WorkerInterrupt(BaseException):
    """Worker interrupt"""


class Shutdown(BaseException):
    pass


class Scheduler(list):

    heappush = heapq.heappush
    heappop = heapq.heappop

    def call_in(self, timeout, handler, args=()):
        self.heappush(self, (timeout + time.time(), handler, args))

    def call_at(self, at, handler, args=()):
        self.heappush(self, (at, handler, args))

    def run_once(self):
        curtime = time.time()
        while self:
            event_time, handler, args = self[0]
            if event_time > curtime:
                return event_time - curtime
            self.heappop(self)
            handler(*args)


def add_console_arguments(parser):
    parser.add_argument('-n', '--node',
                        dest='worker_node_id',
                        help='Set custom node id.',
                        default=default(str(uuid.uuid4())))
    parser.add_argument('-c', '--concurrency',
                        dest='worker_concurrency',
                        help=('Number of child processes processing the queue. '
                              'The default is the number of CPUs available on your system.'),
                        type=int,
                        default=default(mp.cpu_count()))
    parser.add_argument('-q', '--queues',
                        dest='worker_queues',
                        help=('List of queues to enable for this worker, separated by comma. '
                              'By default "default" queue are enabled.'),
                        type=lambda x: x.split(','),
                        default=default(['default']))
    parser.add_argument('--result-expires-in',
                        dest='result_expires_in',
                        help=('Time (in seconds) to hold task results. Default is 3600.'),
                        type=int,
                        default=default(3600))
    parser.add_argument('--watchdog-interval',
                        dest='worker_fall_down_watchdog_interval',
                        help=('Fall down watchdog interval (in seconds). Default is 10.'),
                        type=int,
                        default=default(10))
    parser.add_argument('--fetch-timeout',
                        dest='broker_fetch_timeout',
                        type=int,
                        default=default(10))
    parser.add_argument('--restart-died-workers',
                        dest='restart_died_workers',
                        action='store_true',
                        default=default(False))


def bootstrap():
    return [
        validate_config,
        initialize,
        add_watch_dog,
        start
    ]


def validate_config(config: Config):
    try:
        validate(config.get('worker_node_id'),
                 type=str,
                 regex=r'[a-zA-Z0-9_\-.]+',
                 msg='Invalid node_id value')
        validate(config.get('worker_concurrency'),
                 type=int,
                 min_value=1,
                 msg='Invalid concurrency value')
        validate(config.get('broker_fetch_timeout'),
                 type=int,
                 min_value=1,
                 msg='Invalid fetch_timeout value')
        validate(config.get('result_expires_in'),
                 type=int,
                 min_value=5,
                 msg='Invalid fetch_timeout value')
        validate(config.get('worker_queues'),
                 type=list,
                 min_length=1,
                 msg='Invalid queues value')
        validate(config.get('worker_fall_down_watchdog_interval'),
                 coerce=int,
                 min_value=1,
                 msg='Invalid watchdog interval')
        for item in config['worker_queues']:
            validate(item,
                     type=str,
                     regex=r'[a-zA-Z0-9_\-.]+',
                     msg='Invalid queue name')

    except ValueError as exc:
        print('Error: %s' % exc)
        sys.exit(-1)

    c = get_colorizer()
    print('\n\U0001F966', c('broccoli v%s.' % __version__, color.green))
    print(c('[node_id]    ', color.cyan), c(config['worker_node_id'], color.yellow))
    print(c('[concurrency]', color.cyan), c(config['worker_concurrency'], color.yellow))
    print(c('[queues]     ', color.cyan), c(', '.join(config['worker_queues']), color.yellow))
    print(c('[result_expires_in]', color.cyan), c(config['result_expires_in'], color.yellow))
    print()


def initialize(app: App, config: Config, context: Context):
    app.update_context_and_reset(
        node_id=config['worker_node_id'],
        thread_name='master'
    )
    context.scheduler = Scheduler()


def add_watch_dog(broker: Broker, config: Config, context: Context, logger: Logger):
    node_id = config['worker_node_id']
    scheduler = context.scheduler
    watch_dog_enabled = False

    def hold_elections():
        nodes = broker.get_nodes()
        if any(n[1].endswith('@') for n in nodes):
            return False
        nodes.sort(reverse=True)
        if nodes[0][1] == node_id:
            broker.set_node_id(node_id + '@')
            logger.info('Worker fall down watchdog activated.')
            return True
        return False

    fall_down_watchdog_interval = config['worker_fall_down_watchdog_interval']

    def watch_dog():
        nonlocal watch_dog_enabled
        if not watch_dog_enabled:
            watch_dog_enabled = hold_elections()
        if watch_dog_enabled:
            result = broker.run_gc(verbose=True)
            if result:
                for msg, *args in result:
                    logger.debug(msg, *args)
        scheduler.call_in(fall_down_watchdog_interval, watch_dog)

    scheduler.call_in(0, watch_dog)


def start(app: App, broker: Broker, config: Config, context: Context, logger: Logger):

    def start_worker(thread_name):
        c1, c2 = mp.Pipe(True)
        p = mp.Process(target=run_worker, args=[app, thread_name, c2])
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        p.start()
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        c1.p = p
        c1.thread_name = thread_name
        connections.append(c1)

    def restart_worker(conn):
        if conn.p.is_alive():
            conn.p.terminate()
        conn.close()
        connections.remove(conn)
        restart_timeout = 3 + int(random.random() * 5)
        logger.critical('%s died unexpectedly \U0001f480, restart in %d seconds...',
                        conn.thread_name, restart_timeout)
        scheduler.call_in(restart_timeout, start_worker, (conn.thread_name,))

    shutdown_started = False

    def shutdown_handler(_, __):
        nonlocal shutdown_started
        if not shutdown_started:
            shutdown_started = True
            raise Shutdown()

    def on_task_start(_conn, _data):
        pass

    def on_task_done(_conn, _data):
        pass

    event_handlers = {
        'task_start': on_task_start,
        'task_done': on_task_done
    }

    node_id = config['worker_node_id']
    scheduler = context.scheduler
    connections = []

    try:
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        time.sleep(random.random() * 3)
        broker.set_node_id(node_id)
        time.sleep(0.5)

        for ident in range(config['worker_concurrency']):
            start_worker('worker%d' % (ident + 1))

        while 1:
            # noinspection PyBroadException
            try:
                wait_timeout = scheduler.run_once()
                ready: list = wait(connections, wait_timeout)
                for conn in ready:
                    try:
                        event, data = conn.recv()
                    except EOFError:
                        logger.debug('Broken pipe to %s', conn.thread_name)
                        restart_worker(conn)
                        continue
                    event_handlers[event](conn, data)
            except Exception:
                logger.critical(traceback.format_exc())
                break
    except Shutdown:
        pass

    logger.warning('Shutdown started.')

    while connections:
        alive = []
        for conn in connections:
            if conn.p.is_alive():
                conn.p.terminate()
                conn.close()
                alive.append(conn)
        connections = alive
        if connections:
            time.sleep(0.5)


def run_worker(app, thread_name, conn):
    worker_id = str(uuid.uuid4())
    app.update_context_and_reset(
        worker_id=worker_id,
        thread_name=thread_name
    )
    app.inject([worker], kwargs={'conn': conn, 'worker_id': worker_id}, cache=False)


def worker(conn, worker_id, app: App, broker: Broker, logger: Logger, config: Config):
    fence_counter = 0
    shutdown_started = False

    class fence:
        __slots__ = ()

        def __enter__(self):
            nonlocal fence_counter
            fence_counter += 1

        def __exit__(self, *exc_info):
            nonlocal fence_counter
            fence_counter -= 1
            if fence_counter == 0 and shutdown_started:
                raise WorkerInterrupt()

    fence = fence()

    def worker_interrupt_handler(_, __):
        nonlocal shutdown_started
        if not shutdown_started:
            shutdown_started = True
            if fence_counter == 0:
                raise WorkerInterrupt()

    def worker_state_handler(_, __):
        frame = sys._getframe(1)
        logger.info('%s: line %s', frame.f_code.co_filename, frame.f_lineno)

    # def task_interrupt_handler(_, __):
    #     pass

    fetch_timeout = config['broker_fetch_timeout']
    result_expires_in = config['result_expires_in']

    def emit(event, data=None):
        conn.send((event, data))

    def send_reply(reply):
        reply_to = reply.get('reply_to')
        result_key = reply.get('result_key')
        if reply_to or result_key:
            while 1:
                try:
                    if result_key:
                        broker.set_result(result_key, reply, expires_in=result_expires_in)
                    else:
                        broker.send_reply(reply_to, reply)
                    break
                except broker.BrokerError as exc:
                    logger.critical('Broker error: %s', str(exc))
                    time.sleep(3 + random.random() * 2)

    def on_complete(reply, start_time, ackkey):
        running_time = time.time() - start_time
        try:
            if 'exc' in reply:
                logger.error('Task %s raised exception - %s: %s\n%s',
                             message,
                             reply['exc'].__class__.__name__,
                             str(reply['exc']),
                             reply.get('traceback', ''))

            logger.info('Task %s done in %s.', reply, running_time)
            logger.debug('Reply: %r.', reply)

            emit('task_done', {
                'id': reply['id'],
                'task': reply['task'],
                'start_time': start_time,
                'running_time': running_time
            })

            send_reply(reply)
            broker.ack_message(ackkey)

        except Exception:
            tb = traceback.format_exc()
            logger.critical('Critical error:\n%s', tb)

    logger.info('Started, pid=%d, id=%s', os.getpid(), worker_id)

    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, worker_interrupt_handler)
        # signal.signal(signal.SIGUSR1, task_interrupt_handler)
        signal.signal(signal.SIGUSR2, worker_state_handler)

        broker.setup(worker_id, config['worker_queues'])

        num_errors = 0
        sleep_timeout = random.random() * 3

        while 1:
            if sleep_timeout:
                time.sleep(sleep_timeout)
                sleep_timeout = 0.
            # noinspection PyBroadException
            try:
                try:
                    messages = broker.get_messages(prefetch=1, timeout=fetch_timeout)
                    num_errors = 0
                except broker.DecodeError as exc:
                    logger.warning("Can't decode incoming message: %s", str(exc))
                    continue
                except broker.BrokerError as exc:
                    logger.critical('Broker error: %s', str(exc))
                    sleep_timeout = (min(num_errors, 5) * 5) + 2 * random.random() + 1
                    num_errors += 1
                    continue

                if not messages:
                    continue

                for ackkey, message in messages:
                    if message.is_reply:
                        start_time = None
                        logger.info('Received reply %s.', message)
                        broker.ack_message(ackkey)
                    else:
                        start_time = time.time()
                        logger.info('Received task %s.', message)

                    emit('task_start', {
                        'id': message['id'],
                        'task': message['task'],
                        'time_limit': message.get('time_limit'),
                        'start_time': start_time
                    })

                    logger.debug('Message: %r.', message)
                    try:
                        app.run_message(message, on_complete, (start_time, ackkey), fence=fence)
                    except app.RejectMessage as exc:
                        logger.error('Message rejected: %s: %s', str(exc), message)

            except Exception:
                # Something went wrong
                tb = traceback.format_exc()
                logger.critical('Critical error:\n%s', tb)

    except WorkerInterrupt:
        pass

    broker.close()
