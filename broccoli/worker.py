import os
import sys
import uuid
import time as _time
import random as _random
import heapq
import signal
import traceback
import multiprocessing as mp
from multiprocessing import connection
from broccoli import __version__
from broccoli.utils import get_colorizer, color, default, validate
from broccoli.components import ReturnValue
from broccoli.types import App, Broker, LoggerService, Config, Message, MsgRepr as _MsgRepr
from broccoli.exceptions import WorkerInterrupt, Shutdown, Reject as _Reject


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
    parser.add_argument('-l', '--loglevel',
                        dest='loglevel',
                        help=('Logging level for default logger.'),
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default=default(['INFO']))
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


def initialize(app: App, config: Config):
    app.set_context(
        node_id=config['worker_node_id']
    )


def start(app: App, broker: Broker, config: Config, logger_service: LoggerService):
    logger = logger_service.get_logger('bro.master')

    def start_worker():
        c1, c2 = mp.Pipe(True)
        p = mp.Process(target=run_worker, args=[app, c2])
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        p.start()
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        c1.p = p
        connections.append(c1)

    def restart_worker(worker_conn):
        if worker_conn.p.is_alive():
            worker_conn.p.terminate()
        worker_conn.close()
        connections.remove(worker_conn)
        restart_timeout = 3 + int(random() * 5)
        logger.critical('%s died unexpectedly \U0001f480, start new worker in %d seconds...',
                        worker_conn.p.name, restart_timeout)
        call_in(restart_timeout, start_worker)

    def shutdown_handler(_, __):
        nonlocal shutdown_started
        if not shutdown_started:
            shutdown_started = True
            raise Shutdown()

    def on_task_start(_conn, _data):
        pass

    def on_task_done(_conn, _data):
        pass

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

    def watch_dog():
        nonlocal watch_dog_enabled
        if not watch_dog_enabled:
            watch_dog_enabled = hold_elections()
        if watch_dog_enabled:
            broker.run_gc()
        call_in(fall_down_watchdog_interval, watch_dog)

    def call_in(in_seconds, handler, args=()):
        heappush(tasks, (time() + in_seconds, (handler, args)))

    random = _random.random
    sleep = _time.sleep
    time = _time.time
    wait = connection.wait
    heappush = heapq.heappush
    heappop = heapq.heappop

    tasks = []
    connections = []
    shutdown_started = False
    watch_dog_enabled = False
    node_id = config['worker_node_id']
    fall_down_watchdog_interval = config['worker_fall_down_watchdog_interval']

    event_handlers = {
        'task_start': on_task_start,
        'task_done': on_task_done
    }

    call_in(0, watch_dog)

    try:
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        sleep(random() * 3)
        broker.set_node_id(node_id)
        sleep(0.5)

        for ident in range(config['worker_concurrency']):
            start_worker()

        while 1:
            try:
                while tasks:
                    next_event_at = tasks[0][0]
                    timeout = next_event_at - time()
                    if timeout <= 0:
                        event_handler, event_args = heappop(tasks)[1]
                        event_handler(*event_args)
                    else:
                        break

                if not tasks:
                    timeout = None

                ready: list = wait(connections, timeout)
                for conn in ready:
                    try:
                        event, data = conn.recv()
                    except EOFError:
                        logger.debug('Broken pipe to %s', conn.p.name)
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
            sleep(0.5)


def run_worker(app, conn):
    worker_id = str(uuid.uuid4())
    app.set_context(
        worker_id=worker_id
    )
    app.inject([worker], args={'conn': conn, 'worker_id': worker_id}, cache=False)


def worker(conn, worker_id, app: App, broker: Broker, logger_service: LoggerService, config: Config):
    logger = logger_service.get_logger('bro.worker')

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

    def worker_interrupt_handler(_, __):
        nonlocal shutdown_started
        if not shutdown_started:
            shutdown_started = True
            if fence_counter == 0:
                raise WorkerInterrupt()

    def worker_state_handler(_, __):
        frame = sys._getframe(1)
        logger.info('%s: line %s', frame.f_code.co_filename, frame.f_lineno)

    def emit(event, data=None):
        conn.send((event, data))

    def on_request(msg: Message):
        logger.info('Received task %s.', MsgRepr(msg))
        start_time = time()
        msg['_start'] = start_time
        emit('task_start', {
            'id': msg['id'],
            'task': msg['task'],
            'start_time': start_time
        })

    def on_response(msg: Message, ret: ReturnValue):
        running_time = time() - msg['_start']
        logger.info('Task %s done in %s.', MsgRepr(msg), running_time)
        emit('task_done', {
            'id': msg['id'],
            'task': msg['task'],
            'start_time': msg['_start'],
            'running_time': running_time
        })
        return ret

    def send_reply(reply):
        key = reply.pop('_context', None)
        while 1:
            try:
                if 'result_key' in reply:
                    if reply['result_key']:
                        broker.set_result(reply['result_key'], reply, expires_in=result_expires_in)
                        logger.debug('Set result: %r', MsgRepr(reply))
                    broker.ack(key)
                    return
                elif 'reply_to' in reply:
                    broker.send_reply(reply['reply_to'], reply)
                    logger.debug('Send reply: %r', MsgRepr(reply))
                    broker.ack(key)
                    return
                else:
                    broker.send_message(reply, reply_back=True)
                    logger.debug('Send message: %r', MsgRepr(reply))
                    return
            except broker.BrokerError as err:
                logger.critical('Broker error: %s', str(err))
                sleep(3 + random() * 3)

    def main_loop():
        num_errors = 0
        while 1:
            try:
                messages = None
                if deferred_messages:
                    next_message_at = deferred_messages[0][0]
                    timeout = next_message_at - time()
                    if timeout <= 0:
                        messages = [heappop(deferred_messages)[1]]
                    timeout = getmin(timeout, fetch_timeout)
                else:
                    timeout = fetch_timeout

                if not messages:
                    try:
                        messages = broker.get_messages(timeout=timeout)
                        num_errors = 0
                    except broker.BrokerError as exc:
                        logger.critical('Broker error: %s', str(exc))
                        num_errors += 1
                        sleep_timeout = getmin(num_errors, 10) + random() * 3
                        if deferred_messages:
                            sleep_timeout = getmin(sleep_timeout, next_message_at - time())
                        if sleep_timeout > 0:
                            sleep(sleep_timeout)
                        continue

                for message_key, message in messages:
                    message_repr = MsgRepr(message)
                    run_after = message.get('run_after')
                    if run_after is not None and isinstance(run_after, (int, float)):
                        timeout = run_after - time()
                        if timeout >= 0:
                            heappush(deferred_messages, (run_after, (message_key, message)))
                        logger.info('Deferred message %s received. Should be started in %.2f seconds.',
                                    message_repr, timeout)
                        continue

                    logger.debug('Got message: %r.', message_repr)
                    is_reply = 'reply_id' in message
                    if not is_reply:
                        message['_context'] = message_key
                    try:
                        for message_reply in app.serve_message(message, fence=fence):
                            send_reply(message_reply)
                    except Reject as exc:
                        logger.info('Message %s was rejected: %s', message_repr, str(exc))
                        continue
                    finally:
                        if is_reply:
                            broker.ack(message_key)

            except Exception:
                # Something went wrong
                logger.critical('Critical error:\n%s', traceback.format_exc())

    heappush = heapq.heappush
    heappop = heapq.heappop
    getmin = min
    time = _time.time
    sleep = _time.sleep
    random = _random.random
    Reject = _Reject
    MsgRepr = _MsgRepr

    fence = fence()
    fence_counter = 0
    deferred_messages = []
    shutdown_started = False
    fetch_timeout = config['broker_fetch_timeout']
    result_expires_in = config['result_expires_in']

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, worker_interrupt_handler)
    signal.signal(signal.SIGUSR2, worker_state_handler)

    app.set_hooks(on_request=on_request,
                  on_response=on_response)

    logger.info('Started, pid=%d, id=%s', os.getpid(), worker_id)

    try:
        broker.setup(worker_id, config['worker_queues'])
        sleep(random() * 1.5)
        main_loop()
    except WorkerInterrupt:
        pass

    broker.close()
