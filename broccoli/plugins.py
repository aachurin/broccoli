# import os
# import re
# import time
# from calendar import monthrange
# from datetime import datetime
# from collections import Iterable
# from . exceptions import BrokerError
# from . interfaces import Logger
# from . utils import cached_property
# import signal

#
# def plugin(fun):
#     return type(fun.__name__, (object,), {
#         'start': staticmethod(fun)
#     })
#
#
# @plugin
# def task_killer(logger: Logger):
#     running_tasks: set = set()
#
#     def kill_task(source, id):
#         if id in running_tasks:
#             logger.warning('\U0001F4A3 kill task %s due to time limit', id)
#             os.kill(source.p.pid, signal.SIGUSR1)
#             running_tasks.remove(id)
#
#     def on_task_start(source, sched, id, options, start_time, **kwargs):
#         limit = options.get('time_limit')
#         if limit is not None:
#             running_tasks.add(id)
#             sched.call_at(start_time + limit, kill_task, args=(source, id))
#
#     def on_task_done(id, **kwargs):
#         if id in running_tasks:
#             running_tasks.remove(id)
#
#     logger.info('task killer starts watching \U0001F4A3')
#
#     return {
#         'task_start': on_task_start,
#         'task_done': on_task_done
#     }


# class CronBeat(Plugin):

#     def __init__(self,
#                  app: App,
#                  schedule: str,
#                  error_timeout: int,
#                  logger: typing.Union[logging.Logger, Logger],
#                  **kwargs) -> None:
#         self.app = app
#         self.logger = logger
#         self.error_timeout = error_timeout
#         self.schedule = schedule
#         self.next_run = 0

#     @classmethod
#     def add_console_args(cls, parser) -> None:
#         parser.add_argument('--schedule',
#                             dest='schedule',
#                             default='schedule.py',
#                             help='Schedule rules')

#     def get_applied_conf(self):
#         return {
#             'schedule': self.schedule
#         }

#     @cached_property
#     def heap(self):
#         dct = {'crontab': crontab}
#         with open(self.schedule, 'rt') as f:
#             rules = eval(f.read(), dct)
#         if not isinstance(rules, dict):
#             raise TypeError('Must be a dict')
#         start = datetime.now()
#         heap = []
#         for key, entry in rules.items():
#             if not entry.get('task'):
#                 raise TypeError('`task` must be set')
#             schedule = entry.get('schedule')
#             if not isinstance(schedule, crontab):
#                 raise TypeError('`schedule` must be a crontab')
#             schedule = schedule.start(start)
#             heappush(heap, (next(schedule).timestamp(), schedule, entry))
#         return heap

#     def master_idle(self, curtime):
#         if not self.heap:
#             return

#         if self.next_run > curtime:
#             return self.next_run - curtime

#         task_sent = False

#         while self.heap and self.heap[0][0] <= curtime:
#             _, schedule, entry = self.heap[0]
#             try:
#                 self.app.send_task(entry['task'],
#                                    args=entry.get('args', ()),
#                                    kwargs=entry.get('kwargs', {}))
#             except BrokerError:
#                 self.logger.error('[beat] - cant send task, retry in %ss.',
#                                   self.error_timeout)
#                 self.next_run = self.error_timeout + curtime
#                 return self.error_timeout
#             else:
#                 self.logger.debug('[beat] - %s sent.', entry['task'])
#                 heappop(self.heap)
#                 heappush(self.heap, (
#                     next(schedule).timestamp(), schedule, entry))
#             task_sent = True

#         if self.heap:
#             self.next_run = self.heap[0][0]
#             timeout = self.next_run - curtime
#             if task_sent:
#                 self.logger.debug('[beat] - next task in %fs.', timeout)
#             return timeout


# def reiter(v, seq):
#     yield v
#     yield from seq


# def rewind(vals, chains):
#     if not chains or not vals:
#         return False, ()
#     val, *rest_vals = vals
#     chain, *rest_chains = chains
#     chain = iter(chain)
#     for v in chain:
#         if v > val:
#             return False, (reiter(v, chain), *rest_chains)
#         elif v == val:
#             restarted, rest_chains = rewind(rest_vals, rest_chains)
#             return False, (
#                 chain if restarted else reiter(v, chain),
#                 *rest_chains
#                 )
#     else:
#         return True, chains


# class crontab_parser:
#     """Parser for Crontab expressions."""

#     _range = r'(\d+?)-(\d+)'
#     _steps = r'/(\d+)'
#     _number = r'(\d+)'
#     _star = r'\*'

#     def __init__(self, min_, max_):
#         self.max_ = max_
#         self.min_ = min_
#         self.pats = (
#             (re.compile('^' + self._range + self._steps + '$'),
#                 self._range_steps),
#             (re.compile('^' + self._range + '$'),
#                 self._expand_range),
#             (re.compile('^' + self._star + self._steps + '$'),
#                 self._star_steps),
#             (re.compile('^' + self._star + '$'),
#                 self._expand_star),
#             (re.compile('^' + self._number + '$'),
#                 self._expand_range)
#         )

#     def parse(self, spec):
#         acc = set()
#         for part in spec.split(','):
#             if not part:
#                 raise ValueError('empty part')
#             acc |= set(self._parse_part(part))
#         return sorted(acc)

#     def _parse_part(self, part):
#         for regex, handler in self.pats:
#             m = regex.match(part)
#             if m:
#                 return handler(m.groups())
#         raise ValueError('invalid filter: %r' % part)

#     def _expand_range(self, toks):
#         fr = self._expand_number(toks[0])
#         if len(toks) > 1:
#             to = self._expand_number(toks[1])
#             if to < fr:
#                 raise ValueError('invalid range')
#             return list(range(fr, to + 1))
#         return [fr]

#     def _range_steps(self, toks):
#         if len(toks) != 3 or not toks[2]:
#             raise ValueError('empty filter')
#         return self._expand_range(toks[:2])[::int(toks[2])]

#     def _star_steps(self, toks):
#         if not toks or not toks[0]:
#             raise ValueError('empty filter')
#         return self._expand_star()[::int(toks[0])]

#     def _expand_star(self, *args):
#         return list(range(self.min_, self.max_ + self.min_ + 1))

#     def _expand_number(self, s):
#         try:
#             i = int(s)
#         except ValueError:
#             raise ValueError('invalid number: %r' % s)
#         if i > self.max_:
#             raise ValueError(
#                 'invalid end range: {0} > {1}.'.format(i, self.max_))
#         if i < self.min_:
#             raise ValueError(
#                 'invalid beginning range: {0} < {1}.'.format(i, self.min_))
#         return i


# class crontab:

#     def __init__(self,
#                  minute='*',
#                  hour='*',
#                  day_of_month='*',
#                  month_of_year='*',
#                  day_of_week='*'):
#         self._orig_minute = minute
#         self._orig_hour = hour
#         self._orig_day_of_week = day_of_week
#         self._orig_day_of_month = day_of_month
#         self._orig_month_of_year = month_of_year
#         self.hour = self._expand_spec(hour, 0, 23)
#         self.minute = self._expand_spec(minute, 0, 59)
#         self.day_of_week = self._expand_spec(day_of_week, 0, 6)
#         self.day_of_month = self._expand_spec(day_of_month, 1, 31)
#         self.month_of_year = self._expand_spec(month_of_year, 1, 12)

#     def __repr__(self):
#         return ('<crontab: {0._orig_minute} {0._orig_hour} '
#                 '{0._orig_day_of_week} {0._orig_day_of_month} '
#                 '{0._orig_month_of_year}>').format(self)

#     @staticmethod
#     def _expand_spec(cronspec, min_, max_):
#         """Expand cron specification."""

#         if isinstance(cronspec, int):
#             result = [cronspec]
#         elif isinstance(cronspec, str):
#             result = crontab_parser(min_, max_).parse(cronspec)
#         elif isinstance(cronspec, (list, tuple, set)):
#             result = sorted(cronspec)
#         elif isinstance(cronspec, Iterable):
#             result = sorted(cronspec)
#         else:
#             raise TypeError("Argument cronspec needs to be of any of the "
#                             "following types: int, str, or an iterable type. "
#                             "%r was given." % type(cronspec))
#         for number in result:
#             if not isinstance(number, int):
#                 raise ValueError("Argument cronspec needs to be an int: "
#                                  "%r was given." % type(number))
#         for number in [result[0], result[-1]]:
#             if result[0] < min_ or result[0] > max_:
#                 raise ValueError(
#                     "Invalid crontab pattern. Valid range is {%d}-{%d}. "
#                     "'%r' was found." % (min_, max_, result[0]))
#         return result

#     def start(self, start_date=None):
#         y = start_date.year
#         complete, (month_of_year, day_of_month, hour, minute) = rewind(
#             start_date.timetuple()[1:5], (
#                 self.month_of_year,
#                 self.day_of_month,
#                 self.hour,
#                 self.minute
#             ))

#         if complete:
#             y += 1

#         while 1:
#             for m in month_of_year:
#                 max_d = monthrange(y, m)[1]
#                 for d in day_of_month:
#                     if d > max_d:
#                         break
#                     for h in hour:
#                         for mi in minute:
#                             yield datetime(y, m, d, h, mi)
#                         minute = self.minute
#                     hour = self.hour
#                 day_of_month = self.day_of_month
#             month_of_year = self.month_of_year
#             y += 1
