import re
import abc
import time
import random
import pytz
import typing
from dataclasses import dataclass
from uuid import uuid4
from datetime import tzinfo, datetime
from broccoli import __version__
from broccoli.components import Optional
from broccoli.utils import get_colorizer, color, default
from broccoli.types import Config, App, Logger


class Schedule(Optional, abc.ABC):

    @abc.abstractmethod
    def get_entries(self) -> typing.List['ScheduleEntry']:
        raise NotImplementedError


def add_console_arguments(parser):
    parser.add_argument('--loader',
                        dest='beat_schedule_loader',
                        default=default('broccoli.beat.ConfigScheduleLoader'),
                        help=('Scheduler loader to use.'))
    parser.add_argument('--timezone',
                        dest='beat_timezone',
                        type=validate_timezone,
                        default=default('UTC'),
                        help=('Timezone to use, default is UTC.'))


def bootstrap():
    return [initialize, main]


def initialize(app: App, config: Config):
    c = get_colorizer()
    print('\n\U0001F966', c('broccoli beat v%s.' % __version__, color.green))
    print(c('[timezone]', color.cyan), c(config['beat_timezone'], color.yellow))
    print()

    app.set_context_and_reset({
        'thread_name': 'beat'
    })


def main(app: App, logger: Logger, config: Config, schedule: Schedule = None):

    current_month = None
    current_day = None
    current_hour = None
    current_month_entries = []
    current_day_entries = []
    current_hour_entries = []
    entries = {}

    if schedule is None:
        schedule = ConfigScheduleLoader(config)

    timezone = validate_timezone(config['beat_timezone'])

    def update_entries(month, day, day_of_week, hour):
        nonlocal current_month, current_day, current_hour
        nonlocal current_month_entries, current_day_entries, current_hour_entries
        nonlocal entries
        if month != current_month:
            current_month = month
            current_day = None
            current_month_entries = [
                entry for entry in schedule.get_entries()
                if month in entry.get_months()
            ]
        if day != current_day:
            current_day = day
            current_hour = None
            current_day_entries = [
                entry for entry in current_month_entries
                if day in entry.get_days() and day_of_week in entry.get_days_of_week()
            ]
        if hour != current_hour:
            current_hour = hour
            current_hour_entries = [
                entry for entry in current_day_entries
                if hour in entry.get_hours()
            ]
            entries = {}
            for entry in current_hour_entries:
                for minute in entry.get_minutes():
                    entries.setdefault(minute, []).append(entry)
        return entries

    try:
        while 1:
            time.sleep(60 - time.time() % 60)
            now = pytz.utc.localize(datetime.utcnow()).astimezone(timezone)
            entries = update_entries(now.month, now.day, now.isoweekday(), now.hour)
            try:
                for entry in entries.get(now.minute, []):
                    message = app.task_message(str(uuid4()), entry.name, kwargs=entry.kwargs, **(entry.options or {}))
                    logger.debug('Send message %s', message)
                    app.send_message(message, queue=entry.queue)
            except Exception as exc:
                logger.critical('Error - %s', str(exc))
                time.sleep(random.random() * 5 + 4)
                continue
    except KeyboardInterrupt:
        pass


def validate_timezone(value):
    if isinstance(value, tzinfo):
        return value
    msg = 'Invalid timezone value.'
    try:
        return pytz.timezone(value)
    except Exception:
        raise ValueError(msg)


class ConfigScheduleLoader(Schedule):

    def __init__(self, config):
        self.entries = config.get('beat_schedule', {})

    def get_entries(self):
        return [ScheduleEntry(name=k, **v) for k, v in self.entries.items()]


@dataclass
class ScheduleEntry:

    name: str
    task: str
    schedule: tuple
    kwargs: dict = None
    options: dict = None
    queue: str = None
    expires_in: int = None

    def get_months(self):
        return crontab_parser(1, 12).parse(self.schedule[0])

    def get_days(self):
        return crontab_parser(1, 31).parse(self.schedule[1])

    def get_days_of_week(self):
        return crontab_parser(1, 7).parse(self.schedule[2])

    def get_hours(self):
        return crontab_parser(0, 23).parse(self.schedule[3])

    def get_minutes(self):
        return crontab_parser(0, 59).parse(self.schedule[4])


class crontab_parser:
    """Parser for Crontab expressions."""

    match_range_steps = re.compile(r'^(\d+)-(\d+)/(\d+)$').match
    match_range = re.compile(r'^(\d+)-(\d+)$').match
    match_star_steps = re.compile(r'^\*/(\d+)$').match
    match_star = re.compile(r'^\*$').match
    match_number = re.compile(r'^(\d+)$').match

    __slots__ = ('min', 'max')

    def __init__(self, min_, max_):
        self.min = min_
        self.max = max_

    def parse(self, spec):
        acc = set()
        for part in spec.split(','):
            if not part:
                msg = 'Empty part.'
                raise ValueError(msg)
            acc |= set(self._parse_part(part))
        return acc

    def _parse_part(self, part):
        match = self.match_range_steps(part)
        if match:
            return self._expand_range_and_steps(*match.groups())
        match = self.match_range(part)
        if match:
            return self._expand_range(*match.groups())
        match = self.match_star_steps(part)
        if match:
            return self._expand_star_and_steps(*match.groups())
        match = self.match_star(part)
        if match:
            return self._expand_star()
        match = self.match_number(part)
        if match:
            return [self._expand_number(*match.groups())]
        msg = 'Invalid part %r.'
        raise ValueError(msg % part)

    def _expand_range_and_steps(self, x, y, z):
        return self._expand_range(x, y)[::int(z)]

    def _expand_range(self, x, y):
        x = self._expand_number(x)
        y = self._expand_number(y)
        if x > y:
            msg = 'Invalid range: %r > %r.'
            raise ValueError(msg % (x, y))
        return list(range(x, y + 1))

    def _expand_star_and_steps(self, x):
        return self._expand_star()[::int(x)]

    def _expand_star(self):
        return list(range(self.min, self.max + 1))

    def _expand_number(self, x):
        try:
            x = int(x)
        except ValueError:
            msg = 'Invalid number %r.'
            raise ValueError(msg % x)
        if x > self.max:
            msg = 'Invalid end range: %r > %r.'
            raise ValueError(msg % (x, self.max))
        if x < self.min:
            msg = 'Invalid beginning range: %r < %r.'
            raise ValueError(msg % (x, self.min))
        return x
