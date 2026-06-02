"""A Jalali-aware cron timetable for Apache Airflow.

The schedule is given as an ordinary 5-field cron string::

    minute  hour  day-of-month  month  day-of-week

but the **day-of-month and month fields are interpreted on the Jalali
(Persian / Solar Hijri) calendar**. The minute/hour fields are plain
``Asia/Tehran`` wall-clock time.

Day-of-week uses the **Iranian week**, the same numbering ``jdatetime`` uses
(and that the Persian names encode):

==========  ==  ============
weekday      #  Persian name
==========  ==  ============
Saturday     0  shanbe
Sunday       1  yek-shanbe
Monday       2  do-shanbe
Tuesday      3  se-shanbe
Wednesday    4  chahar-shanbe
Thursday     5  panj-shanbe
Friday       6  jome
==========  ==  ============

By default, a day-of-month that exceeds the length of a given Jalali month is
**clamped to the last day** of that month -- i.e. the effective firing day is
``min(day, month_length)``. So ``"0 0 31 * *"`` runs on the last day of every
month: the 31st in months 1-6, the 30th in months 7-11, and the 29th/30th in
Esfand. Pass ``clamp_overflow_days=False`` for standard cron behavior, where an
out-of-range day simply never fires in shorter months.

Examples
--------
``"0 0 1 * *"``      -> 00:00 on the 1st of every Jalali month (the default;
                        reproduces the original "start of each Jalali month").
``"30 21 5 * *"``    -> 21:30 on the 5th of every Jalali month.
``"0 0 31 * *"``     -> 00:00 on the last day of every Jalali month.
``"0 0 1 */2 *"``    -> 00:00 on the 1st of every *other* Jalali month
                        (Farvardin, Khordad, Mordad, ...).
``"0 12 * * 6"``     -> 12:00 every Friday (jome).

The class is defined here, so its serialization path is stable:
``airflow_jalali_cron.timetable.JalaliCron``.

Semantics are interval-based, matching Airflow's ``CronDataIntervalTimetable``:
a DAG run covers the period ``[this firing, next firing)`` and executes at the
end of that period.
"""

from __future__ import annotations

import jdatetime
import pendulum

from airflow.timetables.base import (
    DagRunInfo,
    DataInterval,
    TimeRestriction,
    Timetable,
)


DEFAULT_CRON = "0 0 1 * *"
DEFAULT_TIMEZONE = "Asia/Tehran"

# Upper bound on the forward/backward day-scan when locating a firing. Eight
# Jalali years comfortably covers sparse schedules such as "Esfand 30", which
# only occurs in leap years. If nothing matches within this window the cron can
# never fire (e.g. "day 31 of Mehr" with clamping off) and we raise.
_MAX_SEARCH_DAYS = 366 * 8


def _parse_field(expr: str, lo: int, hi: int) -> frozenset[int]:
    """Expand a single cron field (``*``, ``a``, ``a-b``, ``*/s``, ``a-b/s``,
    comma-separated lists thereof) into the set of integers it matches."""
    expr = expr.strip()
    if not expr:
        raise ValueError("empty cron field")

    values: set[int] = set()
    for part in expr.split(","):
        part = part.strip()
        step = 1
        rng = part
        if "/" in part:
            rng, _, step_str = part.partition("/")
            try:
                step = int(step_str)
            except ValueError:
                raise ValueError(f"invalid step in cron field {part!r}") from None
            if step <= 0:
                raise ValueError(f"step must be positive in cron field {part!r}")

        rng = rng.strip()
        if rng == "*":
            start, end = lo, hi
        elif "-" in rng:
            a, _, b = rng.partition("-")
            try:
                start, end = int(a), int(b)
            except ValueError:
                raise ValueError(f"invalid range in cron field {part!r}") from None
        else:
            try:
                start = end = int(rng)
            except ValueError:
                raise ValueError(f"invalid value in cron field {part!r}") from None

        if start < lo or end > hi or start > end:
            raise ValueError(f"cron field {part!r} out of range [{lo}, {hi}]")
        values.update(range(start, end + 1, step))

    return frozenset(values)


def _jalali_month_length(year: int, month: int) -> int:
    """Number of days in a Jalali month (Esfand is 29, or 30 in a leap year)."""
    if month <= 6:
        return 31
    if month <= 11:
        return 30
    try:
        jdatetime.date(year, 12, 30)
    except ValueError:
        return 29
    return 30


class JalaliCron(Timetable):
    """Schedule DAG runs on a Jalali-calendar cron expression."""

    def __init__(
        self,
        cron: str = DEFAULT_CRON,
        *,
        timezone: str = DEFAULT_TIMEZONE,
        clamp_overflow_days: bool = True,
    ):
        fields = cron.split()
        if len(fields) != 5:
            raise ValueError(
                f"expected a 5-field cron string "
                f"(minute hour day-of-month month day-of-week), got {cron!r}"
            )

        self._cron = cron
        self._timezone = timezone
        self._clamp = clamp_overflow_days

        self._minutes = _parse_field(fields[0], 0, 59)
        self._hours = _parse_field(fields[1], 0, 23)
        self._days = _parse_field(fields[2], 1, 31)          # Jalali day-of-month
        self._months = _parse_field(fields[3], 1, 12)        # Jalali month
        self._weekdays = _parse_field(fields[4], 0, 6)       # Persian week, Sat=0

        # Vixie-cron rule: when *both* day-of-month and day-of-week are
        # restricted, a day matches if *either* matches.
        self._dom_restricted = fields[2].strip() != "*"
        self._dow_restricted = fields[4].strip() != "*"

        self._max_day = max(self._days)
        self._sorted_hours = sorted(self._hours)
        self._sorted_minutes = sorted(self._minutes)

        self.description = f"Jalali cron '{cron}' ({timezone})"

    # -- Airflow Timetable API ------------------------------------------------

    @property
    def summary(self) -> str:
        return f"Jalali: {self._cron}"

    def serialize(self) -> dict:
        return {
            "cron": self._cron,
            "timezone": self._timezone,
            "clamp_overflow_days": self._clamp,
        }

    @classmethod
    def deserialize(cls, data: dict) -> "JalaliCron":
        return cls(
            data["cron"],
            timezone=data.get("timezone", DEFAULT_TIMEZONE),
            clamp_overflow_days=data.get("clamp_overflow_days", True),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JalaliCron):
            return NotImplemented
        return self.serialize() == other.serialize()

    def __hash__(self) -> int:
        return hash((self._cron, self._timezone, self._clamp))

    def infer_manual_data_interval(self, *, run_after: pendulum.DateTime) -> DataInterval:
        end = self._align_prev(run_after)
        start = self._prev(end)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:
            start = last_automated_data_interval.end
        else:
            if restriction.earliest is None:
                return None
            start = self._align_next(restriction.earliest)

        end = self._next(start)

        if not restriction.catchup:
            now = pendulum.now(self._timezone)
            while end <= now:
                start = end
                end = self._next(start)

        if restriction.latest is not None and start > restriction.latest:
            return None

        return DagRunInfo.interval(start=start, end=end)

    # -- Cron engine (Jalali-aware) ------------------------------------------

    def _day_matches(self, day: pendulum.DateTime) -> bool:
        """Does this calendar day satisfy the month + day-of-month/week rules?

        ``day`` is a tz-aware ``pendulum.DateTime`` at the start of a day in the
        timetable's timezone.
        """
        jalali = jdatetime.date.fromgregorian(year=day.year, month=day.month, day=day.day)
        if jalali.month not in self._months:
            return False

        dom_ok = jalali.day in self._days
        if not dom_ok and self._clamp and self._max_day > jalali.day:
            # An out-of-range requested day clamps onto the last day of the
            # month: the last day fires iff some requested day overflows it.
            month_length = _jalali_month_length(jalali.year, jalali.month)
            if jalali.day == month_length and self._max_day > month_length:
                dom_ok = True

        dow_ok = jalali.weekday() in self._weekdays  # Persian week, Saturday=0

        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        if self._dom_restricted:
            return dom_ok
        if self._dow_restricted:
            return dow_ok
        return True

    def _next_after(self, moment: pendulum.DateTime, *, inclusive: bool) -> pendulum.DateTime:
        """Smallest firing >= moment (inclusive) or > moment (exclusive)."""
        moment = moment.in_timezone(self._timezone)
        truncated = moment.set(second=0, microsecond=0)
        if inclusive and moment == truncated:
            search_from = truncated
        else:
            search_from = truncated.add(minutes=1)

        day_cursor = search_from.start_of("day")
        first_day = True
        for _ in range(_MAX_SEARCH_DAYS):
            if self._day_matches(day_cursor):
                for hour in self._sorted_hours:
                    for minute in self._sorted_minutes:
                        candidate = day_cursor.set(hour=hour, minute=minute)
                        if first_day and candidate < search_from:
                            continue
                        return candidate
            day_cursor = day_cursor.add(days=1)
            first_day = False

        raise ValueError(
            f"cron {self._cron!r} has no firing within {_MAX_SEARCH_DAYS} days "
            f"of {moment.to_iso8601_string()} (is the schedule satisfiable?)"
        )

    def _prev_before(self, moment: pendulum.DateTime, *, inclusive: bool) -> pendulum.DateTime:
        """Largest firing <= moment (inclusive) or < moment (exclusive)."""
        moment = moment.in_timezone(self._timezone)
        truncated = moment.set(second=0, microsecond=0)
        if not inclusive and moment == truncated:
            search_to = truncated.subtract(minutes=1)
        else:
            search_to = truncated

        day_cursor = search_to.start_of("day")
        first_day = True
        for _ in range(_MAX_SEARCH_DAYS):
            if self._day_matches(day_cursor):
                for hour in reversed(self._sorted_hours):
                    for minute in reversed(self._sorted_minutes):
                        candidate = day_cursor.set(hour=hour, minute=minute)
                        if first_day and candidate > search_to:
                            continue
                        return candidate
            day_cursor = day_cursor.subtract(days=1)
            first_day = False

        raise ValueError(
            f"cron {self._cron!r} has no firing within {_MAX_SEARCH_DAYS} days "
            f"before {moment.to_iso8601_string()} (is the schedule satisfiable?)"
        )

    def _align_next(self, moment: pendulum.DateTime) -> pendulum.DateTime:
        return self._next_after(moment, inclusive=True)

    def _next(self, moment: pendulum.DateTime) -> pendulum.DateTime:
        return self._next_after(moment, inclusive=False)

    def _align_prev(self, moment: pendulum.DateTime) -> pendulum.DateTime:
        return self._prev_before(moment, inclusive=True)

    def _prev(self, moment: pendulum.DateTime) -> pendulum.DateTime:
        return self._prev_before(moment, inclusive=False)
