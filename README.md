# airflow-jalali-cron

A custom [Apache Airflow](https://airflow.apache.org/) **timetable** that schedules DAGs
with an ordinary **cron expression whose day-of-month and month fields are read on the
Jalali (Persian / Solar Hijri) calendar**, in the `Asia/Tehran` timezone. The default
(`JalaliCron()`) runs at the start of every Jalali month.

Distributing the timetable as an installable package is the approach
[recommended by Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/timetable.html):
it gives the timetable class one stable import path that is available to every Airflow
component (scheduler, webserver/API server, workers, triggerer, DAG processor), and that
same path is what gets serialized into the DAG — which avoids the "timetable not
registered / cannot import" errors you hit when loading it straight from the plugins
folder.

## Installation

Install it into the **same Python environment as Airflow** (every node that parses or
runs DAGs):

```bash
pip install airflow-jalali-cron
```

That single install does two things:

1. Makes `JalaliCron` importable in your DAG files.
2. Auto-registers the plugin with Airflow via the `airflow.plugins` entry point — so
   you do **not** need to copy anything into `$AIRFLOW_HOME/plugins`.

## Usage

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow_jalali_cron import JalaliCron

with DAG(
    dag_id="jalali_monthly_report",
    start_date=datetime(2024, 3, 20),     # ~ start of a Jalali year
    timetable=JalaliCron("0 9 1 * *"),  # 09:00 on the 1st of each Jalali month
    catchup=False,
    tags=["jalali"],
):
    EmptyOperator(task_id="run_monthly_job")
```

After installing, confirm Airflow sees the plugin:

```bash
airflow plugins
```

You should see `jalali_cron_plugin` listed with `JalaliCron` under its
timetables.

## Schedule syntax

`JalaliCron(cron, *, timezone="Asia/Tehran", clamp_overflow_days=True)`

The `cron` is a standard 5-field expression — `minute hour day-of-month month
day-of-week` — supporting `*`, lists (`,`), ranges (`-`) and steps (`*/n`, `a-b/n`).
What's special:

- **day-of-month** and **month** are interpreted on the **Jalali** calendar
  (month 1 = Farvardin … 12 = Esfand).
- **minute / hour** are plain `Asia/Tehran` wall-clock time.
- **day-of-week** uses the **Iranian week** (same numbering as `jdatetime`):
  `0`=Saturday (shanbe), `1`=Sunday, `2`=Monday, `3`=Tuesday, `4`=Wednesday,
  `5`=Thursday, `6`=Friday (jome).

| Goal | `cron` |
| --- | --- |
| Start of every Jalali month (default) | `0 0 1 * *` |
| 09:30 on the 5th of each month | `30 9 5 * *` |
| Every **other** month (Farvardin, Khordad, …) | `0 0 1 */2 *` |
| Every **third** month (Farvardin, Tir, Mehr, Dey) | `0 0 1 */3 *` |
| Noon every Friday | `0 12 * * 6` |
| **Last day** of every month | `0 0 31 * *` |

**`clamp_overflow_days`** (default `True`): a day-of-month larger than a month's length
is clamped to the **last day** — the effective day is `min(day, month_length)`. So
`0 0 31 * *` fires on the 31st in months 1–6, the 30th in months 7–11, and the 29th/30th
in Esfand. Set it to `False` for standard cron behavior, where an out-of-range day simply
never fires in shorter months.

## How it works

- Semantics are **interval-based**, matching Airflow's `CronDataIntervalTimetable`: each
  run covers `[this firing, next firing)` and is triggered at the end of that interval.
- All times are computed in `Asia/Tehran` (configurable via `timezone`).
- With `catchup=False`, the timetable fast-forwards to the current interval instead of
  backfilling every period since `start_date`.
- When **both** day-of-month and day-of-week are restricted, a day matches if **either**
  does (the standard Vixie-cron rule).

## Compatibility

- **Airflow:** 2.4+
- **Python:** 3.9–3.12 (matching Airflow 2.x support)

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install build twine
python -m build          # creates dist/*.whl and dist/*.tar.gz
twine check dist/*
```

## License

[MIT](LICENSE)
