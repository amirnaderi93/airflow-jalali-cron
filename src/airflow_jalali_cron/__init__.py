"""Apache Airflow timetable driven by a Jalali-calendar cron expression.

Import it directly in your DAG file::

    from airflow_jalali_cron import JalaliCron

    with DAG(..., timetable=JalaliCron("0 9 1 * *")):
        ...
"""

from __future__ import annotations

from airflow_jalali_cron.plugin import JalaliCronPlugin
from airflow_jalali_cron.timetable import JalaliCron

__all__ = ["JalaliCron", "JalaliCronPlugin"]
__version__ = "0.1.0"
