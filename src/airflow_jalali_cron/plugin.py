"""Airflow plugin that registers the Jalali cron timetable for (de)serialization.

Airflow discovers this class through the ``airflow.plugins`` setuptools entry
point declared in ``pyproject.toml`` once the package is ``pip install``ed -- so
you do *not* need to copy anything into ``$AIRFLOW_HOME/plugins``.
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from airflow_jalali_cron.timetable import JalaliCron


class JalaliCronPlugin(AirflowPlugin):
    name = "jalali_cron_plugin"
    timetables = [JalaliCron]
