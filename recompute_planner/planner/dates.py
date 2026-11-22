"""Partition-key date arithmetic. Day keys are 'YYYY-MM-DD', month keys 'YYYY-MM'."""

import calendar
import datetime as _dt


def is_day(key):
    if not isinstance(key, str) or len(key) != 10:
        return False
    try:
        _dt.date.fromisoformat(key)
        return True
    except ValueError:
        return False


def is_month(key):
    if not isinstance(key, str) or len(key) != 7 or key[4] != "-":
        return False
    try:
        return 1 <= int(key[5:7]) <= 12
    except ValueError:
        return False


def gran_of(key):
    if is_day(key):
        return "day"
    if is_month(key):
        return "month"
    raise ValueError(f"bad partition key: {key!r}")


def month_of(day_key):
    return day_key[:7]


def days_of_month(month_key):
    year, mon = int(month_key[:4]), int(month_key[5:7])
    n = calendar.monthrange(year, mon)[1]
    return [f"{month_key}-{d:02d}" for d in range(1, n + 1)]


def add_days(day_key, n):
    return (_dt.date.fromisoformat(day_key) + _dt.timedelta(days=n)).isoformat()


def add_months(month_key, n):
    total = int(month_key[:4]) * 12 + (int(month_key[5:7]) - 1) + n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def add_period(key, n, gran):
    return add_days(key, n) if gran == "day" else add_months(key, n)
