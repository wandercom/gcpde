"""Base utilities."""

import datetime


def get_utc_now() -> datetime.datetime:
    """Return a timezone aware datetime object in UTC timezone."""
    return datetime.datetime.now(datetime.timezone.utc)
