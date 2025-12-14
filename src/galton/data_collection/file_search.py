import re
from datetime import date, datetime, timedelta
from typing import Iterable, Optional


_DATE_FORMAT_MAP: dict[str, str] = {
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYYMMDD": "%Y%m%d",
    "YYMMMDD": "%y%b%d",  # output uppercased to match 25SEP23
}


def _parse_date(date_str: str, date_format: str) -> date:
    """
    Parse a date string using one of the supported config formats.

    Supported date_format values:
        - 'YYYY-MM-DD'
        - 'YYYYMMDD'
        - 'YYMMMDD'
    """
    if date_format not in _DATE_FORMAT_MAP:
        raise ValueError(f"Unsupported date_format: {date_format!r}")

    fmt = _DATE_FORMAT_MAP[date_format]

    candidate = date_str
    if date_format == "YYMMMDD":
        # Allow '25SEP23' or '25Sep23'
        candidate = date_str.capitalize()

    try:
        return datetime.strptime(candidate, fmt).date()
    except ValueError as e:
        raise ValueError(
            f"Could not parse date_str={date_str!r} with date_format={date_format!r}"
        ) from e


def _format_date(d: date, date_format: str) -> str:
    """
    Format a date using one of the supported config formats.
    """
    if date_format not in _DATE_FORMAT_MAP:
        raise ValueError(f"Unsupported date_format: {date_format!r}")

    s = d.strftime(_DATE_FORMAT_MAP[date_format])
    if date_format == "YYMMMDD":
        s = s.upper()
    return s


def enumerate_date_range(
    start_date: str,
    start_date_format: str,
    end_date: Optional[str] = None,
    end_date_format: Optional[str] = None,
) -> list[str]:
    """
    (1) Parse dates + enumerate date range + return a list of formatted date strings.

    Returns dates in the *start_date_format* (i.e., the dataset's configured format).

    If end_date is None, uses today's local date.
    If end_date_format is None (and end_date is provided), defaults to start_date_format.
    """
    start = _parse_date(start_date, start_date_format)

    if end_date is None:
        end = date.today()
    else:
        end_fmt = end_date_format or start_date_format
        end = _parse_date(end_date, end_fmt)

    if start > end:
        return []

    num_days = (end - start).days + 1
    return [
        _format_date(start + timedelta(days=i), start_date_format)
        for i in range(num_days)
    ]


def build_file_stem_candidates(
    *,
    prefixes: Iterable[str],
    dates: Iterable[str],
    suffixes: Optional[Iterable[str]] = None,
) -> list[str]:
    """
    Build the full permutation of:
        prefix + date + suffix

    Notes:
    - This intentionally ignores extensions.
    - If suffixes is None (or empty), it is treated as [""].
    """
    prefixes_list = list(prefixes)
    dates_list = list(dates)
    suffixes_list = list(suffixes) if suffixes else [""]

    if not prefixes_list:
        raise ValueError("prefixes must contain at least one prefix")

    if not dates_list:
        return []

    return [
        f"{prefix}{d}{suffix}"
        for prefix in prefixes_list
        for d in dates_list
        for suffix in suffixes_list
    ]
