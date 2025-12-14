import pandas as pd
import pytz

from datetime import datetime
from typing import Optional


def save(
    df: pd.DataFrame,
    path: str,
    file_name: str,
    current_timestamp: Optional[datetime] = None,
) -> None:
    """
    Save a DataFrame to a Parquet file with an optional timestamp appended.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to save.
    path : str
        The directory or base path where the file will be saved.
    file_name : str
        The base name of the file (without extension).
    current_timestamp : datetime, optional
        A datetime object whose timestamp and timezone (if provided)
        will be sanitized and appended to the file name.
    """

    # Build the base file path before extension
    base = f"{path}/{file_name}"

    if current_timestamp:
        # Format datetime safely for file names
        # Example: 2025-11-03T22-26-40.645330
        dt_str = current_timestamp.isoformat()

        # Replace illegal filename characters
        dt_str = (
            dt_str.replace(":", "-")
            .replace("+", "_plus_")
            .replace("-", "_")  # first replace creates conflicts; we handle below
        )

        # Fix date hyphens (turn back into hyphens for readability)
        # year_month_day portion is safe
        dt_str = dt_str.replace("_", "-", 2)  # only fix first two segments

        # Append to base path
        base = f"{base}--{dt_str}"

    # Always use .parquet extension
    full_path = f"{base}.parquet"

    df.to_parquet(full_path)


def normalize_field_names(df):
    df = df.rename(
        columns={
            "forecast_date": "datetime",
            "obs_datetime": "datetime",
            "obs_date": "date",
            "temperature_2m": "forecast_temperature",
            "strike_midpoint": "event_midpoint",
        }
    )
    return df


def get_current_timestamp(time_zone="US/Central"):
    central_tz = pytz.timezone(time_zone)
    current_timestamp = datetime.now(central_tz)
    return current_timestamp


def convert_datetime_to_utc(
    df: pd.DataFrame, col: str = "datetime", drop_col: bool = False
) -> pd.DataFrame:
    """
    Convert a dataframe column with timezone-aware datetimes
    to UTC and rename it to 'datetime_utc'.

    Example input: '2025-09-14 00:00:00-05:00'
    """
    df = df.copy()  # avoid mutating caller’s df
    df["datetime_utc"] = pd.to_datetime(df[col], utc=True).dt.tz_convert("UTC")
    if drop_col:
        df = df.drop(columns=[col])
    return df


# def add_date_fields(df):
#     df["date"] = pd.to_datetime(df["date"])
#     df["date"] = df["date"].dt.strftime("%Y-%m-%d")  # convert back to formatted string

#     df["month"] = df["date"].str[5:7]  # use string slice for consistency
#     df["year"] = df["date"].str[:4]
#     df["year_month"] = df["year"] + "-" + df["month"]

#     df["year"] = df["year"].astype(int)
#     df["month"] = df["month"].astype(int)
#     return df
