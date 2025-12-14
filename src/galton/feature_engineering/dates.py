import pandas as pd


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


def add_date_fields(df):
    df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")

    df["month"] = df["date"].str[5:7]  # use string slice for consistency
    df["year"] = df["date"].str[:4]
    df["year_month"] = df["year"] + "-" + df["month"]

    return df
