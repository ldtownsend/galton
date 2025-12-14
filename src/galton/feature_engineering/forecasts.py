import pandas as pd
import numpy as np


def add_forecast_fields(df):
    df["forecast_horizon"] = df["datetime"] - df["model_timestamp"]
    df["forecast_horizon_hours"] = df["forecast_horizon"].dt.total_seconds() / 3600

    df["is_model_timestamp_prior_day"] = (
        df["model_timestamp"].dt.date == (df["datetime"] - pd.Timedelta(days=1)).dt.date
    )
    df["is_model_timestamp_same_day"] = (
        df["model_timestamp"].dt.date == (df["datetime"] - pd.Timedelta(days=0)).dt.date
    )
    return df


# def add_forecast_fields(df: pd.DataFrame) -> pd.DataFrame:
#     # Coerce both to datetime and force them to UTC (tz-aware)
#     df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
#     df["model_timestamp"] = pd.to_datetime(
#         df["model_timestamp"], utc=True, errors="coerce"
#     )

#     # Now both are tz-aware UTC; subtraction will work
#     df["forecast_horizon"] = df["datetime"] - df["model_timestamp"]
#     df["forecast_horizon_hours"] = df["forecast_horizon"].dt.total_seconds() / 3600

#     df["is_model_timestamp_prior_day"] = (
#         df["model_timestamp"].dt.date == (df["datetime"] - pd.Timedelta(days=1)).dt.date
#     )

#     return df


def filter_redundant_forecasts(df):
    filtered_df = df[df["datetime"] > df["model_timestamp"]]
    deduped_df = filtered_df.drop_duplicates(
        subset=[
            "datetime",
            "forecast_temperature",
            "city",
            "model_timestamp",
            "model_name",
            "model_id",
            "date",
        ]
    )
    return deduped_df.reset_index(drop=True)


def filter_unused_forecast_data(df):
    mask = (
        df["forecast_temperature"].notna()
        & (df["city"] != "Houston")
        & (df["model_name"] != "best_match")
    )
    return df[mask].reset_index(drop=True)
