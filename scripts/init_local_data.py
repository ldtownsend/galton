# scripts/init_local_data.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---- 1) Base path ----
BASE = Path("local_data")

# ---- 2) Schemas (align with your earlier Postgres columns) ----
SCHEMAS: dict[str, pa.schema] = {
    "location": pa.schema(
        [
            ("name", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("series_id", pa.string()),
            ("nws_id", pa.string()),
            ("tz", pa.string()),
            ("weather_id", pa.string()),
        ]
    ),
    # Facts (wide-open minimal schemas; add columns as you go)
    "observation": pa.schema(
        [
            ("city", pa.string()),
            ("station_id", pa.string()),
            ("valid_time_utc", pa.timestamp("us")),
            ("as_of_time_utc", pa.timestamp("us")),
            ("temp_c", pa.float64()),
            ("quality_flag", pa.string()),
            ("provider", pa.string()),
            ("prov_hash", pa.string()),
            ("raw_payload", pa.string()),
        ]
    ),
    "forecast": pa.schema(
        [
            ("city", pa.string()),
            ("provider", pa.string()),
            ("issue_time_utc", pa.timestamp("us")),
            ("valid_time_utc", pa.timestamp("us")),
            ("lead_hours", pa.int16()),
            ("temp_c", pa.float64()),
            ("model_run", pa.string()),
            ("as_of_time_utc", pa.timestamp("us")),
            ("prov_hash", pa.string()),
            ("raw_payload", pa.string()),
        ]
    ),
    "quote": pa.schema(
        [
            ("market_id", pa.string()),
            ("ts_utc", pa.timestamp("us")),
            ("side", pa.string()),  # 'bid'/'ask'
            ("price", pa.decimal128(8, 2)),
            ("size", pa.int32()),
            ("book_level", pa.int16()),
        ]
    ),
    "order": pa.schema(
        [
            ("account_id", pa.string()),
            ("market_id", pa.string()),
            ("ts_utc", pa.timestamp("us")),
            ("side", pa.string()),        # 'buy'/'sell'
            ("order_type", pa.string()),  # 'limit'/'market'
            ("price_limit", pa.decimal128(8, 2)),
            ("size", pa.int32()),
            ("time_in_force", pa.string()),
            ("decision_id", pa.string()),
        ]
    ),
    "execution": pa.schema(
        [
            ("account_id", pa.string()),
            ("broker_order_id", pa.string()),
            ("market_id", pa.string()),
            ("ts_utc", pa.timestamp("us")),
            ("side", pa.string()),
            ("price", pa.decimal128(8, 2)),
            ("size", pa.int32()),
            ("fee_usd", pa.decimal128(10, 2)),
            ("raw_payload", pa.string()),
        ]
    ),
    "cash_ledger": pa.schema(
        [
            ("account_id", pa.string()),
            ("ts_utc", pa.timestamp("us")),
            ("amount_usd", pa.decimal128(14, 2)),
            ("reason", pa.string()),
            ("ref_id", pa.string()),
            ("meta", pa.string()),
        ]
    ),
}

# ---- 3) Minimal Location registry (from your mapping) ----
CITIES = {
    "Austin": {
        "latitude": 30.1941,
        "longitude": -97.6711,
        "series_id": "KXHIGHAUS",
        "nws_id": "AUS",
        "tz": "America/Chicago",
        "weather_id": "05a7e7f73567bcc0ce8fac95250c4a0b26580d12fa0797e832d875d46ed01a30",
    },
    "Chicago": {
        "latitude": 41.7868,
        "longitude": -87.7522,
        "series_id": "KXHIGHCHI",
        "nws_id": "MDW",
        "tz": "America/Chicago",
        "weather_id": "4c9ff75840c6ce23fa10812d0f14b605af47896e9ca3fd59abdb9edd1b9d486a",
    },
    "Denver": {
        "latitude": 39.8563,
        "longitude": -104.6764,
        "series_id": "KXHIGHDEN",
        "nws_id": "DEN",
        "tz": "America/Denver",
        "weather_id": "b81fbb91071aa45b5119729b5bd58e27f8e397fc860ab0ba891fae1ef987cf2b",
    },
    "Houston": {
        "latitude": 29.6459,
        "longitude": -95.2769,
        "series_id": "KXHIGHHOU",
        "nws_id": "HOU",
        "tz": "America/Chicago",
        "weather_id": "da00666b99e29819289c87a0c1e7c4813ce274131a5131564adbb59caf76fcfe",
    },
    "Los Angeles": {
        "latitude": 33.9422,
        "longitude": -118.4036,
        "series_id": "KXHIGHLAX",
        "nws_id": "LAX",
        "tz": "America/Los_Angeles",
        "weather_id": "4facbbbb39938d43bf8e8f58a2f32dc61b6fd97d57c89ed8fd3ecbd8079003da",
    },
    "Miami": {
        "latitude": 25.7923,
        "longitude": -80.2823,
        "series_id": "KXHIGHMIA",
        "nws_id": "MIA",
        "tz": "America/New_York",
        "weather_id": "92cba1c3903ca3481419603edf4cce05fa6181189af9e61839ba3c941b378871",
    },
    "New York": {
        "latitude": 40.7826,
        "longitude": -73.9656,
        "series_id": "KXHIGHNY",
        "nws_id": "NYC",
        "tz": "America/New_York",
        "weather_id": "98e8083bb7de0fc467fd1e22a1692f8f200343e4e0acc3b3fc31e71d29113b54",
    },
    "Philadelphia": {
        "latitude": 39.87294,
        "longitude": -75.243988,
        "series_id": "KXHIGHPHIL",
        "nws_id": "PHL",
        "tz": "America/New_York",
        "weather_id": "1f6f9851651ae31314e564a45a97ac1e6780f295adf762161768fb73b3bc6450",
    },
}


def _ensure_dirs() -> None:
    BASE.mkdir(exist_ok=True)
    for tbl in ["fact_observation", "fact_forecast", "fact_quote", "fact_order", "fact_execution", "fact_cash_ledger"]:
        (BASE / tbl).mkdir(parents=True, exist_ok=True)


def _write_empty_if_missing(file: Path, schema: pa.schema) -> None:
    if file.exists():
        return
    empty = pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], names=[f.name for f in schema])
    pq.write_table(empty, file)


def init() -> None:
    _ensure_dirs()

    # Create empty dims if missing
    _write_empty_if_missing(BASE / "dim_location.parquet", SCHEMAS["dim_location"])
    _write_empty_if_missing(BASE / "dim_market_contract.parquet", SCHEMAS["dim_market_contract"])

    # Seed dim_location from CITIES (overwrite for idempotency)
    rows = []
    for name, v in CITIES.items():
        rows.append(
            {
                "name": name,
                "latitude": v["latitude"],
                "longitude": v["longitude"],
                "series_id": v["series_id"],
                "nws_id": v["nws_id"],
                "tz": v["tz"],
                "weather_id": v["weather_id"],
            }
        )
    df = pd.DataFrame(rows, columns=[f.name for f in SCHEMAS["dim_location"]])
    pq.write_table(pa.Table.from_pandas(df, schema=SCHEMAS["dim_location"]), BASE / "dim_location.parquet")

    print(f"Initialized local_data at {BASE.resolve()}")


if __name__ == "__main__":
    init()
