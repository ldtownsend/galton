from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timezone
from pathlib import Path

import os
import pandas as pd
import re
import requests
import time


try:
    # Optional: auto-load .env if python-dotenv is installed.
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


# ---- Your city reference (expects city_reference.py on PYTHONPATH) ----------
# city_reference.py contains:
# cities = {"Chicago": {"latitude": ..., "longitude": ...}, ...}
try:
    from galton.data_collection.city_reference import cities as CITY_REFERENCE
except Exception as exc:  # pragma: no cover
    CITY_REFERENCE = {}
    _city_ref_import_error = exc
else:
    _city_ref_import_error = None


ACCU_BASE = "https://dataservice.accuweather.com"


class AccuWeatherError(RuntimeError):
    pass


@dataclass
class AccuWeatherClient:
    """
    If api_key is None, the client will look for ACCUWEATHER_API_KEY
    in the environment. If python-dotenv is installed, it will lazily
    load a .env file (without overriding existing env vars).
    """

    api_key: Optional[str] = None
    session: Optional[requests.Session] = None
    timeout: Tuple[float, float] = (10.0, 30.0)  # (connect, read)

    def __post_init__(self) -> None:
        if self.api_key is None:
            # Load .env if available (no-op if dotenv isn't installed)
            if load_dotenv is not None:
                load_dotenv(override=False)
            self.api_key = os.getenv("ACCUWEATHER_API_KEY")

        if not self.api_key:
            raise AccuWeatherError(
                "Missing AccuWeather API key. "
                "Set ACCUWEATHER_API_KEY in your environment or .env."
            )

    # ---- Internal HTTP helper ------------------------------------------------

    def _get(self, path: str, params: Dict[str, Any], retries: int = 3) -> Any:
        url = f"{ACCU_BASE}{path}"
        s = self.session or requests.Session()
        params = dict(params or {})
        params["apikey"] = self.api_key

        last_exc: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                resp = s.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp.json()
                raise AccuWeatherError(
                    f"AccuWeather API error {resp.status_code} for {url} "
                    f"with params {params}: {resp.text[:500]}"
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                if attempt == retries:
                    break
                time.sleep(0.75 * attempt)
        raise AccuWeatherError(f"Network error calling {url}: {last_exc}")

    # ---- Public helpers ------------------------------------------------------

    def get_location_key_by_geoposition(
        self,
        lat: float,
        lon: float,
        language: str = "en-us",
        details: bool = False,
        toplevel: Optional[bool] = None,
    ) -> str:
        params: Dict[str, Any] = {
            "q": f"{lat:.6f},{lon:.6f}",
            "language": language,
            "details": str(details).lower(),
        }
        if toplevel is not None:
            params["toplevel"] = str(toplevel).lower()

        data = self._get("/locations/v1/cities/geoposition/search", params=params)
        try:
            key = data["Key"]
        except Exception as exc:
            raise AccuWeatherError(
                f"Unexpected Geoposition response shape: {data}"
            ) from exc
        return key

    def get_hourly_forecast_12h(
        self,
        location_key: str,
        *,
        metric: bool = True,
        details: bool = True,
        language: str = "en-us",
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "metric": str(metric).lower(),
            "details": str(details).lower(),
            "language": language,
        }
        path = f"/forecasts/v1/hourly/12hour/{location_key}"
        data = self._get(path, params=params)
        if not isinstance(data, list):
            raise AccuWeatherError(f"Unexpected hourly forecast payload: {data}")
        return data

    def get_hourly_forecast_12h_by_latlon(
        self,
        lat: float,
        lon: float,
        *,
        metric: bool = True,
        details: bool = True,
        language: str = "en-us",
        as_dataframe: bool = True,
    ) -> Union[List[Dict[str, Any]], "pd.DataFrame"]:
        location_key = self.get_location_key_by_geoposition(
            lat=lat, lon=lon, language=language
        )
        hourly12 = self.get_hourly_forecast_12h(
            location_key, metric=metric, details=details, language=language
        )

        if as_dataframe:
            if pd is None:
                raise AccuWeatherError(
                    "pandas is not installed; set as_dataframe=False to get raw JSON."
                )
            df = pd.json_normalize(hourly12)
            for col_old, col_new in [
                ("DateTime", "datetime"),
                ("Temperature.Value", "temp"),
                ("Temperature.Unit", "temp_unit"),
                ("RealFeelTemperature.Value", "realfeel"),
                ("RealFeelTemperature.Unit", "realfeel_unit"),
                ("IconPhrase", "phrase"),
                ("HasPrecipitation", "has_precip"),
                ("PrecipitationProbability", "precip_prob"),
                ("Wind.Speed.Value", "wind_speed"),
                ("Wind.Speed.Unit", "wind_unit"),
                ("Wind.Direction.Localized", "wind_dir"),
                ("RelativeHumidity", "rh"),
                ("UVIndex", "uv_index"),
            ]:
                if col_old in df.columns:
                    df.rename(columns={col_old: col_new}, inplace=True)
            return df

        return hourly12

    # ---- City reference convenience -----------------------------------------

    def get_city_coords(self, city_name: str) -> Tuple[float, float]:
        """
        Resolve a city's latitude/longitude using city_reference.cities.
        """
        if not CITY_REFERENCE and _city_ref_import_error is not None:
            raise AccuWeatherError(
                "Unable to import city_reference.cities. "
                f"Import error: {_city_ref_import_error}"
            )

        if city_name not in CITY_REFERENCE:
            # Offer closest matches by simple case-insensitive search
            lower = {k.lower(): k for k in CITY_REFERENCE}
            if city_name.lower() in lower:
                city_name = lower[city_name.lower()]
            else:
                known = ", ".join(sorted(CITY_REFERENCE.keys()))
                raise AccuWeatherError(
                    f"City '{city_name}' not found in city_reference.cities. "
                    f"Known: {known}"
                )

        c = CITY_REFERENCE[city_name]
        try:
            return float(c["latitude"]), float(c["longitude"])
        except Exception as exc:
            raise AccuWeatherError(
                f"City entry for '{city_name}' is missing 'latitude'/'longitude': {c}"
            ) from exc

    def get_hourly_forecast_12h_by_city(
        self,
        city_name: str,
        *,
        metric: bool = True,
        details: bool = True,
        language: str = "en-us",
        as_dataframe: bool = True,
    ) -> Union[List[Dict[str, Any]], "pd.DataFrame"]:
        """
        Convenience: city name -> lat/lon (from city_reference.py) -> 12h forecast.
        """
        lat, lon = self.get_city_coords(city_name)
        return self.get_hourly_forecast_12h_by_latlon(
            lat=lat,
            lon=lon,
            metric=metric,
            details=details,
            language=language,
            as_dataframe=as_dataframe,
        )

    def get_hourly_forecast_12h_all_cities(
        self,
        *,
        metric: bool = True,
        details: bool = True,
        language: str = "en-us",
        add_fahrenheit: bool = False,
        include_city_meta: bool = True,
    ) -> "pd.DataFrame":
        """
        Fetch 12h forecasts for every city in city_reference.cities and concatenate.

        Returns:
            pd.DataFrame with one row per hour per city.
        Raises:
            AccuWeatherError if all fetches fail or pandas is missing.
        """
        if pd is None:
            raise AccuWeatherError("pandas is required for concatenated results.")

        frames: List[pd.DataFrame] = []
        failures: Dict[str, str] = {}

        for city, meta in CITY_REFERENCE.items():
            try:
                df = self.get_hourly_forecast_12h_by_city(
                    city_name=city,
                    metric=metric,
                    details=details,
                    language=language,
                    as_dataframe=True,
                )
                # Tag with city + coordinates
                df["city"] = city
                df["latitude"] = float(meta.get("latitude"))
                df["longitude"] = float(meta.get("longitude"))

                # Optional: attach your other city_reference metadata
                if include_city_meta:
                    for k in ("series_id", "nws_id", "tz_convert", "weather_id"):
                        if k in meta:
                            df[k] = meta[k]

                # Optional: Fahrenheit column alongside whatever unit came back
                if add_fahrenheit and "temp" in df and "temp_unit" in df:
                    if str(df["temp_unit"].iloc[0]).upper() == "C":
                        df["temp_f"] = df["temp"] * 9.0 / 5.0 + 32.0
                    else:
                        df["temp_f"] = df["temp"]

                frames.append(df)

            except Exception as exc:
                failures[city] = str(exc)

        if not frames:
            raise AccuWeatherError(f"All city fetches failed: {failures}")

        out = pd.concat(frames, ignore_index=True)

        # Make sure datetime is parsed to a proper dtype if present
        if "datetime" in out.columns:
            out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")

        # Optional: bring rows into a consistent order
        sort_cols = [c for c in ("city", "datetime") if c in out.columns]
        if sort_cols:
            out.sort_values(sort_cols, inplace=True, ignore_index=True)

        # Surface failures but still return the successful rows
        if failures:
            print(f"Note: {len(failures)} city(ies) failed -> {failures}")

        return out


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# -------------------------
# Core staging function
# -------------------------


def stage_accuweather_12h_all_cities(
    output_dir: str = "data/staging/accuweather_forecasts/",
    *,
    metric: bool = False,
    details: bool = True,
    language: str = "en-us",
    add_fahrenheit: bool = False,
    include_city_meta: bool = True,
    model_name: str = "accuweather_12h",
    model_id: str = "1001",
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Fetch 12-hour AccuWeather forecasts for all cities in CITY_REFERENCE,
    annotate with metadata, and save one parquet per city into `output_dir`.

    Args:
        output_dir: Folder to write staged parquet files (one per city per run).
        metric: If False, API returns Fahrenheit directly; if True, Celsius.
        details: Pass-through for AccuWeather details payload.
        language: API language code.
        add_fahrenheit: If True and API returned °C, also add 'temp_f'.
        include_city_meta: Include extra city_reference fields if present.
        model_name: Value for 'model_name' column.
        model_id: Value for 'model_id' column.
        verbose: Print per-city progress.

    Returns:
        Concatenated DataFrame of all successfully-staged city forecasts.
    """
    client = AccuWeatherClient()
    out_path = Path(output_dir)
    _ensure_dir(out_path)

    timestamp = datetime.now(timezone.utc)
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")

    frames: List[pd.DataFrame] = []
    failures: Dict[str, str] = {}

    for city, meta in CITY_REFERENCE.items():
        try:
            df = client.get_hourly_forecast_12h_by_city(
                city_name=city,
                metric=metric,
                details=details,
                language=language,
                as_dataframe=True,
            )

            # Ensure datetime dtype
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

            # Add standardized columns used by your aggregator
            df["forecast_temperature"] = df.get("temp")
            df["city"] = city
            df["model_timestamp"] = timestamp
            df["model_name"] = model_name
            df["model_id"] = model_id

            # Include coordinates + other metadata from city_reference
            df["latitude"] = float(meta.get("latitude"))
            df["longitude"] = float(meta.get("longitude"))
            if include_city_meta:
                for k in ("series_id", "nws_id", "tz_convert", "weather_id"):
                    if k in meta:
                        df[k] = meta[k]

            # Optional Fahrenheit alongside whatever the API returned
            if add_fahrenheit and "temp" in df and "temp_unit" in df:
                if str(df["temp_unit"].iloc[0]).upper() == "C":
                    df["temp_f"] = df["temp"] * 9.0 / 5.0 + 32.0
                else:
                    df["temp_f"] = df["temp"]

            # File per city
            city_slug = _slugify(city)
            fname = f"accuweather_12h_{city_slug}_{ts_str}.parquet"
            fpath = out_path / fname
            df.to_parquet(fpath, index=False)
            if verbose:
                print(f"Saved {city}: {fpath}")

            frames.append(df)

        except Exception as exc:  # noqa: BLE001
            failures[city] = str(exc)
            if verbose:
                print(f"FAILED {city}: {exc}")

    if not frames:
        msg = f"All city fetches failed: {failures}" if failures else "No cities found."
        raise AccuWeatherError(msg)

    result = pd.concat(frames, ignore_index=True)

    if verbose and failures:
        print(f"Note: {len(failures)} city(ies) failed -> {failures}")

    return result
