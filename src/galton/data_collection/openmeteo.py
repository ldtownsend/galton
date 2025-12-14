import openmeteo_requests
import pandas as pd
import pytz
import requests_cache

from retry_requests import retry
from datetime import datetime

from galton.data_collection.city_reference import cities
from galton.data_collection.utilities import save, get_current_timestamp


def connect_to_openmeteo():
    cache_session = requests_cache.CachedSession(".cache", expire_after=300)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    return openmeteo


def update_multi_model_forecast_params(cities, city, forecast_days):

    latitude = cities[city]["latitude"]
    longitude = cities[city]["longitude"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "forecast_days": forecast_days,
        "current": "temperature_2m",
        "hourly": ["temperature_2m"],
        "temperature_unit": "fahrenheit",
        "models": [
            "best_match",
            "ecmwf_ifs04",
            "ecmwf_ifs025",
            "ecmwf_aifs025",
            "gfs_global",
            "gfs_hrrr",
            "ncep_nbm_conus",
            "gfs_graphcast025",
            "jma_gsm",
            "icon_global",
            "gem_global",
            "gem_regional",
            "meteofrance_arpege_world",
            "ukmo_global_deterministic_10km",
        ],
    }

    return params


# def get_current_timestamp(time_zone="US/Central"):
#     central_tz = pytz.timezone(time_zone)
#     current_timestamp = datetime.now(central_tz)
#     return current_timestamp


def add_multi_model_metadata(
    forecast_update, city, model_timestamp, current_timestamp, model_name, model_id
):

    forecast_update["city"] = city
    forecast_update["model_timestamp"] = model_timestamp
    forecast_update["current_timestamp"] = current_timestamp
    forecast_update["model_name"] = model_name
    forecast_update["model_id"] = model_id

    return forecast_update


def parse_multi_model_response(responses, params, city):
    current_timestamp = get_current_timestamp()
    hourly_df = pd.DataFrame()
    for model in range(len(responses)):

        response = responses[model]
        model_id = response.Model()

        model_name = params["models"][model]

        current = response.Current()
        model_timestamp = pd.to_datetime(current.Time(), unit="s", utc=True).tz_convert(
            "US/Central"
        )

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

        hourly_data = {
            "forecast_date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True).tz_convert(
                    "US/Central"
                ),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True).tz_convert(
                    "US/Central"
                ),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }

        hourly_data["temperature_2m"] = hourly_temperature_2m

        hourly_dataframe = pd.DataFrame(data=hourly_data)
        hourly_dataframe = add_multi_model_metadata(
            hourly_dataframe,
            city,
            model_timestamp,
            current_timestamp,
            model_name,
            model_id,
        )

        hourly_df = pd.concat([hourly_df, hourly_dataframe])

    return hourly_df, current_timestamp


def get_multi_model_forecast(cities):

    openmeteo = connect_to_openmeteo()

    forecast_url = "https://api.open-meteo.com/v1/forecast"

    request_count = 0
    all_cities_df = pd.DataFrame()
    for city in cities.keys():
        multi_model_forecast_params = update_multi_model_forecast_params(
            cities, city, forecast_days=3
        )
        responses = openmeteo.weather_api(
            forecast_url, params=multi_model_forecast_params
        )
        request_count += 1
        hourly_df, current_timestamp = parse_multi_model_response(
            responses, params=multi_model_forecast_params, city=city
        )
        all_cities_df = pd.concat([all_cities_df, hourly_df])

    save(
        all_cities_df,
        path=f"data/staging/openmeteo_forecasts",
        file_name="multi_model_forecasts",
        current_timestamp=current_timestamp,
    )
