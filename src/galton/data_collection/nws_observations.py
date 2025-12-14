from __future__ import annotations

import pandas as pd

from galton.data_collection.city_reference import cities
from galton.data_collection.utilities import save, get_current_timestamp


def read_nws_table(
    nws_id,
    column_numbers=[0, 1, 2, 4, 6, 8],
    column_names=["date", "time", "wind", "weather", "air", "6_hour_max"],
):

    df = pd.read_html(f"https://forecast.weather.gov/data/obhistory/K{nws_id}.html")[0]

    # Select columns by column number
    df_selected = df.iloc[:, column_numbers]

    # Rename Columns
    new_column_names = column_names
    df_selected.columns = new_column_names

    # Remove Footer
    filtered_df = df_selected[~df_selected["date"].str.contains("Date", na=False)]
    filtered_df = filtered_df.reset_index(drop=True)

    return filtered_df


def get_nws_data(cities):

    all_cities_df = pd.DataFrame()

    for city in cities.keys():
        nws_id = cities[city]["nws_id"]

        current_timestamp = get_current_timestamp()
        nws_update = read_nws_table(nws_id)

        nws_update["timestamp"] = current_timestamp
        nws_update["city"] = city

        all_cities_df = pd.concat([all_cities_df, nws_update]).reset_index(drop=True)

    save(
        all_cities_df,
        path=f"data/staging/nws_observations",
        file_name="nws_observations",
        current_timestamp=current_timestamp,
    )

    print(f"NWS History shape: {all_cities_df.shape} -- {current_timestamp}")
