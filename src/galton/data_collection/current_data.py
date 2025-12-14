import pandas as pd
import duckdb


def load_current_data(
    dataset_name: str, start_date_field: str, n_latest_dates: int
) -> pd.DataFrame:

    con = duckdb.connect()

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {dataset_name} AS
        SELECT *
        FROM read_parquet('data/local_data/{dataset_name}/**/*.parquet');
    """
    )

    query = f"""
    WITH latest_dates AS (
        SELECT DISTINCT {start_date_field}
        FROM {dataset_name}
        ORDER BY {start_date_field} DESC
        LIMIT {n_latest_dates}
    )
    SELECT f.*
    FROM {dataset_name} f
    JOIN latest_dates d
    USING ({start_date_field})
    """

    con.execute(query)
    df_current = con.fetch_df()
    return df_current
