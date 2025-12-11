def get_market_candlestick_data(
    client, series_ticker, market_ticker, start_ts, end_ts, period_interval
):
    response = client.get(
        path=f"/trade-api/v2/series/{series_ticker}/markets/{market_ticker}/candlesticks",
        params={
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
        },
    )

    return response


def get_event_candlestick_data(
    client, series_ticker, event_ticker, start_ts, end_ts, period_interval
):
    response = client.get(
        path=f"/trade-api/v2/series/{series_ticker}/events/{event_ticker}/candlesticks",
        params={
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
        },
    )

    return response
