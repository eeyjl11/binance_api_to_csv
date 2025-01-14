import requests
import pandas as pd
import time
from datetime import datetime

def fetch_binance_data(symbol, interval, start_time, end_time, limit):
    """

    Returns candlestick data from binanceapi: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/Kline-Candlestick-Data

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick
        start_time -> the date and time you wish to save data from
        end_time -> the date and time you wish to save data to
        limit -> amount of data to save per api call

    RETURNS:
        response.json -> returns json response of api call
    """
    base_url = "https://api.binance.com/api/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }
    response = requests.get(base_url, params=params)
    return response.json()

def convert_to_dataframe(data):
    """
    Converts json api response to a pandas dataframe
    
    PARAMETERS:
        data -> data return from binance api

    RETURNS:
        df -> dataframe created from binance data
    """
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume", "closeTime", "baseAssetVolume", "numberOfTrades", "takerBuyVolume", "takerBuyBaseAssetVolume", "Ignore"
    ])

    # Convert timestamp to datetime
    df["datetime"] = pd.to_datetime(df["timestamp"], unit='ms')

    df = df[["datetime", "open", "high", "low", "close", "volume"]]
    df.set_index("datetime", inplace = True)

    return df

def get_full_data(symbol, interval, start_time, end_time):
    """
    Returns the full range of data from start and end date, due to limit restrictions from api

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick
        start_time -> the date and time you wish to save data from
        end_time -> the date and time you wish to save data to

    RETURNS:
        df -> Returns dataframe of candlestick data for symbol over requested time period
    """
    df_list = []
    limit = 1500

    # Loops as data has to be broken up into chunks due to the 1500 limit to the binance api
    while start_time < end_time:
        df = convert_to_dataframe(fetch_binance_data(symbol, interval, start_time, end_time, limit))
        df_list.append(df)
        start_time = int(df.index.max().timestamp() * 1e3) + 1

        # Sleep to avoid rate limit
        time.sleep(1)

    # Combine dataframe and return
    return pd.concat(df_list)

def allData(symbol, interval):
    """
    Returns all historical data of symbol

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick

    RETURNS:
        df -> Returns dataframe of candlestick data for symbol
    """
    start_time = int(pd.Timestamp("2017-01-01").timestamp() * 1000)  # Start date in milliseconds
    now = datetime.now().timestamp()*1e3 # Get current timestamp in milliseconds
    end_time = int(now)  # End date in milliseconds
    
    # Fetch the data
    crypto_data = get_full_data(symbol, interval, start_time, end_time)

    # Save data to CSV
    csv_filename = f"{symbol}_{interval}_data.csv"
    crypto_data.to_csv(f"Data/{csv_filename}")
    print(f"Data saved to {csv_filename}")

def specificData(symbol,interval, startDate, endDate):
    """
    Returns the data from start to end date for requested symbol

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick
        start_time -> the date and time you wish to save data from (YYY-MM-DD)
        end_time -> the date and time you wish to save data to (YYY-MM-DD)

    RETURNS:
        df -> Returns dataframe of candlestick data for symbol over requested time period
    """
    start_time = int(pd.Timestamp(startDate).timestamp() * 1000)  # Start date in milliseconds
    end_time = int(pd.Timestamp(endDate).timestamp() * 1000)  # End date in milliseconds

    # Fetch the data
    crypto_data = get_full_data(symbol, interval, start_time, end_time)

    # Save data to CSV
    csv_filename = f"{symbol}_{interval}_{startDate}_to_{endDate}.csv"
    crypto_data.to_csv(f"Data/{csv_filename}")
    print(f"Data saved to {csv_filename}")

specificData("BTCUSDT", "1m", "2021-10-01", "2021-10-03")
#allData("BTCUSDT", "1w")