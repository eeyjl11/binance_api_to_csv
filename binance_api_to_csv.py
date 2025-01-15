import requests
import pandas as pd
import time
from datetime import datetime

def fetchBinanceData(symbol, interval, startTime, endTime, limit):
    """

    Returns candlestick data from binanceapi: https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/Kline-Candlestick-Data

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick
        startTime -> the date and time you wish to save data from
        endTime -> the date and time you wish to save data to
        limit -> amount of data to save per api call

    RETURNS:
        response.json -> returns json response of api call
    """
    baseUrl = "https://api.binance.com/api/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": startTime,
        "endTime": endTime,
        "limit": limit
    }
    response = requests.get(baseUrl, params=params)
    return response.json()

def convertToDataframe(data):
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
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

    df = df[["datetime", "open", "high", "low", "close", "volume"]]
    df.set_index("datetime", inplace = True)

    return df

def getFullData(symbol, interval, startTime, endTime):
    """
    Returns the full range of data from start and end date, due to limit restrictions from api

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick
        startTime -> the date and time you wish to save data from
        endTime -> the date and time you wish to save data to

    RETURNS:
        df -> Returns dataframe of candlestick data for symbol over requested time period
    """
    dfList = []
    limit = 1500

    # Loops as data has to be broken up into chunks due to the 1500 limit to the binance api
    while startTime < endTime:
        data = fetchBinanceData(symbol, interval, startTime, endTime, limit)
        df = convertToDataframe(data)
        dfList.append(df)

        if df.empty:
            break

        startTime = int(df.index.max().timestamp() * 1e3) + 1

        # Sleep to avoid rate limit
        time.sleep(1)

    # Combine dataframe and return
    return pd.concat(dfList)

def allData(symbol, interval):
    """
    Returns all historical data of symbol

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick

    RETURNS:
        df -> Returns dataframe of candlestick data for symbol
    """
    startTime = int(pd.Timestamp("2017-01-01").timestamp() * 1e3)  # Start date in milliseconds
    now = datetime.now().timestamp()*1e3 # Get current timestamp in milliseconds
    endTime = int(now)  # End date in milliseconds
    
    # Fetch the data
    cryptoData = getFullData(symbol, interval, startTime, endTime)

    # Save data to CSV
    csvFilename = f"{symbol}_{interval}_all_data.csv"
    cryptoData.to_csv(f"Data/{csvFilename}")
    print(f"Data saved to {csvFilename}")

def specificData(symbol,interval, startDate, endDate):
    """
    Returns the data from start to end date for requested symbol

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to save
        interval -> the time interval of the candlestick
        startTime -> the date and time you wish to save data from (YYY-MM-DD)
        endTime -> the date and time you wish to save data to (YYY-MM-DD)

    RETURNS:
        df -> Returns dataframe of candlestick data for symbol over requested time period
    """
    startTime = int(pd.Timestamp(startDate).timestamp() * 1000)  # Start date in milliseconds
    endTime = int(pd.Timestamp(endDate).timestamp() * 1000)  # End date in milliseconds

    # Fetch the data
    cryptoData = getFullData(symbol, interval, startTime, endTime)

    # Save data to CSV
    csvFilename = f"{symbol}_{interval}_{startDate}_to_{endDate}.csv"
    cryptoData.to_csv(f"Data/{csvFilename}")
    print(f"Data saved to {csvFilename}")

def updateAllData(symbol, interval):
    """
    Updates an already created instance of all data to current timestamp

    PARAMETERS:
        symbol -> the symbol of the cryptocurrency you want to update
        interval -> the time interval of the crypto you want to update

    RETURNS:
        df -> Returns updated dataframe for symbol
    """
    # Load in corresponding csv
    csvFilename = f"{symbol}_{interval}_all_data.csv"
    cryptoData = pd.read_csv(f"Data/{csvFilename}", parse_dates=["datetime"], index_col="datetime")
    
    startTime = int(cryptoData.index.max().timestamp() * 1e3) + 1 # Get last timestamp in milliseconds
    now = datetime.now().timestamp()*1e3 # Get current timestamp in milliseconds
    endTime = int(now)

    df = getFullData(symbol, interval, startTime, endTime)
    cryptoData = pd.concat([cryptoData, df])

    cryptoData.to_csv(f"Data/{csvFilename}")
    print(f"Data saved to {csvFilename}")

#specificData("BTCUSDT", "1m", "2025-01-12", "2025-01-13")
#allData("BTCUSDT", "1m")
#updateAllData("BTCUSDT", "1m")