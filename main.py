from binance.client import Client
from binance.enums import *
import pandas as pd
import numpy as np
import time
from keys import api, secret

client = Client(api, secret)


# Function to fetch historical data for a given symbol and interval
def get_historical_data(symbol, interval, lookback):
    print(f"Fetching {lookback} minutes of data for {symbol}...")
    try:
        # Fetch historical klines (candlestick data) from Binance
        frame = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback + ' min ago UTC'))
        
        # Check if data was returned
        if frame.empty or len(frame.columns) < 6:
            print(f"No data returned for {symbol}.")
            return pd.DataFrame()  # Return an empty DataFrame if no data
        
        # Keep only the first 6 columns and rename them
        frame = frame.iloc[:, :6]
        frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # Set the 'Time' column as the index and convert to datetime
        frame = frame.set_index('Time')
        frame.index = pd.to_datetime(frame.index, unit='ms')
        
        # Convert all columns to float type
        frame = frame.astype(float)
        return frame
    except Exception as e:
        print(f"An error occurred while fetching data for {symbol}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


# Function to apply a moving average crossover strategy
def apply_strategy_moving_average(df, short_window, long_window):
    if df.empty:
        print("DataFrame is empty after fetching data.")
        return df
    
    # Calculate the short and long moving averages
    df['Short_MA'] = df['Close'].rolling(window=short_window).mean()
    df['Long_MA'] = df['Close'].rolling(window=long_window).mean()
    print(f"Before dropna: {df.tail(5)}")  # Debugging output
    df.dropna(inplace=True)   # Drop rows with NaN values
    print(f"After dropna: {df.tail(5)}")  # Debugging output
    if df.empty:
        print("DataFrame is empty after applying strategy and dropping NaNs.")
        return df
    
    # Generate buy/sell signals
    df['Signal'] = 0
    df.loc[df.index[short_window:], 'Signal'] = np.where(df['Short_MA'][short_window:] > df['Long_MA'][short_window:], 1, 0)
    df['Position'] = df['Signal'].diff()
    return df


def execute_order(symbol, qty, side, order_type=ORDER_TYPE_MARKET):
    try:
        order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=qty)
        print(f"Order executed: {order}")
    except Exception as e:
        print(f"An exception occurred - {e}")
        return False
    return True


def run_bot():
    symbols = ['BTCUSDT', 'BTCBUSD', 'BTCUSDC', 'BTCBNB']
    qty = 0.0001  # Adjusted Quantity to meet minimum order size
    short_window = 50
    long_window = 200
    lookback_minutes = 300  # Increased lookback period to ensure enough data

    while True:
        for symbol in symbols:
            df = get_historical_data(symbol, '1m', str(lookback_minutes))  # Fetching data
            if df.empty:
                print(f"No data fetched for {symbol}.")
                time.sleep(15)
                continue
            df = apply_strategy_moving_average(df, short_window, long_window)
            if df.empty:
                print(f"No data after applying strategy for {symbol}.")
                lookback_minutes += 100  # Increase lookback period if data is still not sufficient
                print(f"Increasing lookback period for {symbol} to {lookback_minutes} minutes.")
                time.sleep(15)
                continue
            last_row = df.iloc[-1]
            print(f"{symbol} Last Row['Position']: {last_row['Position']}")
            if last_row['Position'] == 1:
                print(f"{symbol} Buy Signal")
                #execute_order(symbol, qty, SIDE_BUY)
            elif last_row['Position'] == -1:
                print(f"{symbol} Sell Signal")
                #execute_order(symbol, qty, SIDE_SELL)
            time.sleep(15)  # Wait a bit before fetching the next symbol data to avoid rate limits
        print("Completed a cycle of checking all symbols. Waiting 1 minute before the next cycle.")
        time.sleep(60)  # Wait for a minute before the next cycle


if __name__ == "__main__":
    run_bot()
