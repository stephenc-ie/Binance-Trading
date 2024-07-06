from binance.client import Client
from binance.enums import *
import pandas as pd
import numpy as np
import time
from keys import api, secret

client = Client(api, secret)

def get_historical_data(symbol, interval, lookback):
    print(f"Fetching {lookback} minutes of data for {symbol}...")
    frame = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback + ' min ago UTC'))
    frame = frame.iloc[:, :6]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame

def apply_strategy(df, short_window, long_window):
    if df.empty:
        print("DataFrame is empty after fetching data.")
        return df
    df['Short_MA'] = df['Close'].rolling(window=short_window).mean()
    df['Long_MA'] = df['Close'].rolling(window=long_window).mean()
    print(f"Before dropna: {df.tail(5)}")  
    df.dropna(inplace=True)
    print(f"After dropna: {df.tail(5)}")  
    if df.empty:
        print("DataFrame is empty after applying strategy and dropping NaNs.")
        return df
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
    symbol = 'BTCUSDT'
    qty = 0.0001  
    short_window = 50
    long_window = 200
    lookback_minutes = 300
    while True:
        df = get_historical_data(symbol, '1m', str(lookback_minutes))  # Fetching data
        if df.empty:
            print("No data fetched for the given symbol and interval.")
            time.sleep(60)
            continue
        df = apply_strategy(df, short_window, long_window)
        if df.empty:
            print("No data after applying strategy.")
            lookback_minutes += 100  # Increase lookback period if data is still not sufficient
            print(f"Increasing lookback period to {lookback_minutes} minutes.")
            time.sleep(60)
            continue
        last_row = df.iloc[-1]
        print("Last Row['Position']: ", last_row['Position'])
        if last_row['Position'] == 1:
            print("Buy Signal")
            execute_order(symbol, qty, SIDE_BUY)
        elif last_row['Position'] == -1:
            print("Sell Signal")
            execute_order(symbol, qty, SIDE_SELL)
        time.sleep(60)  # Run the loop every minute

if __name__ == "__main__":
    run_bot()
