import yfinance as yf
import pandas as pd
import numpy as np
import praw 
from talib import RSI, MACD
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from retrying import retry
from datetime import datetime, timedelta
import os

# Reddit API credentials (replace with your own)
reddit = praw.Reddit(
    client_id='B_3OP6wtYHBpgcxthSTyVQ',
    client_secret='BJC_8XZL1UHQp1Y2GTFDGiPcBPv1Qw',
    user_agent='Trying something new/0.1 by Quanteroooooni'
)

def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    table = pd.read_html(url)[0]
    return table['Symbol'].tolist()

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def get_stock_data(ticker, period, interval):
    stock = yf.Ticker(ticker)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # Fetch 1 week of data for all except weekly
    
    if interval == "1wk":
        start_date = end_date - timedelta(days=365)  # Fetch 1 year of data for weekly
    
    data = stock.history(start=start_date, end=end_date, interval=interval)
    
    if data.empty:
        raise ValueError(f"No trading data available for {ticker} in the specified period")
    
    return data

def calculate_indicators(data):
    data['RSI'] = RSI(data['Close'])
    data['MACD'], _, _ = MACD(data['Close'])
    return data

def check_divergence(price, indicator):
    if indicator is None or len(indicator) < 2:
        return False
    price_direction = np.sign(price.diff().iloc[-1])
    indicator_direction = np.sign(indicator.diff().iloc[-1])
    return price_direction != indicator_direction

def analyze_timeframes(ticker):
    weekly_data = get_stock_data(ticker, period="1y", interval="1wk")
    ninety_min_data = get_stock_data(ticker, period="7d", interval="90m")
    hourly_data = get_stock_data(ticker, period="7d", interval="1h")
    minute_15_data = get_stock_data(ticker, period="7d", interval="15m")

    weekly_data = calculate_indicators(weekly_data)
    ninety_min_data = calculate_indicators(ninety_min_data)
    hourly_data = calculate_indicators(hourly_data)
    minute_15_data = calculate_indicators(minute_15_data)

    divergences = {
        'weekly': {
            'RSI': check_divergence(weekly_data['Close'], weekly_data['RSI']),
            'MACD': check_divergence(weekly_data['Close'], weekly_data['MACD']),
        },
        'ninety_min': {
            'RSI': check_divergence(ninety_min_data['Close'], ninety_min_data['RSI']),
            'MACD': check_divergence(ninety_min_data['Close'], ninety_min_data['MACD']),
        },
        'hourly': {
            'RSI': check_divergence(hourly_data['Close'], hourly_data['RSI']),
            'MACD': check_divergence(hourly_data['Close'], hourly_data['MACD']),
        },
        'minute_15': {
            'RSI': check_divergence(minute_15_data['Close'], minute_15_data['RSI']),
            'MACD': check_divergence(minute_15_data['Close'], minute_15_data['MACD']),
        }
    }

    return divergences, weekly_data, ninety_min_data, hourly_data, minute_15_data

def grade_opportunity(ticker):
    divergences, weekly_data, ninety_min_data, hourly_data, minute_15_data = analyze_timeframes(ticker)
    
    # Calculate the grade based on divergences
    grade = 1  # Start with a base grade of 1
    weights = {'weekly': 0.4, 'ninety_min': 0.3, 'hourly': 0.2, 'minute_15': 0.1}
    
    for timeframe, indicators in divergences.items():
        for indicator, has_divergence in indicators.items():
            if has_divergence:
                grade += weights[timeframe]

    # Cap the grade at 5
    grade = min(grade, 5)

    # Determine if it's a long or short opportunity
    weekly_close = weekly_data['Close'].iloc[-1]
    ninety_min_close = ninety_min_data['Close'].iloc[-1]
    hourly_close = hourly_data['Close'].iloc[-1]
    minute_15_close = minute_15_data['Close'].iloc[-1]

    if weekly_close > ninety_min_close > hourly_close > minute_15_close:
        trade_type = 'short'
    elif weekly_close < ninety_min_close < hourly_close < minute_15_close:
        trade_type = 'long'
    else:
        trade_type = 'neutral'

    return round(grade, 2), trade_type

def get_market_cap(ticker):
    stock = yf.Ticker(ticker)
    return stock.info.get('marketCap', 0)

def calculate_size_weight(market_cap):
    if market_cap < 2e9:  # Small Cap
        return 0.8
    elif market_cap < 10e9:  # Mid Cap
        return 1.0
    else:  # Large Cap
        return 1.2

def analyze_stock(ticker):
    try:
        time.sleep(1)  # Add a 1-second delay between API calls
        grade, trade_type = grade_opportunity(ticker)
        market_cap = get_market_cap(ticker)
        size_weight = calculate_size_weight(market_cap)
        weighted_grade = grade * size_weight
        return (ticker, grade, weighted_grade, trade_type, market_cap)
    except Exception as e:
        print(f"Error analyzing {ticker}: {str(e)}")
        return (ticker, np.nan, np.nan, 'error', np.nan)

def main():
    print("Fetching S&P 500 tickers...")
    tickers = get_sp500_tickers()
    
    print(f"Analyzing {len(tickers)} stocks...")
    results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(analyze_stock, ticker): ticker for ticker in tickers}
        for future in tqdm(as_completed(future_to_ticker), total=len(tickers)):
            result = future.result()
            if result:
                results.append(result)
    
    # Filter out stocks with NaN values before sorting
    valid_results = [r for r in results if not np.isnan(r[2])]
    sorted_results = sorted(valid_results, key=lambda x: x[2], reverse=True)
    
    # Get top 3 results
    top_tickers = sorted_results[:3] 

    # Print the results
    print(f"\nTop 3 Opportunities as of {time.strftime('%Y-%m-%d %H:%M:%S')}:")
    print("=" * 80)
    print(f"{'Ticker':<10}{'Raw Grade':<15}{'Weighted Grade':<20}{'Trade Type':<15}{'Market Cap':<20}{'Timestamp':<20}")
    print("-" * 80)
    
    # Prepare data for CSV
    csv_data = []
    
    for ticker, grade, weighted_grade, trade_type, market_cap in top_tickers:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get the current date and time
        csv_data.append((ticker, grade, weighted_grade, trade_type, market_cap, timestamp))
        print(f"{ticker:<10}{grade:<15.2f}{weighted_grade:<20.4f}{trade_type:<15}{market_cap:,.0f}{timestamp:<20}")

    print("=" * 80)

    # Save top 3 results to CSV
    csv_file_path = 'top_divergence_tickers.csv'
    
    if not os.path.isfile(csv_file_path):
        # If the file does not exist, create it and write the header
        with open(csv_file_path, 'w') as f:
            f.write('Ticker,Raw Grade,Weighted Grade,Trade Type,Market Cap,Timestamp\n')
    
    # Append the results to the CSV file with the current timestamp
    with open(csv_file_path, 'a') as f:
        for ticker, grade, weighted_grade, trade_type, market_cap, timestamp in csv_data:
            f.write(f'{ticker},{grade},{weighted_grade},{trade_type},{market_cap},{timestamp}\n')

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProgram stopped by user.")