from sqlalchemy import create_engine
import pandas as pd
import datetime
import yfinance as yf

tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', '^GSPC'] # ^GSPC = S&P 500

end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=5*365)

print("Downloading data from Yahoo Finance...")
data = yf.download(tickers, start=start_date, end=end_date)

print("\nData shape:", data.shape)

print(data.columns)

if isinstance(data.columns, pd.MultiIndex):
    # stack ticker level (level=1 = 'Ticker') to rows
    tidy = data.stack(level=1).reset_index()   # columns now: ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', ...]
else:
    # single-ticker case: make columns consistent (put ticker label from tickers[0])
    data.columns = pd.MultiIndex.from_product([data.columns, [tickers[0]]])
    tidy = data.stack(level=1).reset_index()

tidy.columns = [str(c).lower().replace(' ', '_') for c in tidy.columns]

print("\nTidy (long) table sample:")
print(tidy.head(10))
print("\nTidy shape:", tidy.shape)
print("\nTidy columns:", tidy.columns.tolist())

db_string = 'postgresql+psycopg2://analyst:analyst123@localhost:1234/stock_analysis'

engine = create_engine(db_string)

try:
    tidy.to_sql(
        'stock_prices',        # This will be the table name in PostgreSQL
        con=engine,            # The connection engine
        if_exists='replace',   # 'replace', 'append', or 'fail'
        index=False,           # Don't send the pandas index as a column
        method='multi'         # Use 'multi' for faster insertion
    )
    print("Success! Data loaded into 'stock_prices' table in 'stock_analysis' database.")

except Exception as e:
    print(f"❌ An error occurred: {e}")
