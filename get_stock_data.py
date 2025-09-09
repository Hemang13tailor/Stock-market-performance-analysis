# Import the necessary libraries
import yfinance as yf          # For downloading stock data
import pandas as pd            # For data manipulation
from sqlalchemy import create_engine # To connect to the SQL database
import datetime                # To handle dates

# --- STEP 1: Define the stocks and the date range ---
# You can change these tickers to any you are interested in.
# ^GSPC is the ticker for the S&P 500 index, which is good for market comparison.
tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', '^GSPC']
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=5*365) # 5 years of data

# --- STEP 2: Download the historical stock data ---
print(f"Downloading data for: {', '.join(tickers)}")

# yfinance returns data in a wide format. .stack() pivots the data to a long format,
# which is much better for database storage and analysis.
# .reset_index() turns the 'Date' and 'Ticker' from an index into regular columns.
try:
    data = yf.download(tickers, start=start_date, end=end_date).stack(level=1).rename_axis(['Date', 'Ticker']).reset_index()
    print("✅ Data download successful.")
    print("Here's a sample of the data:")
    print(data.head())
except Exception as e:
    print(f"❌ Failed to download data: {e}")
    exit() # Exit the script if download fails

# --- STEP 3: Connect to your SQL database and store the data ---
# This is the connection string. You MUST configure this for your setup.

# OPTION 1: For SQLite (Recommended for beginners)
# This will create a file named 'stock_market.db' in the same folder as your script.
db_string = 'sqlite:///stock_market.db'

# OPTION 2: For PostgreSQL (More advanced)
# You need to replace 'user', 'password', 'hostname', 'port', and 'database_name'
# with your own PostgreSQL credentials.
# db_string = 'postgresql://postgres:your_password@localhost:5432/stocks'

# Create the database engine
engine = create_engine(db_string)

# Load the data into a SQL table named 'daily_prices'.
# if_exists='replace' will drop the old table and create a new one.
# This is useful for re-running the script with fresh data.
print("\nWriting data to SQL database...")
try:
    # The column names from yfinance might have spaces or be in TitleCase.
    # Let's clean them up to be more database-friendly (lowercase_with_underscores).
    data.columns = [col.lower().replace(' ', '_') for col in data.columns]
    data.to_sql('daily_prices', engine, if_exists='replace', index=False)
    print("✅ Data successfully written to the 'daily_prices' table.")
except Exception as e:
    print(f"❌ Failed to write to database: {e}")


print("\n🎉 Phase 1 Complete! Your data is ready for analysis. 🎉")