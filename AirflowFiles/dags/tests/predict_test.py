import yfinance as yf
import pandas as pd

def test_yfinance(symbol="AAPL"):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="5d")

        if data.empty:
            print("No data returned")
            return False

        print("Fetched data:")
        print(data.head())
        return True

    except Exception as e:
        print(f"Error fetching data: {e}")
        return False


if __name__ == "__main__":
    test_yfinance()