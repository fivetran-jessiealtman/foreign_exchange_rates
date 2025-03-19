# This is a Fivetran connector that fetches stock price data from Financial Modeling Prep API.
# It demonstrates how to use the fivetran_connector_sdk to create a connector that 
# retrieves historical stock prices for a specified ticker symbol for the last 30 days.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

from datetime import datetime, timedelta  # Import datetime for handling date and time conversions

import requests as rq  # Import the requests module for making HTTP requests, aliased as rq
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "stock_prices",  # Name of the table in the destination
            "primary_key": ["date", "symbol"],  # Primary key columns for the table
            "columns": {  # Define the columns and their data types
                "symbol": "STRING",  # String column for the stock symbol
                "date": "UTC_DATETIME",  # Date column for the price date
                "open": "FLOAT",  # Float column for opening price
                "high": "FLOAT",  # Float column for highest price
                "low": "FLOAT",  # Float column for lowest price
                "close": "FLOAT",  # Float column for closing price
                "volume": "INT",  # Integer column for volume
            },
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# With each run, it fetches the last 30 days of stock price data regardless of previous runs.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync (not used in this implementation).
def update(configuration: dict, state: dict):
    # Get configuration values
    api_key = configuration.get("api_key")
    symbol = configuration.get("symbol", "AAPL")  # Default to AAPL if not specified
    days = configuration.get("days", 30)  # Number of days to fetch, default 30
    
    # Calculate date range for the last specified number of days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    # Format dates as strings for the API
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    # Construct the API URL for historical price data
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    params = {
        "from": start_date_str,
        "to": end_date_str,
        "apikey": api_key
    }
    
    # Make the API request
    response = rq.get(url, params=params)
    
    # Check if the request was successful
    if response.status_code != 200:
        return
    
    # Parse the JSON response to get the historical price data
    data = response.json()
    
    # If the response doesn't contain the expected structure, return
    if "historical" not in data:
        return
    
    # Get the historical price data
    historical_data = data["historical"]
    
    # Process each price record
    for price_data in historical_data:
        date_str = price_data.get("date")
        
        # Skip if date is missing
        if not date_str:
            continue
        
        # Yield an upsert operation to insert/update the row in the "stock_prices" table
        yield op.upsert(
            table="stock_prices",
            data={
                "symbol": symbol,
                "date": date_str,
                "open": price_data.get("open"),
                "high": price_data.get("high"),
                "low": price_data.get("low"),
                "close": price_data.get("close"),
                "volume": price_data.get("volume")
            }
        )
    
    # Since we're not tracking state between runs, we yield an empty checkpoint
    # This is still required by Fivetran's connector framework
    yield op.checkpoint(state={})


# This creates the connector object that will use the update and schema functions defined in this connector.py file
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
# This is useful for debugging while you write your code
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE
    connector.debug()

# Expected resulting table:
# ┌─────────┬────────────┬───────┬───────┬──────┬───────┬──────────┐
# │ symbol  │    date    │ open  │ high  │ low  │ close │  volume  │
# │ varchar │    date    │ float │ float │ float│ float │   int    │
# ├─────────┼────────────┼───────┼───────┼──────┼───────┼──────────┤
# │ AAPL    │ 2024-08-01 │ 212.3 │ 215.7 │ 210.1│ 214.8 │ 64295100 │
# │ AAPL    │ 2024-08-02 │ 215.0 │ 217.2 │ 214.1│ 216.9 │ 58342600 │
# │ AAPL    │ 2024-08-05 │ 216.2 │ 218.3 │ 215.5│ 217.4 │ 52148700 │
# │ AAPL    │ 2024-08-06 │ 216.8 │ 219.5 │ 216.4│ 218.7 │ 48651200 │
# │ AAPL    │ 2024-08-07 │ 219.1 │ 221.8 │ 218.6│ 221.5 │ 62487300 │
# │ AAPL    │ 2024-08-08 │ 220.9 │ 223.2 │ 220.0│ 222.8 │ 55784900 │
# │ AAPL    │ 2024-08-09 │ 222.3 │ 224.1 │ 221.5│ 223.6 │ 48259700 │
# │ AAPL    │ 2024-08-12 │ 223.0 │ 225.4 │ 222.4│ 225.1 │ 51326800 │
# │ AAPL    │ 2024-08-13 │ 224.5 │ 226.3 │ 223.7│ 225.9 │ 47895200 │
# │ AAPL    │ 2024-08-14 │ 225.3 │ 227.5 │ 224.8│ 227.0 │ 53467100 │
# ├─────────┴────────────┴───────┴───────┴──────┴───────┴──────────┤
# │ 10 rows                                             7 columns  │
# └───────────────────────────────────────────────────────────────┘