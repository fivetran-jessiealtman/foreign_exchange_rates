# This connector fetches foreign exchange rates data from Exchange Rate API
# and syncs it to a Fivetran destination using the fivetran_connector_sdk.
# This example includes a dummy API key for demonstration purposes.
# Replace with your actual API key from https://app.exchangerate-api.com

from datetime import datetime, timedelta, timezone

import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def schema(configuration: dict):
    return [
        {
            "table": "daily_exchange_rates",
            "primary_key": ["base_currency", "date","target_currency"],
            "columns": {
                "base_currency": "STRING",
                "date": "NAIVE_DATE",
                "target_currency": "STRING",
                "rate": "FLOAT",
            },
        }
    ]


def update(configuration: dict, state: dict):
    log.info("Starting Daily Foreign Exchange Rate Connector sync")
    
    # Use API key from configuration or fall back to dummy key
    # IMPORTANT: Replace this dummy key with your actual API key in production
    api_key = configuration.get("api_key", "1eb7446284c4bbc366a33cbc")
    
    # Get base currencies from configuration or use default
    base_currencies = [configuration.get("base_currencies", "USD")]
    target_currencies = configuration.get("target_currencies", "USD,EUR,GBP,JPY").split(",")
    
    # Retrieve the cursor from the state to determine the last sync date
    last_sync = state.get('last_sync', '0001-01-01')
    
    # Make sure we're working with timezone-aware datetime objects
    try:
        # Parse the last sync date
        last_sync_dt = datetime.strptime(last_sync, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        # Fallback if the format is unexpected
        last_sync_dt = datetime(2001, 1, 1, tzinfo=timezone.utc)
    
    # Determine the year to fetch (current year or specified year)
    current_year = datetime.now(timezone.utc).year
    target_year = configuration.get("target_year", current_year)
    
    # Calculate the date range for the last 30 days
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=2)
    
    current_date = start_date
    
    log.info(f"Fetching daily exchange rates for base currencies: {base_currencies}")
    log.info(f"Target Currencies: {target_currencies}")
    log.info(f"Fetching rates for year: {target_year}")
    
    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y-%m-%d")
        
        for base in base_currencies:
            try:
                # Fetch exchange rates from the API
                url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base}"
                log.fine(f"Requesting data from: {url}")
                
                response = rq.get(url)
                response.raise_for_status()  # Raise exception for HTTP errors
                
                data = response.json()
                
                if data["result"] != "success":
                    log.error(f"API returned error: {data.get('error', 'Unknown error')}")
                    continue
                
                # Extract rates and last update time
                rates = data["conversion_rates"]
                
                # Ensure last_updated is timezone aware
                last_updated_timestamp = data["time_last_update_unix"]
                last_updated = datetime.fromtimestamp(last_updated_timestamp, tz=timezone.utc)
                last_updated_str = last_updated.strftime("%Y-%m-%dT%H:%M:%S") + 'Z'
                
                log.info(f"Got {len(rates)} rates for {base}")
                
                # Process each currency pair
                for target_currency, rate in rates.items():
                    yield op.upsert(
                        table="daily_exchange_rates",
                        data={
                            "base_currency": base,
                            "date": current_date_str,
                            "target_currency": target_currency,
                            "rate": rate,
                            "last_updated": last_updated_str
                        }
                    )
                    
            except Exception as e:
                log.error(f"Error fetching rates for {base}: {str(e)}")
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Update the checkpoint with the last date processed
    yield op.checkpoint(state={"last_sync": end_date.strftime("%Y-%m-%d")})

# Create the connector
connector = Connector(update=update, schema=schema)

# For local debugging
if __name__ == "__main__":
    connector.debug()

# Example resulting table:
# ┌───────────────┬─────────────────────────┬─────────────────┬──────────┬─────────────────────────┐
# │ base_currency │        timestamp        │ target_currency │   rate   │      last_updated       │
# │    varchar    │ timestamp with time zone│     varchar     │  float   │ timestamp with time zone│
# ├───────────────┼─────────────────────────┼─────────────────┼──────────┼─────────────────────────┤
# │ USD           │ 2025-03-12 10:15:00+00  │ USD             │ 1.0      │ 2025-03-12 00:00:00+00  │
# │ USD           │ 2025-03-12 10:15:00+00  │ EUR             │ 0.92     │ 2025-03-12 00:00:00+00  │
# │ USD           │ 2025-03-12 10:15:00+00  │ GBP             │ 0.78     │ 2025-03-12 00:00:00+00  │
# │ USD           │ 2025-03-12 10:15:00+00  │ JPY             │ 147.82   │ 2025-03-12 00:00:00+00  │
# │ EUR           │ 2025-03-12 10:15:00+00  │ USD             │ 1.09     │ 2025-03-12 00:00:00+00  │
# │ EUR           │ 2025-03-12 10:15:00+00  │ EUR             │ 1.0      │ 2025-03-12 00:00:00+00  │
# │ EUR           │ 2025-03-12 10:15:00+00  │ GBP             │ 0.85     │ 2025-03-12 00:00:00+00  │
# │ EUR           │ 2025-03-12 10:15:00+00  │ JPY             │ 160.67   │ 2025-03-12 00:00:00+00  │
# └───────────────┴─────────────────────────┴─────────────────┴──────────┴─────────────────────────┘
