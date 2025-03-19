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
            "table": "exchange_rates",
            "primary_key": ["base_currency", "timestamp"],
            "columns": {
                "base_currency": "STRING",
                "timestamp": "UTC_DATETIME",
                "target_currency": "STRING",
                "rate": "FLOAT",
                "last_updated": "UTC_DATETIME",
            },
        }
    ]


def update(configuration: dict, state: dict):
    log.info("Starting Foreign Exchange Rate Connector sync")
    
    # Use API key from configuration or fall back to dummy key
    # IMPORTANT: Replace this dummy key with your actual API key in production
    api_key = configuration.get("api_key", "3712acaa9e39ab4d7dab0ab7")
    
    # Get base currencies from configuration or use default
    base_currencies = configuration.get("base_currencies", "USD,EUR,GBP,JPY").split(",")
    
    # Retrieve the cursor from the state to determine the last sync time
    last_sync = state.get('last_sync', '0001-01-01T00:00:00Z')
    
    # Make sure we're working with timezone-aware datetime objects
    try:
        # Parse ISO format with timezone info
        last_sync_dt = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
    except ValueError:
        # Fallback if the format is unexpected
        last_sync_dt = datetime(2001, 1, 1, tzinfo=timezone.utc)
    
    # Current time to use as new checkpoint (with timezone info)
    current_time = datetime.now(timezone.utc).replace(microsecond=0)
    current_time_str = current_time.strftime("%Y-%m-%dT%H:%M:%S") + 'Z'
    
    # Only sync if more than 1 hour has passed since last sync
    # (to avoid unnecessary API calls and respect rate limits)
    if current_time - last_sync_dt < timedelta(hours=1) and last_sync != '0001-01-01T00:00:00Z':
        log.info(f"Skipping sync, last sync was less than 1 hour ago: {last_sync}")
        yield op.checkpoint(state={"last_sync": last_sync})
        return
    
    log.info(f"Fetching exchange rates for base currencies: {base_currencies}")
    
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
                    table="exchange_rates",
                    data={
                        "base_currency": base,
                        "timestamp": current_time_str,
                        "target_currency": target_currency,
                        "rate": rate,
                        "last_updated": last_updated_str
                    }
                )
                
        except Exception as e:
            log.error(f"Error fetching rates for {base}: {str(e)}")
    
    # Update the checkpoint with the current time
    yield op.checkpoint(state={"last_sync": current_time_str})


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
