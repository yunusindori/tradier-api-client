# tradier_api_client

Python client for the Tradier Brokerage API (REST) plus optional streaming helpers.

This repo is intended to give you a lightweight, script-friendly way to call Tradier endpoints (quotes, options, orders, account data) using a single `RestClient`.

> Note: Some examples below are adapted from `tests/real_data.py` which is intended for *local* use with a real API key. Don’t commit keys.

---

## Installation

From source (editable):

```bash
python -m pip install -U pip
python -m pip install -e .
```

Regular install (from the cloned repo):

```bash
python -m pip install .
```

---

## Authentication

Set your Tradier API key in an environment variable.

### Windows (cmd.exe)

```bat
set API_KEY=your_key_here
```

### macOS/Linux

```bash
export API_KEY=your_key_here
```

---

## Quick start

### Create a `RestClient`

The tests create a client like this:

```python
import os
from tradier_api_client import RestClient

client = RestClient(
    base_url="https://sandbox.tradier.com/v1",  # use https://api.tradier.com/v1 for production
    api_key=os.environ.get("API_KEY"),
    verbose=True,
)

print("Account:", client.account_number)
```

---

## Common REST examples

### 1) Quotes

```python
quotes = client.get_quotes(["AAPL", "MSFT"])
print(quotes)
```

### 2) Historical quotes

```python
history = client.get_historical_quotes("AAPL", start="1979-01-01")
print(history)
```

### 3) Option expirations + option chains

```python
expirations_resp = client.get_option_expirations("AAPL")
expirations = expirations_resp.get("expirations", {}).get("date", [])
print(expirations)

if expirations:
    chain = client.get_option_chains("AAPL", expirations[0])
    print(chain)
```

### 4) Options helper workflow (OCC symbols)

The repo includes an `OptionsWrapper` and helper utilities used by `tests/real_data.py`.

```python
from tradier_api_client.rest.extensions.options import (
    OptionsWrapper,
    parse_occ,
    filter_options_occ,
    filter_for_tradable_options_strike_plus_bid,
)

ow = OptionsWrapper(client)
occ_symbols = ow.get_call_options_occ_symbols_list("AAPL")

# Parse OCC symbols into structured info
parsed = parse_occ(occ_symbols)

# Filter by expiration window and strike constraints (example)
filtered_occ = filter_options_occ(options=occ_symbols, exp_end="2027-08-30", exp_start="2027-01-01")

# Quote options and filter for tradable candidates
quotes = client.get_quotes(filtered_occ)
tradable = filter_for_tradable_options_strike_plus_bid(
    quotes=quotes,
    occ_symbols=filtered_occ,
    target_price=5.5,
)
print(tradable)
```

### 5) Account endpoints (example: gain/loss)

```python
gain_loss = client.get_gain_loss(client.account_number)
print(gain_loss)
```

---

## Orders

The order helpers live under `tradier_api_client.rest.extensions.orders`.

### Equity limit buy (example)

```python
from tradier_api_client.rest.extensions.orders import equity_limit

order_request = equity_limit(
    symbol="AAPL",
    side="buy",
    quantity=1,
    price=150,
    duration="gtc",
)

resp = client.place_order(client.account_number, order_request)
print(resp)
```

### Read / cancel orders

```python
orders = client.get_orders(client.account_number)
print(orders)

# If you have an order id:
# order = client.get_order(client.account_number, "12345678")
# print(order)

# cancel_resp = client.cancel_order(client.account_number, "12345678")
# print(cancel_resp)
```

---

## Running tests (local repo)

This project includes tests/integration scripts under `tests/`.

- Some scripts (like `tests/real_data.py`) may call live/sandbox endpoints and require `API_KEY`.
- The `tests/` folder is **not** intended to be installed with the client distribution.

To run tests locally:

```bash
python -m pip install -U pytest
pytest -q
```

---

## Notes / troubleshooting

- **Sandbox vs production**: use `https://sandbox.tradier.com/v1` for paper testing and `https://api.tradier.com/v1` for production.
- If you see authentication errors, confirm `API_KEY` is set in the environment used by your Python interpreter.
