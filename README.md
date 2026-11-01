# Delta Signal Desk

A localhost market scanner built with FastAPI and React. It reads public Delta Exchange OHLC candles and calculates session VWAP, RSI, and MACD locally. Optional live futures execution can be armed after a verified account connection, while the scanner is running, and after explicit risk acknowledgement and confirmation.

## Run locally

1. Python 3.10+ recommended. In a terminal at this folder, create a virtual environment, install `backend/requirements.txt`, then start the API:

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r backend/requirements.txt
   python -m uvicorn main:app --app-dir backend --host 127.0.0.1 --port 8000
   ```

2. In a second terminal:

   ```powershell
   cd frontend
   npm install
   npm run dev
   ```

3. Open the local address printed by Vite (normally `http://127.0.0.1:5173`). Choose a market environment, adjust indicators, and start the scan. The scanner uses the selected environment for public candle requests. The default watchlist is a starting point; add or remove symbols to suit the selected Delta market.

## Credentials and connection state

Credentials are optional for scanning and are held only by the in-memory Tradehull client in the backend. They are never written to disk or sent to another service. Stop or disconnect the API session to clear them. Connecting verifies authenticated access to the account's positions. The market scanner itself uses public market data and does not require API credentials. Never paste credentials into a remotely hosted frontend.

## Signals

Every completed candle is checked against session VWAP; a fresh VWAP crossover is not required. Buy requires the candle close above VWAP, RSI from 50 through 75 inclusive, and MACD above its signal line (bullish). Sell requires the close below VWAP, RSI from 25 through 40 inclusive, and MACD below its signal line (bearish). All other readings are Neutral. A signal can persist across consecutive completed candles while its conditions remain true. Each pass fetches the configured candle history for all watchlist symbols concurrently and begins another pass immediately after completion. Data availability, network conditions, and Delta rate limits affect refresh latency. Signal labels are informational and do not guarantee performance. Crypto derivatives carry substantial risk.

When connected, the Open Positions panel refreshes account positions and supports confirmed reduce-only market closes for one market or all open positions. The scanner shows recent order acceptance, rejection, skip, and close outcomes with the returned reason where available. These account controls require the updated local API and trading-enabled Delta credentials.

## Execution safety

Automatic futures execution is off by default. It requires a verified trading connection, a running scanner, risk acknowledgement, and a separate arm confirmation. While armed, each new completed-candle Buy/Sell signal can place one market bracket order per symbol and candle, using the configured contract quantity, leverage, and percentage stop/target. An existing position for that symbol blocks another entry. Stopping the scanner or using close-all disarms execution. Delta may reject leverage above a product's current limit. Market reference prices can differ from the eventual fill. The close-all action calls Delta's positions-only close endpoint and leaves open orders in place. Successful orders are logged and trigger an audible browser chime. Live trading can lose money rapidly; use testnet and minimum size while learning.

## References

- [Delta Exchange API documentation](https://docs.delta.exchange/)
- [Delta Exchange Python REST client](https://github.com/delta-exchange/python-rest-client/blob/master/README.md)
- [Tradehull Delta Exchange package on PyPI](https://pypi.org/project/Tradehull-Delta-Exchange/)
