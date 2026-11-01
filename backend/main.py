from __future__ import annotations

import asyncio
import aiohttp
import socket
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from email.utils import parsedate_to_datetime
from types import MethodType
from typing import Any

import httpx
from Tradehull_Delta_Exchange import Tradehull
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

BASE_URLS = {
    "india": "https://api.india.delta.exchange",
    "global": "https://api.delta.exchange",
    "testnet-india": "https://cdn-ind.testnet.deltaex.org",
    "testnet-global": "https://testnet-api.delta.exchange",
}
RESOLUTIONS = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "1d", "1w"}
credentials: dict[str, str] | None = None
trade_client: Tradehull | None = None
trade_lock = threading.Lock()
order_submission_lock = asyncio.Lock()

class MarginPreflightError(RuntimeError):
    pass
activity_logs: list[dict[str, str]] = []
order_events: list[dict[str, Any]] = []
scan_task: asyncio.Task | None = None
scan_config: dict[str, Any] = {}
auto_execution: dict[str, Any] = {"armed": False, "quantity": 1, "leverage": 10, "stop_loss_pct": 1.0, "target_pct": 2.0}
last_auto_candle: dict[str, int] = {}
auto_inflight_symbols: set[str] = set()
auto_tasks: set[asyncio.Task] = set()
latest: dict[str, Any] = {"running": False, "rows": [], "timestamp": None, "error": None}


class Credentials(BaseModel):
    key: str = Field(min_length=1, max_length=256)
    secret: str = Field(min_length=1, max_length=512)
    environment: str = "india"


class Settings(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=40)
    rsi_period: int = Field(default=14, ge=2, le=100)
    macd_fast: int = Field(default=12, ge=2, le=100)
    macd_slow: int = Field(default=26, ge=3, le=200)
    macd_signal: int = Field(default=9, ge=2, le=50)
    timeframe: str = "5m"


class ScanRequest(Settings):
    pass


class AutoExecutionRequest(BaseModel):
    quantity: int = Field(ge=1, le=1_000_000)
    stop_loss_pct: float = Field(gt=0, le=99.99)
    target_pct: float = Field(gt=0, le=99.99)
    leverage: int = Field(ge=1, le=100)
    confirmed: bool = False


class ConfirmAction(BaseModel):
    confirmed: bool = False


class PositionAction(BaseModel):
    symbol: str = Field(min_length=1, max_length=40)
    confirmed: bool = False


def add_log(message: str, level: str = "info") -> None:
    activity_logs.insert(0, {"time": datetime.now(timezone.utc).isoformat(), "message": message, "level": level})
    del activity_logs[100:]


def add_order_event(symbol: str, side: str, status: str, reason: str = "", order_id: Any = None) -> None:
    order_events.insert(0, {
        "time": datetime.now(timezone.utc).isoformat(), "symbol": symbol, "side": side.upper(),
        "status": status, "reason": reason, "order_id": str(order_id) if order_id is not None else None,
    })
    del order_events[50:]


async def delta_server_clock_offset(base_url: str) -> float:
    """Get Delta's current HTTP Date over verified TLS for accurate signed requests."""
    async with httpx.AsyncClient(transport=delta_http_transport(), timeout=8) as client:
        response = await client.get(f"{base_url}/v2/products/ETHUSD")
        response.raise_for_status()
        date_header = response.headers.get("date")
    if not date_header:
        raise RuntimeError("Delta did not return a server Date header; check Windows date and time synchronization.")
    server_epoch = parsedate_to_datetime(date_header).timestamp()
    offset = server_epoch - time.time()
    if abs(offset) > 7 * 24 * 60 * 60:
        raise RuntimeError("The time reported by Delta differs by over seven days; check the computer clock.")
    return offset


def use_ipv4_for_tradehull(client: Tradehull, server_clock_offset: float) -> None:
    """Use IPv4 and Delta's verified server time for Tradehull signed requests."""
    api = client._api
    original_auth_headers = api._auth_headers

    def delta_clock_auth_headers(self, method, path, query_string, payload):
        headers = original_auth_headers(method, path, query_string, payload)
        timestamp = str(int(time.time() + server_clock_offset))
        signature_data = method.upper() + timestamp + path + query_string + payload
        signature = self._generate_signature(self.api_secret, signature_data)
        headers.update({"timestamp": timestamp, "signature": signature})
        return headers

    api._auth_headers = MethodType(delta_clock_auth_headers, api)

    async def ensure_ipv4_session(self):
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(family=socket.AF_INET)
            self._session = aiohttp.ClientSession(timeout=self.timeout, connector=connector)
        return self._session

    api._ensure_session = MethodType(ensure_ipv4_session, api)


def delta_http_transport() -> httpx.AsyncHTTPTransport:
    """Bind HTTPX requests to IPv4 for networks that cannot route IPv6."""
    return httpx.AsyncHTTPTransport(local_address="0.0.0.0", retries=1)


async def run_trade_call(function, *args, **kwargs):
    def invoke():
        with trade_lock:
            return function(*args, **kwargs)
    return await asyncio.to_thread(invoke)


def _rsi(values: list[float], period: int) -> float:
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    if len(changes) < period:
        return 50.0
    sample = changes[:period]
    gains = sum(max(v, 0) for v in sample) / period
    losses = sum(max(-v, 0) for v in sample) / period
    for change in changes[period:]:
        gains = (gains * (period - 1) + max(change, 0)) / period
        losses = (losses * (period - 1) + max(-change, 0)) / period
    if losses == 0:
        return 100.0 if gains else 50.0
    return 100 - 100 / (1 + gains / losses)


def _ema_series(values: list[float], period: int) -> list[float]:
    alpha = 2 / (period + 1)
    result = values[0]
    series = [result]
    for value in values[1:]:
        result = alpha * value + (1 - alpha) * result
        series.append(result)
    return series


def _adx(candles: list[dict[str, Any]], period: int = 14) -> float:
    """Return Wilder's 14-period Average Directional Index."""
    true_ranges: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for index in range(1, len(candles)):
        high = float(candles[index]["high"])
        low = float(candles[index]["low"])
        previous_high = float(candles[index - 1]["high"])
        previous_low = float(candles[index - 1]["low"])
        previous_close = float(candles[index - 1]["close"])
        up_move = high - previous_high
        down_move = previous_low - low
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)

    if len(true_ranges) < 2 * period - 1:
        return 0.0

    smooth_tr = sum(true_ranges[:period])
    smooth_plus = sum(plus_dm[:period])
    smooth_minus = sum(minus_dm[:period])
    dx_values: list[float] = []
    for index in range(period - 1, len(true_ranges)):
        if index >= period:
            smooth_tr += true_ranges[index] - smooth_tr / period
            smooth_plus += plus_dm[index] - smooth_plus / period
            smooth_minus += minus_dm[index] - smooth_minus / period
        plus_di = 100 * smooth_plus / smooth_tr if smooth_tr else 0.0
        minus_di = 100 * smooth_minus / smooth_tr if smooth_tr else 0.0
        di_sum = plus_di + minus_di
        dx_values.append(100 * abs(plus_di - minus_di) / di_sum if di_sum else 0.0)

    adx = sum(dx_values[:period]) / period
    for dx in dx_values[period:]:
        adx = (adx * (period - 1) + dx) / period
    return adx


def classify(symbol: str, candles: list[dict[str, Any]], settings: Settings) -> dict[str, Any]:
    candles.sort(key=lambda candle: int(candle["time"]))
    closes = [float(candle["close"]) for candle in candles]
    current = candles[-1]
    previous = candles[-2]
    open_price, close = float(current["open"]), closes[-1]
    rsi = _rsi(closes, settings.rsi_period)
    candle = "Green" if close > open_price else "Red" if close < open_price else "Flat"
    macd_fast = _ema_series(closes, settings.macd_fast)
    macd_slow = _ema_series(closes, settings.macd_slow)
    macd_line = [fast - slow for fast, slow in zip(macd_fast, macd_slow)]
    macd_signal = _ema_series(macd_line, settings.macd_signal)
    macd_value, signal_value = macd_line[-1], macd_signal[-1]
    macd_previous, signal_previous = macd_line[-2], macd_signal[-2]
    macd_cross = "Bullish" if macd_previous <= signal_previous and macd_value > signal_value else "Bearish" if macd_previous >= signal_previous and macd_value < signal_value else "None"
    macd_state = "Bullish" if macd_value > signal_value else "Bearish" if macd_value < signal_value else "Neutral"

    # Session VWAP uses UTC-day candles and volume-weighted typical prices.
    session_date = datetime.fromtimestamp(int(current["time"]), timezone.utc).date()
    session_candles = [item for item in candles if datetime.fromtimestamp(int(item["time"]), timezone.utc).date() == session_date]
    weighted_volume = sum(((float(item["high"]) + float(item["low"]) + float(item["close"])) / 3) * float(item.get("volume", 0)) for item in session_candles)
    total_volume = sum(float(item.get("volume", 0)) for item in session_candles)
    vwap = weighted_volume / total_volume if total_volume else close
    previous_date = datetime.fromtimestamp(int(previous["time"]), timezone.utc).date()
    previous_session = [item for item in candles[:-1] if datetime.fromtimestamp(int(item["time"]), timezone.utc).date() == previous_date]
    prev_volume = sum(float(item.get("volume", 0)) for item in previous_session)
    prev_vwap = (sum(((float(item["high"]) + float(item["low"]) + float(item["close"])) / 3) * float(item.get("volume", 0)) for item in previous_session) / prev_volume) if prev_volume else float(previous["close"])
    vwap_cross = "Above" if float(previous["close"]) <= prev_vwap and close > vwap else "Below" if float(previous["close"]) >= prev_vwap and close < vwap else "None"
    # Evaluate every completed candle by its VWAP position, not only on a fresh cross.
    vwap_position = "Above" if close > vwap else "Below" if close < vwap else "At"

    adx = _adx(candles)
    if vwap_position == "Above" and 50 <= rsi <= 70 and macd_state == "Bullish" and adx >= 25:
        signal = "Buy"
    elif vwap_position == "Below" and 30 <= rsi <= 40 and macd_state == "Bearish" and adx >= 25:
        signal = "Sell"
    else:
        signal = "Neutral"
    return {"symbol": symbol, "candle_time": int(current["time"]), "close": close, "open": open_price, "rsi": round(rsi, 2), "vwap": round(vwap, 8),
            "vwap_cross": vwap_cross, "vwap_position": vwap_position, "macd": round(macd_value, 8), "macd_signal": round(signal_value, 8),
            "macd_cross": macd_cross, "macd_state": macd_state, "adx": round(adx, 2), "candle": candle, "momentum": round(macd_value - signal_value, 8),
            "trend": "Up" if macd_value > signal_value else "Down" if macd_value < signal_value else "Flat", "signal": signal}


async def fetch_candles(client: httpx.AsyncClient, base_url: str, symbol: str, settings: Settings) -> dict[str, Any]:
    now = int(time.time())
    seconds = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
               "2h": 7200, "4h": 14400, "6h": 21600, "1d": 86400, "1w": 604800}[settings.timeframe]
    count = max(settings.macd_slow + settings.macd_signal * 3, settings.rsi_period) + 12
    response = await client.get(f"{base_url}/v2/history/candles", params={"resolution": settings.timeframe,
        "symbol": symbol, "start": now - count * seconds, "end": now}, timeout=12)
    response.raise_for_status()
    payload = response.json()
    candles = payload.get("result", [])
    # Only use completed candles; evaluate their VWAP position at candle close.
    candles = [candle for candle in candles if int(candle["time"]) + seconds <= now]
    if len(candles) < max(settings.macd_slow + settings.macd_signal, settings.rsi_period) + 2:
        raise ValueError("Not enough candle history")
    return classify(symbol, candles, settings)


async def scan_loop() -> None:
    global latest
    env = scan_config.get("environment", "india")
    base_url = BASE_URLS.get(env, BASE_URLS["india"])
    async with httpx.AsyncClient(transport=delta_http_transport(), limits=httpx.Limits(max_connections=50, max_keepalive_connections=20)) as client:
        while True:
            started = time.monotonic()
            settings = Settings(**scan_config["settings"])
            results = await asyncio.gather(*(fetch_candles(client, base_url, symbol, settings) for symbol in settings.symbols), return_exceptions=True)
            rows = []
            errors = []
            for symbol, result in zip(settings.symbols, results):
                if isinstance(result, Exception):
                    errors.append(f"{symbol}: {str(result)[:120]}")
                else:
                    rows.append(result)
            latest = {"running": True, "rows": rows, "timestamp": datetime.now(timezone.utc).isoformat(), "error": "; ".join(errors) or None}
            if rows and auto_execution["armed"]:
                task = asyncio.create_task(process_auto_signals(rows))
                auto_tasks.add(task)
                task.add_done_callback(auto_tasks.discard)
            # Immediate next pass after this batch completes; no user-facing polling interval.
            await asyncio.sleep(max(0.05, 0.35 - (time.monotonic() - started)))


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    global scan_task
    auto_execution["armed"] = False
    if scan_task:
        scan_task.cancel()
    for task in tuple(auto_tasks):
        task.cancel()


app = FastAPI(title="Delta Signal Scanner", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/status")
async def status():
    return {**latest, "connected": trade_client is not None, "execution": dict(auto_execution), "logs": activity_logs, "order_events": order_events, "position_tools_ready": True}


@app.post("/api/execution/arm")
async def arm_auto_execution(body: AutoExecutionRequest):
    if not body.confirmed:
        raise HTTPException(400, "Acknowledge and confirm automated order execution first.")
    if trade_client is None:
        raise HTTPException(401, "Connect a Delta account with trading permission first.")
    if not latest["running"]:
        raise HTTPException(409, "Start the market scanner before arming auto execution.")
    auto_execution.update({
        "armed": True,
        "quantity": body.quantity,
        "leverage": body.leverage,
        "stop_loss_pct": body.stop_loss_pct,
        "target_pct": body.target_pct,
    })
    # Do not trade signals that were already visible before the explicit arm action.
    for row in latest.get("rows", []):
        if row.get("symbol") and row.get("candle_time"):
            last_auto_candle[row["symbol"]] = int(row["candle_time"])
    add_log(f"Auto execution armed for new candle signals only · {body.quantity} contract(s) · {body.leverage}x · SL {body.stop_loss_pct:g}% · target {body.target_pct:g}%.", "warning")
    return {"armed": True, "execution": dict(auto_execution)}


@app.post("/api/execution/disarm")
async def disarm_auto_execution():
    auto_execution["armed"] = False
    add_log("Auto execution disarmed.")
    return {"armed": False}


@app.post("/api/connect")
async def connect(body: Credentials):
    global credentials, trade_client
    if body.environment not in BASE_URLS:
        raise HTTPException(400, "Unknown Delta environment")
    # Pasted credentials can accidentally include whitespace, which changes
    # the HMAC signature and makes a valid key/secret pair look invalid.
    api_key = body.key.strip()
    api_secret = body.secret.strip()
    if not api_key or not api_secret:
        raise HTTPException(400, "Enter both the API key and API secret without surrounding spaces")
    # Keep the authenticated client in process memory and verify private read access.
    client = None
    try:
        server_clock_offset = await delta_server_clock_offset(BASE_URLS[body.environment])
        client = Tradehull(api_key=api_key, api_secret=api_secret, base_url=BASE_URLS[body.environment])
        use_ipv4_for_tradehull(client, server_clock_offset)
        positions = await asyncio.to_thread(client.get_open_positions)
    except Exception as exc:
        if client:
            await asyncio.to_thread(client.close)
        raise HTTPException(400, f"Could not connect to Delta: {exc}") from exc
    previous = trade_client
    trade_client = client
    credentials = {"environment": body.environment}
    auto_execution["armed"] = False
    last_auto_candle.clear()
    activity_logs.clear()
    if previous:
        await asyncio.to_thread(previous.close)
    add_log(f"Connected to Delta {body.environment}; {len(positions)} open position(s) visible.")
    return {"connected": True, "environment": body.environment, "open_positions": len(positions)}


@app.post("/api/disconnect")
async def disconnect():
    global credentials, trade_client
    auto_execution["armed"] = False
    client = trade_client
    trade_client = None
    credentials = None
    if client:
        await asyncio.to_thread(client.close)
    add_log("Delta session disconnected.")
    return {"connected": False}


async def _preflight_order_margin(symbol: str, quantity: int, reference_price: float, leverage: int, product: dict[str, Any]) -> None:
    """Conservatively block an order when the settlement wallet cannot cover estimated margin and fees."""
    if trade_client is None:
        raise RuntimeError("Delta account disconnected before the margin check.")
    wallet = await run_trade_call(trade_client.get_wallet_balance)
    balances = wallet.get("balances", []) if isinstance(wallet, dict) else []
    settlement_asset = (product.get("settling_asset") or product.get("quoting_asset") or {}).get("symbol")
    if not settlement_asset:
        raise MarginPreflightError(f"Pre-trade margin check could not identify {symbol}'s settlement asset; order not sent.")
    balance_row = next((row for row in balances if str(row.get("asset_symbol", "")).upper() == str(settlement_asset).upper()), None)
    if not balance_row or balance_row.get("available_balance") is None:
        raise MarginPreflightError(f"Pre-trade margin check could not read available {settlement_asset} balance; order not sent.")

    available = Decimal(str(balance_row["available_balance"]))
    contract_value = Decimal(str(product.get("contract_value") or "0"))
    price = Decimal(str(reference_price))
    if contract_value <= 0 or price <= 0 or leverage <= 0:
        raise MarginPreflightError(f"Pre-trade margin check could not estimate {symbol}'s order value; order not sent.")
    contract_unit = str(product.get("contract_unit_currency") or "").upper()
    if contract_unit and contract_unit == str(settlement_asset).upper():
        notional = Decimal(quantity) * contract_value
    else:
        notional = Decimal(quantity) * contract_value * price
    # Account for a small market-price move, margin scaling, and both entry/exit taker fees.
    notional *= Decimal("1.02")
    leverage_margin_rate = Decimal("1") / Decimal(leverage)
    base_margin_rate = Decimal(str(product.get("initial_margin") or "0")) / Decimal("100")
    scaling_factor = Decimal(str(product.get("initial_margin_scaling_factor") or "0"))
    margin_rate = max(leverage_margin_rate, base_margin_rate) + scaling_factor * notional
    taker_fee_rate = Decimal(str(product.get("taker_commission_rate") or "0"))
    estimated_fees = notional * taker_fee_rate * 2
    estimated_required = (notional * margin_rate + estimated_fees) * Decimal("1.10")
    if available < estimated_required:
        raise MarginPreflightError(
            f"Pre-trade margin check blocked {symbol}: available {available:.4f} {settlement_asset}, estimated requirement {estimated_required:.4f} {settlement_asset} including fees and safety buffer. Order not sent."
        )

async def _submit_signal_order(symbol: str, side: str, reference_price: float, config: dict[str, Any]) -> dict[str, Any]:
    if trade_client is None:
        raise RuntimeError("Delta account disconnected before order placement.")
    environment = (credentials or {}).get("environment", "india")
    stop_loss_pct = float(config["stop_loss_pct"])
    target_pct = float(config["target_pct"])
    leverage = int(config["leverage"])
    quantity = int(config["quantity"])
    if side == "buy":
        stop_loss = reference_price * (1 - stop_loss_pct / 100)
        target = reference_price * (1 + target_pct / 100)
    else:
        stop_loss = reference_price * (1 + stop_loss_pct / 100)
        target = reference_price * (1 - target_pct / 100)
    async with httpx.AsyncClient(transport=delta_http_transport()) as client:
        response = await client.get(f"{BASE_URLS[environment]}/v2/products/{symbol}", timeout=10)
        response.raise_for_status()
        product = response.json().get("result", {})
    product_type = product.get("product_type") or product.get("contract_type")
    if product_type not in {"futures", "perpetual_futures"}:
        raise ValueError(f"{symbol} is not a futures contract.")
    if product.get("state") != "live" or product.get("trading_status", "operational") != "operational":
        raise ValueError(f"{symbol} is not live and operational.")
    product_id = product.get("id") or product.get("product_id")
    if product_id is None:
        raise ValueError(f"Delta returned no product ID for {symbol}.")
    tick_size = product.get("tick_size")
    if tick_size:
        tick = Decimal(str(tick_size))
        if tick > 0:
            stop_rounding, target_rounding = (ROUND_DOWN, ROUND_UP) if side == "buy" else (ROUND_UP, ROUND_DOWN)
            stop_loss = float((Decimal(str(stop_loss)) / tick).quantize(Decimal("1"), rounding=stop_rounding) * tick)
            target = float((Decimal(str(target)) / tick).quantize(Decimal("1"), rounding=target_rounding) * tick)
    if side == "buy" and not stop_loss < reference_price < target:
        raise ValueError("Stop/target percentages are too small for this contract's tick size.")
    if side == "sell" and not target < reference_price < stop_loss:
        raise ValueError("Stop/target percentages are too small for this contract's tick size.")
    if not auto_execution["armed"] or trade_client is None:
        raise RuntimeError("Auto execution was stopped before the order could be submitted.")
    await _preflight_order_margin(symbol, quantity, reference_price, leverage, product)
    # Delta applies order leverage per product; this may affect other open orders for it.
    await run_trade_call(trade_client._private_request, "POST", f"/v2/products/{product_id}/orders/leverage", {"leverage": leverage})
    if not auto_execution["armed"] or trade_client is None:
        raise RuntimeError("Auto execution was stopped before the order could be submitted.")
    # The installed Tradehull.place_bracket_order helper always sends
    # bracket_stop_loss_limit_price (defaulting it to the trigger price),
    # which creates a stop-limit and can leave the exit unfilled after a gap.
    # Delta treats an SL without a limit price as a stop-market order.
    payload = {
        "product_symbol": symbol,
        "size": quantity,
        "side": side,
        "order_type": "market_order",
        "time_in_force": "gtc",
        "mmp": "disabled",
        "post_only": False,
        "reduce_only": False,
        "bracket_stop_trigger_method": "last_traded_price",
        "client_order_id": f"bo{int(time.time() * 1000)}"[:32],
        "cancel_orders_accepted": False,
        "bracket_stop_loss_price": str(stop_loss),
        "bracket_take_profit_price": str(target),
        "bracket_take_profit_limit_price": str(target),
    }
    # Deliberately omit bracket_stop_loss_limit_price: Delta's market SL
    # executes at the best available price after the last-traded-price trigger.
    response = await run_trade_call(trade_client._private_request, "POST", "/v2/orders", payload)
    order_id, extracted_status = Tradehull._extract_order_summary(response)
    order_status = str(extracted_status or "accepted")
    result = {"status": order_status, "order_id": order_id, "entry": response, "bracket": response}
    if order_status.lower() in {"reject", "rejected", "failed", "error"}:
        reason = str((result.get("entry") or {}).get("error") or result.get("message") or order_status)
        add_log(f"Auto {side.upper()} rejected for {symbol}: {reason[:180]}", "error")
        raise RuntimeError(f"Delta rejected the order: {reason}")
    add_order_event(symbol, side, "Accepted", f"Bracket order {order_status}; SL {stop_loss:g}, target {target:g}.", order_id)
    add_log(f"Auto {side.upper()} bracket accepted: {quantity} {symbol} · {leverage}x · SL {stop_loss_pct:g}% ({stop_loss:g}) · target {target_pct:g}% ({target:g}) · ID {order_id or 'pending'} · {order_status}.", "success")
    return {"status": order_status, "order_id": order_id, "stop_loss": stop_loss, "target": target}


async def submit_signal_order(symbol: str, side: str, reference_price: float, config: dict[str, Any]) -> dict[str, Any]:
    # Serialize preflight plus order submission so simultaneous signals cannot spend the same balance.
    async with order_submission_lock:
        return await _submit_signal_order(symbol, side, reference_price, config)

async def process_auto_signals(rows: list[dict[str, Any]]) -> None:
    if not auto_execution["armed"] or trade_client is None:
        return
    for row in rows:
        side = {"Buy": "buy", "Sell": "sell"}.get(row.get("signal"))
        symbol = row.get("symbol")
        candle_time = int(row.get("candle_time") or 0)
        if not side or not symbol or not candle_time or last_auto_candle.get(symbol) == candle_time or symbol in auto_inflight_symbols:
            continue
        last_auto_candle[symbol] = candle_time
        auto_inflight_symbols.add(symbol)
        try:
            if not auto_execution["armed"] or trade_client is None:
                continue
            positions = await run_trade_call(trade_client.get_open_positions)
            existing = next((position for position in positions
                if str(position.get("product_symbol") or position.get("symbol") or "").upper() == symbol
                and float(position.get("size") or 0) != 0), None)
            if existing:
                reason = "An open position already exists for this market."
                add_order_event(symbol, side, "Skipped", reason)
                add_log(f"Auto {side.upper()} skipped for {symbol}: {reason}")
                continue
            if not auto_execution["armed"] or trade_client is None:
                continue
            await submit_signal_order(symbol, side, float(row["close"]), dict(auto_execution))
        except MarginPreflightError as exc:
            add_order_event(symbol, side, "Blocked", str(exc)[:240])
            add_log(f"Auto {side.upper()} blocked before submission for {symbol}: {str(exc)[:180]}", "warning")
        except Exception as exc:
            add_order_event(symbol, side, "Rejected", str(exc)[:240])
            add_log(f"Auto {side.upper()} failed for {symbol}: {str(exc)[:180]}", "error")
        finally:
            auto_inflight_symbols.discard(symbol)


@app.post("/api/positions/close-all")
async def close_all_positions(body: ConfirmAction):
    if not body.confirmed:
        raise HTTPException(400, "Confirm the close-all action first.")
    if trade_client is None:
        raise HTTPException(401, "Connect to Delta first.")
    auto_execution["armed"] = False
    add_log("Auto execution disarmed by close-all safety action.")
    try:
        positions = await run_trade_call(trade_client.get_open_positions)
        if not positions:
            add_log("Close-all requested; no open positions were found.")
            return {"success": True, "closed": 0, "skipped": []}
        user_id = positions[0].get("user_id")
        if user_id is None:
            raise ValueError("Delta did not return a user ID for the open positions.")
        # Close positions only and preserve all open orders, including brackets.
        result = await run_trade_call(
            trade_client._private_request,
            "POST",
            "/v2/positions/close_all",
            payload={"close_all_portfolio": True, "close_all_isolated": True, "user_id": int(user_id)},
        )
    except Exception as exc:
        add_log(f"Close-all positions failed: {str(exc)[:180]}", "error")
        raise HTTPException(502, f"Could not close positions: {str(exc)[:240]}") from exc
    skipped = (result.get("result") or {}).get("skipped_products", []) if isinstance(result, dict) else []
    skipped_by_symbol = {str(item.get("product_symbol") or item.get("symbol") or ""): item.get("reason", "Delta skipped this market.") for item in skipped if isinstance(item, dict)}
    for position in positions:
        symbol = str(position.get("product_symbol") or position.get("symbol") or "Unknown")
        if symbol in skipped_by_symbol:
            add_order_event(symbol, "close", "Skipped", str(skipped_by_symbol[symbol]))
        else:
            add_order_event(symbol, "close", "Close requested", "Delta close-all request accepted.")
    add_log(f"Close-all request accepted for {len(positions)} position(s); {len(skipped)} skipped by Delta.", "success")
    return {"success": True, "closed": len(positions) - len(skipped), "skipped": skipped}


@app.get("/api/positions")
async def get_positions():
    if trade_client is None:
        return {"connected": False, "positions": [], "error": "Connect to Delta to view account positions."}
    try:
        positions = await run_trade_call(trade_client.get_open_positions)
        return {"connected": True, "positions": positions}
    except Exception as exc:
        add_log(f"Could not refresh open positions: {str(exc)[:180]}", "error")
        raise HTTPException(502, f"Could not refresh positions: {str(exc)[:240]}") from exc


@app.post("/api/positions/close")
async def close_one_position(body: PositionAction):
    if not body.confirmed:
        raise HTTPException(400, "Confirm the position close first.")
    if trade_client is None:
        raise HTTPException(401, "Connect to Delta first.")
    symbol = body.symbol.strip().upper()
    auto_execution["armed"] = False
    add_log(f"Auto execution disarmed before manually closing {symbol}.", "warning")
    try:
        positions = await run_trade_call(trade_client.get_open_positions)
        position = next((item for item in positions
            if str(item.get("product_symbol") or item.get("symbol") or "").upper() == symbol
            and int(float(item.get("size") or 0)) != 0), None)
        if position is None:
            raise ValueError(f"No open {symbol} position was found on Delta.")
        product_id = position.get("product_id") or (position.get("product") or {}).get("id")
        if product_id is None:
            raise ValueError(f"Delta returned no product ID for {symbol}.")
        size = int(float(position["size"]))
        side = str(position.get("side") or ("buy" if size > 0 else "sell")).lower()
        result = await run_trade_call(trade_client.close_position_market, symbol, int(product_id), abs(size), side)
        order_id = result.get("id") or result.get("order_id") if isinstance(result, dict) else None
        add_order_event(symbol, "close", "Close requested", f"Reduce-only market close sent for {abs(size)} contract(s).", order_id)
        add_log(f"Manual close request accepted for {symbol} ({abs(size)} contract(s)); order {order_id or 'accepted'}.", "success")
        return {"success": True, "symbol": symbol, "order_id": order_id, "result": result}
    except Exception as exc:
        add_order_event(symbol, "close", "Rejected", str(exc)[:240])
        add_log(f"Manual close failed for {symbol}: {str(exc)[:180]}", "error")
        raise HTTPException(502, f"Could not close {symbol}: {str(exc)[:240]}") from exc


@app.post("/api/start")
async def start(body: ScanRequest):
    global scan_task, scan_config, latest
    if body.timeframe not in RESOLUTIONS:
        raise HTTPException(400, "Unsupported timeframe")
    if body.macd_fast >= body.macd_slow:
        raise HTTPException(400, "MACD fast must be lower than MACD slow")
    symbols = list(dict.fromkeys(s.strip().upper() for s in body.symbols if s.strip()))
    if not symbols:
        raise HTTPException(400, "Add at least one symbol")
    auto_execution["armed"] = False
    last_auto_candle.clear()
    scan_config = {"settings": {**body.model_dump(), "symbols": symbols}, "environment": (credentials or {}).get("environment", "india")}
    if scan_task and not scan_task.done():
        scan_task.cancel()
    latest = {"running": True, "rows": [], "timestamp": None, "error": None}
    scan_task = asyncio.create_task(scan_loop())
    add_log(f"Scanner started for {len(symbols)} markets ({body.timeframe}).")
    return {"running": True}


@app.post("/api/stop")
async def stop():
    global scan_task, latest
    if scan_task and not scan_task.done():
        scan_task.cancel()
    scan_task = None
    latest["running"] = False
    auto_execution["armed"] = False
    add_log("Scanner stopped.")
    return {"running": False}
