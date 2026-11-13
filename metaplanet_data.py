"""Read the public JSON used by https://analytics.metaplanet.jp/.

Amounts returned by this module are base JPY/USD, never display abbreviations.
The historical feed is a daily, provider-revised series, not a reconstruction
of the old scraper's 17:00 JST observations. Missing source values stay None.
"""

from datetime import date, datetime, timedelta, timezone
import json
import math
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "https://data.strategytracker.com"
POINTER_URL = BASE_URL + "/latest.json"
TICKER = "3350.T"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; meta-analysis/2.0)",
    "Accept": "application/json",
    "Referer": "https://analytics.metaplanet.jp/",
    "Origin": "https://analytics.metaplanet.jp",
}


class SourceDataError(ValueError):
    """The provider response cannot safely be written to the sheet."""


def _get_json(url, attempts=3):
    for attempt in range(attempts):
        try:
            with urlopen(Request(url, headers=HEADERS), timeout=45) as response:
                payload = json.load(response)
            if not isinstance(payload, dict):
                raise SourceDataError(f"Expected JSON object from {url}")
            return payload
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (404, 408, 429, 500, 502, 503, 504):
                raise
            if attempt + 1 == attempts:
                raise
            time.sleep(2 ** attempt)


def _timestamp(payload):
    try:
        value = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError("timestamp has no timezone")
        return value
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceDataError("Missing/invalid provider timestamp") from exc


def fetch_snapshot():
    """Fetch one consistent version; fail on stale data instead of relabeling it."""
    pointer = _get_json(POINTER_URL)
    version = str(pointer.get("version", ""))
    if not re.fullmatch(r"\d{8}T\d{6}Z", version):
        raise SourceDataError("Unexpected version in latest.json")
    url = f"{BASE_URL}/3350_T.v{version}.json"
    payload = _get_json(url)
    timestamp = _timestamp(payload)
    age = datetime.now(timezone.utc) - timestamp
    if age > timedelta(days=3) or age < -timedelta(hours=1):
        raise SourceDataError(f"Provider snapshot timestamp is stale or in the future: {timestamp}")
    payload["_source"] = {
        "url": url,
        "pointer_url": POINTER_URL,
        "version": version,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }
    # Validate the complete series before exposing it to a writer.
    normalize_history(payload)
    return payload


def _number(value, field):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SourceDataError(f"Non-numeric {field}")
    if not math.isfinite(value) or value < 0:
        raise SourceDataError(f"Invalid {field}: {value}")
    return float(value)


def _divide(value, divisor):
    return value / divisor if value is not None and divisor not in (None, 0) else None


def _preferred_usd(stocks, day, fx):
    """Use disclosed effective-date notionals, never today's amount for old dates.

    The official UI's historical EV calculation uses the same effective-date
    history and converts JPY notionals with that day's USD-per-JPY rate.
    Unknown historical coverage or currencies are left missing.
    """
    if stocks is None:
        return None
    if not isinstance(stocks, list):
        raise SourceDataError("preferredStocks is not a list")
    total = 0.0
    for stock in stocks:
        history = stock.get("history")
        if not isinstance(history, list) or not history:
            return None
        for entry in history:
            try:
                date.fromisoformat(entry["effective_date"])
            except (KeyError, TypeError, ValueError) as exc:
                raise SourceDataError("Preferred stock history has invalid effective_date") from exc
        eligible = [entry for entry in history if entry["effective_date"] <= day]
        if not eligible:
            continue  # The disclosed issue did not exist yet.
        entry = max(eligible, key=lambda item: item["effective_date"])
        if entry.get("notionalUSD") is not None:
            amount = _number(entry["notionalUSD"], "preferred notionalUSD")
        else:
            millions = entry.get("notional_millions", entry.get("notionalMillions"))
            millions = _number(millions, "preferred notional_millions")
            currency = entry.get("currency", stock.get("currency"))
            if millions is None:
                return None
            if currency == "JPY":
                if fx is None:
                    return None
                amount = millions * 1_000_000 * fx
            elif currency == "USD":
                amount = millions * 1_000_000
            else:
                return None
        total += amount
    return total


def normalize_history(payload):
    """Return all provider dates with normalized fields and explicit nulls.

    btc_per_1000 and shares_outstanding use diluted shares for compatibility
    with the old Bitcoin per 1,000 Shares card. basic_shares is separate.
    mnav is the official EV definition; mnav_legacy preserves (cap+debt)/NAV.
    """
    try:
        company = payload["companies"][TICKER]
        usd = company["historicalData"]
        jpy = company["historicalDataOriginal"]
        metrics = company["processedMetrics"]
        metadata = company["currencyMetadata"]
        dates = usd["dates"]
    except (KeyError, TypeError) as exc:
        raise SourceDataError("Missing Metaplanet history in provider response") from exc
    if metadata.get("originalCurrency") != "JPY" or metadata.get("targetCurrency") != "USD":
        raise SourceDataError("Unexpected source currency conversion")
    if not isinstance(dates, list) or not dates or jpy.get("dates") != dates:
        raise SourceDataError("Missing or misaligned USD/JPY dates")
    try:
        parsed = [date.fromisoformat(day) for day in dates]
    except (TypeError, ValueError) as exc:
        raise SourceDataError("Invalid history date") from exc
    if any((right - left).days != 1 for left, right in zip(parsed, parsed[1:])):
        raise SourceDataError("History has missing, duplicate, or unordered dates")
    required_usd = [
        "stock_prices", "btc_prices", "btc_balance", "btc_per_share",
        "btc_per_diluted_share", "total_outstanding_shares",
        "diluted_shares_outstanding", "market_cap_basic", "holdings_value",
        "debt", "cash_balance", "use_ev",
    ]
    required_jpy = ["stock_prices", "market_cap_basic", "holdings_value", "historical_fx_rates"]
    for section, keys in ((usd, required_usd), (jpy, required_jpy)):
        for key in keys:
            if not isinstance(section.get(key), list) or len(section[key]) != len(dates):
                raise SourceDataError(f"Missing or misaligned history field: {key}")
    timestamp = _timestamp(payload).isoformat()
    stocks = metrics.get("preferredStocks")
    rows = []
    has_held_bitcoin = False
    for i, day in enumerate(dates):
        u = {key: _number(usd[key][i], key) for key in required_usd if key != "use_ev"}
        j = {key: _number(jpy[key][i], key) for key in required_jpy}
        fx = j["historical_fx_rates"]
        if fx is not None and fx <= 0:
            raise SourceDataError(f"Non-positive exchange rate on {day}")
        if u["btc_balance"] is not None and u["btc_balance"] > 0:
            has_held_bitcoin = True
        if has_held_bitcoin and u["btc_balance"] is None:
            raise SourceDataError(f"Missing BTC balance after adoption on {day}")
        if u["btc_balance"] is not None and u["btc_balance"] > 0:
            positive_fields = (
                "stock_prices", "btc_prices", "btc_per_share", "btc_per_diluted_share",
                "total_outstanding_shares", "diluted_shares_outstanding",
                "market_cap_basic", "holdings_value",
            )
            for key in positive_fields:
                if u[key] is None or u[key] <= 0:
                    raise SourceDataError(f"Missing/non-positive {key} after adoption on {day}")
            for key in ("debt", "cash_balance"):
                if u[key] is None:
                    raise SourceDataError(f"Missing {key} after adoption on {day}")
            for key in required_jpy:
                if j[key] is None or j[key] <= 0:
                    raise SourceDataError(f"Missing/non-positive JPY {key} after adoption on {day}")
        for key in ("stock_prices", "market_cap_basic", "holdings_value"):
            if u[key] is not None and j[key] is not None and fx is not None:
                if not math.isclose(j[key] * fx, u[key], rel_tol=1e-6, abs_tol=0.01):
                    raise SourceDataError(f"USD/JPY mismatch for {key} on {day}")
        for field, shares in (("btc_per_share", "total_outstanding_shares"),
                              ("btc_per_diluted_share", "diluted_shares_outstanding")):
            ratio = _divide(u["btc_balance"], u[shares])
            if ratio is not None and u[field] is not None:
                if not math.isclose(ratio, u[field], rel_tol=1e-6, abs_tol=1e-12):
                    raise SourceDataError(f"Inconsistent {field} on {day}")
        preferred = _preferred_usd(stocks, day, fx)
        nav, cap, debt, cash = (u[key] for key in ("holdings_value", "market_cap_basic", "debt", "cash_balance"))
        legacy = _divide(cap + debt, nav) if cap is not None and debt is not None else None
        use_ev = usd["use_ev"][i]
        if use_ev is not None and not isinstance(use_ev, bool):
            raise SourceDataError(f"Invalid use_ev on {day}")
        official = None
        if use_ev is True and all(v is not None for v in (cap, debt, cash, preferred)):
            official = _divide(cap + debt + preferred - cash, nav)
        elif use_ev is False:
            official = _divide(cap, nav)
        if u["btc_balance"] is not None and u["btc_balance"] > 0 and official is None:
            raise SourceDataError(f"Missing official mNAV definition or preferred-stock history on {day}")
        rows.append({
            "date": day,
            "btc_holdings": u["btc_balance"],
            "btc_per_1000": None if u["btc_per_diluted_share"] is None else u["btc_per_diluted_share"] * 1000,
            "btc_per_1000_basic": None if u["btc_per_share"] is None else u["btc_per_share"] * 1000,
            "share_price_jpy": j["stock_prices"],
            "btc_price_usd": u["btc_prices"],
            "btc_price_jpy": _divide(u["btc_prices"], fx),
            "market_cap_jpy": j["market_cap_basic"],
            "btc_nav_jpy": j["holdings_value"],
            "debt_jpy": _divide(debt, fx),
            "cash_jpy": _divide(cash, fx),
            "preferred_jpy": _divide(preferred, fx),
            "usd_jpy": _divide(1, fx),
            "shares_outstanding": u["diluted_shares_outstanding"],
            "diluted_shares": u["diluted_shares_outstanding"],
            "basic_shares": u["total_outstanding_shares"],
            "mnav": official,
            "mnav_legacy": legacy,
            "source_timestamp": timestamp,
        })
    if parsed[-1] > _timestamp(payload).date() + timedelta(days=1):
        raise SourceDataError("History contains future dates relative to the source timestamp")
    if (_timestamp(payload).date() - parsed[-1]).days > 3:
        raise SourceDataError("History ends too far before the source timestamp")
    if rows[-1]["btc_holdings"] in (None, 0) or rows[-1]["btc_price_usd"] in (None, 0):
        raise SourceDataError("Latest snapshot lacks BTC holdings or price")
    return rows


def fetch_history(payload=None):
    return normalize_history(fetch_snapshot() if payload is None else payload)


def fetch_latest(payload=None):
    """Latest dated provider snapshot (not a separately fetched intraday quote)."""
    row = fetch_history(payload)[-1].copy()
    row["as_of_date"] = row["date"]
    return row
