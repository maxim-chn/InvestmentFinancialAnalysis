from __future__ import annotations

import json
import os
import threading

from typing import Dict, Optional, Union

from src.raw_features.logger import log_message

PriceValue = Union[float, str]

_PRICE_CACHE: Dict[str, Dict[str, PriceValue]] = {}
_PRICE_CACHE_LOADED = False
_PRICE_CACHE_LOCK = threading.Lock()
_PRICE_CACHE_PATH_LOGGED = False


def _log_price_issue(message: str, level: str = "INFO") -> None:
  try:
    log_message(message, level=level)
  except Exception:
    return


def _log_price_info(message: str) -> None:
  _log_price_issue(message, level="INFO")


def _get_assets_dir() -> str:
  assets_dir = os.getenv("RAW_FEATURES_SPARK_PUBLISHER_ASSETS")
  if assets_dir is None or assets_dir.strip() == "":
    raise RuntimeError("Missing required env var RAW_FEATURES_SPARK_PUBLISHER_ASSETS")
  return assets_dir.strip()


def _get_price_cache_path() -> str:
  return os.path.join(_get_assets_dir(), "price_cache.json")


def _normalize_ticker(ticker: object) -> str:
  return str(ticker).strip().upper()


def _normalize_year_key(year: object) -> str:
  return str(int(year))


def _normalize_price_value(price: PriceValue) -> PriceValue:
  if isinstance(price, (int, float)) and not isinstance(price, bool):
    return round(float(price), 4)
  return price


def _load_price_cache() -> Dict[str, Dict[str, PriceValue]]:
  global _PRICE_CACHE, _PRICE_CACHE_LOADED, _PRICE_CACHE_PATH_LOGGED

  if _PRICE_CACHE_LOADED:
    return _PRICE_CACHE

  with _PRICE_CACHE_LOCK:
    if _PRICE_CACHE_LOADED:
      return _PRICE_CACHE

    cache_path = _get_price_cache_path()
    if not _PRICE_CACHE_PATH_LOGGED:
      _log_price_info(f"Price cache path: {cache_path}")
      _PRICE_CACHE_PATH_LOGGED = True
    try:
      with open(cache_path, "r", encoding="utf-8") as cache_file:
        raw_cache = json.load(cache_file)
    except FileNotFoundError:
      raw_cache = {}
    except json.JSONDecodeError:
      raw_cache = {}

    normalized_cache: Dict[str, Dict[str, PriceValue]] = {}
    if isinstance(raw_cache, dict):
      for raw_ticker, ticker_prices in raw_cache.items():
        if not isinstance(ticker_prices, dict):
          continue
        ticker_key = _normalize_ticker(raw_ticker)
        normalized_cache[ticker_key] = {}
        for raw_year, price in ticker_prices.items():
          try:
            year_key = _normalize_year_key(raw_year)
          except (TypeError, ValueError):
            continue
          normalized_cache[ticker_key][year_key] = _normalize_price_value(price)

    _PRICE_CACHE = normalized_cache
    _PRICE_CACHE_LOADED = True
    return _PRICE_CACHE


def get_price_from_cache(ticker: str, year: int) -> Optional[PriceValue]:
  cache = _load_price_cache()
  ticker_key = _normalize_ticker(ticker)
  try:
    year_key = _normalize_year_key(year)
  except (TypeError, ValueError):
    return None
  return cache.get(ticker_key, {}).get(year_key)


def update_price_in_cache(ticker: str, year: int, price: PriceValue) -> PriceValue:
  cache = _load_price_cache()
  ticker_key = _normalize_ticker(ticker)
  year_key = _normalize_year_key(year)
  normalized_price = _normalize_price_value(price)

  with _PRICE_CACHE_LOCK:
    ticker_cache = cache.setdefault(ticker_key, {})
    ticker_cache[year_key] = normalized_price

    cache_path = _get_price_cache_path()
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as cache_file:
      json.dump(cache, cache_file, indent=2, sort_keys=True)
      cache_file.write("\n")
    _log_price_info(
      "Stored price for ticker '%s' fiscal year %s in cache file '%s': %s" % (
        ticker_key,
        year_key,
        cache_path,
        str(normalized_price)
      )
    )

  return normalized_price


def _extract_close_price_from_history(history) -> Optional[float]:
  if history is None or history.empty or "Close" not in history:
    return None

  close_values = history["Close"].dropna()
  if close_values.empty:
    return None

  try:
    return float(close_values.iloc[-1])
  except (TypeError, ValueError):
    return None


def _get_price_from_yfinance(ticker: str, year: int) -> Optional[float]:
  try:
    import yfinance as yf
  except ImportError:
    _log_price_issue(
      "Price lookup skipped for ticker '%s' fiscal year %s -- missing dependency yfinance" % (
        _normalize_ticker(ticker),
        str(year)
      ),
      level="ERROR"
    )
    return None

  start = f"{year}-01-01"
  end = f"{year + 1}-01-01"

  try:
    ticker_handle = yf.Ticker(_normalize_ticker(ticker))
    history = ticker_handle.history(
      start=start,
      end=end,
      auto_adjust=False
    )
  except Exception as e:
    _log_price_issue(
      "Price lookup failed for ticker '%s' fiscal year %s -- %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(e)
      ),
      level="ERROR"
    )
    return None

  price = _extract_close_price_from_history(history)
  if price is not None:
    price = _normalize_price_value(price)
    _log_price_info(
      "Fetched price from Yahoo for ticker '%s' fiscal year %s: %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(price)
      )
    )
    return price

  try:
    max_history = ticker_handle.history(period="max", auto_adjust=False)
  except Exception as e:
    _log_price_issue(
      "Price history fallback failed for ticker '%s' fiscal year %s -- %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(e)
      ),
      level="ERROR"
    )
    return None

  try:
    yearly_history = max_history[max_history.index.year == year]
  except Exception:
    yearly_history = None

  price = _extract_close_price_from_history(yearly_history)
  if price is not None:
    price = _normalize_price_value(price)
    _log_price_info(
      "Fetched price from Yahoo fallback history for ticker '%s' fiscal year %s: %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(price)
      )
    )
    return price

  _log_price_issue(
    "Price lookup returned no close history for ticker '%s' fiscal year %s" % (
      _normalize_ticker(ticker),
      str(year)
    ),
    level="WARNING"
  )
  return None


def get_price(ticker: str, year: int) -> PriceValue:
  cached_price = get_price_from_cache(ticker, year)
  if cached_price is not None:
    cached_price = _normalize_price_value(cached_price)
    _log_price_info(
      "Loaded price from cache for ticker '%s' fiscal year %s: %s" % (
        _normalize_ticker(ticker),
        str(year),
        str(cached_price)
      )
    )
    return cached_price

  try:
    normalized_year = int(year)
  except (TypeError, ValueError):
    _log_price_issue(
      "Price lookup skipped for ticker '%s' -- invalid fiscal year '%s'" % (
        _normalize_ticker(ticker),
        str(year)
      ),
      level="ERROR"
    )
    normalized_year = -1

  price = None
  if normalized_year > 0:
    price = _get_price_from_yfinance(ticker, normalized_year)

  if price is None:
    price = "N/A"
    _log_price_issue(
      "Using fallback price N/A for ticker '%s' fiscal year %s" % (
        _normalize_ticker(ticker),
        str(year)
      ),
      level="WARNING"
    )

  return update_price_in_cache(ticker, year, price)
