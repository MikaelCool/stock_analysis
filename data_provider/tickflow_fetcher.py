# -*- coding: utf-8 -*-
"""
===================================
TickFlowFetcher - market review only
===================================

Issue #632 only requires TickFlow for A-share market review stability.
This fetcher intentionally implements a narrow P0 surface:

1. Main A-share indices quotes
2. A-share market breadth statistics

It does not participate in the general daily-data or per-stock realtime
pipelines and should only be called explicitly by DataFetcherManager.
"""

import logging
import math
import re
from datetime import datetime, timedelta
from threading import RLock
from time import monotonic, sleep
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import (
    _is_hk_market,
    BaseFetcher,
    DataFetchError,
    is_bse_code,
    is_kc_cy_stock,
    is_st_stock,
    normalize_stock_code,
)
from .realtime_types import RealtimeSource, UnifiedRealtimeQuote


logger = logging.getLogger(__name__)

_CN_MAIN_INDEX_QUOTES = (
    ("000001.SH", "000001", "上证指数"),
    ("399001.SZ", "399001", "深证成指"),
    ("399006.SZ", "399006", "创业板指"),
    ("000688.SH", "000688", "科创50"),
    ("000016.SH", "000016", "上证50"),
    ("000300.SH", "000300", "沪深300"),
)
_US_PROXY_INDEX_QUOTES = (
    ("SPY.US", "SPY", "标普500ETF"),
    ("QQQ.US", "QQQ", "纳指100ETF"),
    ("DIA.US", "DIA", "道琼斯ETF"),
    ("VXX.US", "VXX", "VIX短期期货ETN"),
)
_MAX_SYMBOLS_PER_QUOTE_REQUEST = 5
_MAX_SYMBOLS_PER_KLINE_BATCH = 90
_KLINE_BATCH_MIN_INTERVAL_SECONDS = 2.2
_UNIVERSE_PERMISSION_NEGATIVE_CACHE_TTL_SECONDS = 900


class TickFlowFetcher(BaseFetcher):
    """TickFlow-backed market review helper."""

    name = "TickFlowFetcher"
    priority = 99

    def __init__(self, api_key: Optional[str], timeout: float = 30.0):
        self.api_key = (api_key or "").strip()
        self.timeout = timeout
        self._client = None
        self._client_lock = RLock()
        self._universe_query_supported: Optional[bool] = None
        self._universe_query_checked_at: Optional[float] = None

    def close(self) -> None:
        """Close the underlying TickFlow client if it was created."""
        with self._client_lock:
            client = self._client
            self._client = None
            self._universe_query_supported = None
            self._universe_query_checked_at = None
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                logger.debug("[TickFlowFetcher] 关闭客户端失败: %s", exc)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            # Best-effort cleanup during interpreter shutdown.
            pass

    def _build_client(self):
        from tickflow import TickFlow

        return TickFlow(api_key=self.api_key, timeout=self.timeout)

    def _get_client(self):
        if not self.api_key:
            return None
        if self._client is not None:
            return self._client

        with self._client_lock:
            if self._client is None:
                self._client = self._build_client()
            return self._client

    def _fetch_raw_data(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        client = self._get_client()
        if client is None:
            raise DataFetchError("TickFlow API key not configured")

        symbol = self._to_symbol(stock_code)
        if not symbol:
            raise DataFetchError(f"TickFlow unsupported symbol: {stock_code}")

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(milliseconds=1)
        df = client.klines.get(
            symbol,
            period="1d",
            start_time=int(start_dt.timestamp() * 1000),
            end_time=int(end_dt.timestamp() * 1000),
            adjust="forward",
            as_dataframe=True,
        )
        if df is None:
            return pd.DataFrame()
        return df

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        normalized = df.copy()
        if "trade_date" in normalized.columns:
            normalized = normalized.rename(columns={"trade_date": "date"})
        elif "timestamp" in normalized.columns:
            normalized["date"] = (
                pd.to_datetime(normalized["timestamp"], unit="ms", utc=True)
                .dt.tz_convert("Asia/Shanghai")
                .dt.strftime("%Y-%m-%d")
            )
        else:
            raise DataFetchError("TickFlow kline response missing trade_date/timestamp")

        if "pct_chg" not in normalized.columns:
            normalized["pct_chg"] = normalized["close"].astype(float).pct_change() * 100.0

        columns = ["date", "open", "high", "low", "close", "volume", "amount", "pct_chg"]
        for column in columns:
            if column not in normalized.columns:
                normalized[column] = 0.0

        normalized = normalized[columns].copy()
        normalized["date"] = pd.to_datetime(normalized["date"])
        numeric_columns = [c for c in columns if c != "date"]
        for column in numeric_columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        return normalized.sort_values("date").reset_index(drop=True)

    @staticmethod
    def _to_symbol(stock_code: str) -> Optional[str]:
        raw_code = (stock_code or "").strip().upper()
        normalized_code = normalize_stock_code(stock_code)

        if raw_code.endswith((".SH", ".SZ", ".BJ", ".US", ".HK")):
            return raw_code

        if _is_hk_market(raw_code):
            digits = normalized_code.replace("HK", "")
            if digits.isdigit():
                return f"{digits.zfill(5)}.HK"

        if normalized_code.isdigit() and len(normalized_code) == 6:
            if is_bse_code(normalized_code):
                return f"{normalized_code}.BJ"
            if normalized_code.startswith(("5", "6", "9")):
                return f"{normalized_code}.SH"
            return f"{normalized_code}.SZ"

        if raw_code.isalpha():
            return f"{raw_code}.US"

        return None

    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        client = self._get_client()
        if client is None:
            return None

        symbol = self._to_symbol(stock_code)
        if not symbol:
            return None

        quotes = client.quotes.get(symbols=[symbol])
        if not quotes:
            return None

        quote = quotes[0]
        if not quote:
            return None

        ext = quote.get("ext") or {}
        return UnifiedRealtimeQuote(
            code=normalize_stock_code(stock_code),
            name=self._extract_name(quote),
            source=RealtimeSource.TICKFLOW,
            price=self._safe_float(quote.get("last_price")),
            change_pct=self._ratio_to_percent(ext.get("change_pct")),
            change_amount=self._safe_float(ext.get("change_amount")),
            volume=int(self._safe_float(quote.get("volume")) or 0),
            amount=self._safe_float(quote.get("amount")),
            turnover_rate=(self._ratio_to_percent(ext.get("turnover_rate"))),
            amplitude=self._ratio_to_percent(ext.get("amplitude")),
            open_price=self._safe_float(quote.get("open")),
            high=self._safe_float(quote.get("high")),
            low=self._safe_float(quote.get("low")),
            pre_close=self._safe_float(quote.get("prev_close")),
        )

    def get_daily_batch(
        self,
        stock_codes: List[str],
        *,
        count: int,
        end_date: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        client = self._get_client()
        if client is None:
            return {}

        code_symbol_pairs: List[tuple[str, str]] = []
        for stock_code in stock_codes:
            symbol = self._to_symbol(stock_code)
            normalized_code = normalize_stock_code(stock_code)
            if symbol and normalized_code:
                code_symbol_pairs.append((normalized_code, symbol))

        if not code_symbol_pairs:
            return {}

        batch_kwargs: Dict[str, Any] = {
            "count": count,
            "period": "1d",
            "adjust": "forward",
        }
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(milliseconds=1)
            batch_kwargs["end_time"] = int(end_dt.timestamp() * 1000)

        frames: Dict[str, pd.DataFrame] = {}
        last_batch_at: Optional[float] = None
        for offset in range(0, len(code_symbol_pairs), _MAX_SYMBOLS_PER_KLINE_BATCH):
            chunk_pairs = code_symbol_pairs[offset : offset + _MAX_SYMBOLS_PER_KLINE_BATCH]
            chunk_symbols = [symbol for _, symbol in chunk_pairs]
            if last_batch_at is not None:
                elapsed = monotonic() - last_batch_at
                if elapsed < _KLINE_BATCH_MIN_INTERVAL_SECONDS:
                    sleep(_KLINE_BATCH_MIN_INTERVAL_SECONDS - elapsed)

            payload = None
            for attempt in range(3):
                try:
                    payload = client.klines.batch(chunk_symbols, **batch_kwargs)
                    last_batch_at = monotonic()
                    break
                except Exception as exc:
                    message = str(exc)
                    match = re.search(r"(\d+)ms\s*后重试", message)
                    if attempt >= 2:
                        raise
                    wait_seconds = max(_KLINE_BATCH_MIN_INTERVAL_SECONDS, (int(match.group(1)) / 1000.0) if match else _KLINE_BATCH_MIN_INTERVAL_SECONDS)
                    logger.info("[TickFlowFetcher] K线批量查询限流，等待 %.2fs 后重试", wait_seconds)
                    sleep(wait_seconds)

            for code, symbol in chunk_pairs:
                raw = (payload or {}).get(symbol)
                if not raw:
                    continue
                normalized = self._normalize_data(pd.DataFrame(raw), code)
                if not normalized.empty:
                    frames[code] = normalized

        return frames

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _ratio_to_percent(cls, value: Any) -> Optional[float]:
        ratio = cls._safe_float(value)
        if ratio is None:
            return None
        return ratio * 100.0

    @staticmethod
    def _extract_name(quote: Dict[str, Any]) -> str:
        ext = quote.get("ext") or {}
        name = ext.get("name") or quote.get("name") or ""
        return str(name).strip()

    @staticmethod
    def _is_universe_permission_error(exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        code = str(getattr(exc, "code", "") or "").upper()
        message = (
            f"{getattr(exc, 'message', '')} {exc}"
        ).strip().lower()

        if status_code == 403:
            return True
        if code in {"PERMISSION_DENIED", "FORBIDDEN"}:
            return True
        return any(
            keyword in message
            for keyword in (
                "标的池查询",
                "universe",
                "permission",
                "forbidden",
            )
        )

    @staticmethod
    def _is_cn_equity_symbol(symbol: str) -> bool:
        normalized = normalize_stock_code(symbol)
        upper_symbol = (symbol or "").strip().upper()
        return (
            normalized.isdigit()
            and len(normalized) == 6
            and upper_symbol.endswith((".SH", ".SZ", ".BJ"))
        )

    @staticmethod
    def _round_limit_price(prev_close: float, ratio: float) -> float:
        return math.floor(prev_close * (1 + ratio) * 100 + 0.5) / 100.0

    @classmethod
    def _get_limit_ratio(cls, pure_code: str, name: str) -> float:
        if is_bse_code(pure_code):
            return 0.30
        if is_kc_cy_stock(pure_code):
            return 0.20
        if is_st_stock(name):
            return 0.05
        return 0.10

    def get_main_indices(self, region: str = "cn") -> Optional[List[Dict[str, Any]]]:
        """Fetch main A-share indices via TickFlow quotes."""
        if region not in {"cn", "us"}:
            return None

        client = self._get_client()
        if client is None:
            return None

        quote_specs = _CN_MAIN_INDEX_QUOTES if region == "cn" else _US_PROXY_INDEX_QUOTES
        symbols = [symbol for symbol, _, _ in quote_specs]
        quotes: List[Dict[str, Any]] = []
        for offset in range(0, len(symbols), _MAX_SYMBOLS_PER_QUOTE_REQUEST):
            batch_symbols = symbols[offset : offset + _MAX_SYMBOLS_PER_QUOTE_REQUEST]
            batch_quotes = client.quotes.get(symbols=batch_symbols)
            if batch_quotes:
                quotes.extend(batch_quotes)
        if not quotes:
            logger.warning("[TickFlowFetcher] 指数行情为空")
            return None

        quotes_by_symbol = {
            str(item.get("symbol", "")).upper(): item for item in quotes if item
        }
        results: List[Dict[str, Any]] = []

        for symbol, code, name in quote_specs:
            quote = quotes_by_symbol.get(symbol)
            if not quote:
                continue

            ext = quote.get("ext") or {}
            current = self._safe_float(quote.get("last_price")) or 0.0
            prev_close = self._safe_float(quote.get("prev_close")) or 0.0
            change = self._safe_float(ext.get("change_amount"))
            if change is None:
                change = current - prev_close if current or prev_close else 0.0
            amplitude = self._ratio_to_percent(ext.get("amplitude"))
            if amplitude is None and prev_close > 0:
                high = self._safe_float(quote.get("high")) or 0.0
                low = self._safe_float(quote.get("low")) or 0.0
                amplitude = (high - low) / prev_close * 100

            results.append(
                {
                    "code": code,
                    "name": name,
                    "current": current,
                    "change": change,
                    "change_pct": self._ratio_to_percent(ext.get("change_pct")) or 0.0,
                    "open": self._safe_float(quote.get("open")) or 0.0,
                    "high": self._safe_float(quote.get("high")) or 0.0,
                    "low": self._safe_float(quote.get("low")) or 0.0,
                    "prev_close": prev_close,
                    "volume": self._safe_float(quote.get("volume")) or 0.0,
                    "amount": self._safe_float(quote.get("amount")) or 0.0,
                    "amplitude": amplitude or 0.0,
                }
            )

        if len(results) != len(quote_specs):
            logger.warning(
                "[TickFlowFetcher] 指数行情不完整: %s/%s",
                len(results),
                len(quote_specs),
            )
            return None

        return results or None

    def get_market_stats(self) -> Optional[Dict[str, Any]]:
        """Calculate A-share market breadth from TickFlow universe quotes."""
        client = self._get_client()
        if client is None:
            return None

        now = monotonic()
        if self._universe_query_supported is False:
            checked_at = self._universe_query_checked_at or 0.0
            if (
                now - checked_at
                < _UNIVERSE_PERMISSION_NEGATIVE_CACHE_TTL_SECONDS
            ):
                return None
            self._universe_query_supported = None
            self._universe_query_checked_at = None

        try:
            quotes = client.quotes.get(universes=["CN_Equity_A"])
            self._universe_query_supported = True
            self._universe_query_checked_at = now
        except Exception as exc:
            if self._is_universe_permission_error(exc):
                self._universe_query_supported = False
                self._universe_query_checked_at = now
                logger.info(
                    "[TickFlowFetcher] 当前套餐不支持标的池查询，市场统计回退到现有数据源"
                )
                return None
            raise
        if not quotes:
            logger.warning("[TickFlowFetcher] 市场统计行情为空")
            return None

        stats = {
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "limit_up_count": 0,
            "limit_down_count": 0,
            "total_amount": 0.0,
        }
        valid_rows = 0

        for quote in quotes:
            if not quote:
                continue

            symbol = str(quote.get("symbol") or "").strip().upper()
            if not self._is_cn_equity_symbol(symbol):
                continue

            amount = self._safe_float(quote.get("amount"))
            if amount is not None and amount > 0:
                stats["total_amount"] += amount / 1e8

            pure_code = normalize_stock_code(symbol)
            last_price = self._safe_float(quote.get("last_price"))
            prev_close = self._safe_float(quote.get("prev_close"))

            if last_price is None or prev_close is None or amount is None or amount <= 0:
                continue

            name = self._extract_name(quote)
            if not name:
                logger.debug("[TickFlowFetcher] 缺少股票名称，按非 ST 处理: %s", symbol)

            ratio = self._get_limit_ratio(pure_code, name)
            limit_up = self._round_limit_price(prev_close, ratio)
            limit_down = math.floor(prev_close * (1 - ratio) * 100 + 0.5) / 100.0
            limit_up_tolerance = round(abs(prev_close * (1 + ratio) - limit_up), 10)
            limit_down_tolerance = round(
                abs(prev_close * (1 - ratio) - limit_down), 10
            )

            valid_rows += 1

            if abs(last_price - limit_up) <= limit_up_tolerance:
                stats["limit_up_count"] += 1
            if abs(last_price - limit_down) <= limit_down_tolerance:
                stats["limit_down_count"] += 1

            if last_price > prev_close:
                stats["up_count"] += 1
            elif last_price < prev_close:
                stats["down_count"] += 1
            else:
                stats["flat_count"] += 1

        if valid_rows == 0:
            logger.warning("[TickFlowFetcher] 市场统计未命中有效 A 股行情")
            return None

        return stats
