"""
MT5 vendor module for TradingAgents dataflows.

Provides OHLCV and technical indicator data directly from the MetaTrader5
terminal via its Python API. This gives real-time broker data instead of
delayed feeds from yfinance/Alpha Vantage.

Requires:
- MetaTrader5 Python package (Windows only, pip install MetaTrader5)
- MT5 terminal running and logged into a broker account
"""

from typing import Annotated
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MT5 connection helpers
# ---------------------------------------------------------------------------

_mt5 = None
_mt5_initialized = False


def _ensure_mt5():
    """Lazy-import and initialize the MetaTrader5 connection."""
    global _mt5, _mt5_initialized
    if _mt5_initialized:
        return _mt5

    try:
        import MetaTrader5 as mt5
        _mt5 = mt5
    except ImportError:
        raise ImportError(
            "MetaTrader5 package not installed. "
            "Install with: pip install MetaTrader5 (Windows only)"
        )

    if not _mt5.initialize():
        error = _mt5.last_error()
        raise RuntimeError(f"MT5 initialization failed: {error}")

    _mt5_initialized = True
    logger.info("MT5 vendor: connected to terminal")
    return _mt5


_TF_MAP = None


def _get_tf_map():
    """Build timeframe mapping after MT5 is imported."""
    global _TF_MAP
    if _TF_MAP is not None:
        return _TF_MAP
    mt5 = _ensure_mt5()
    _TF_MAP = {
        "M1": mt5.TIMEFRAME_M1,
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
        "W1": mt5.TIMEFRAME_W1,
        "MN1": mt5.TIMEFRAME_MN1,
    }
    return _TF_MAP


# ---------------------------------------------------------------------------
# OHLCV data  (matches get_YFin_data_online signature)
# ---------------------------------------------------------------------------

def get_mt5_stock(
    symbol: Annotated[str, "Ticker symbol (e.g. XAUUSD, EURUSD)"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """
    Fetch OHLCV data from the MT5 terminal for the given symbol and date range.

    Returns a CSV-formatted string with the same layout as the yfinance vendor
    so that downstream agents can consume it identically.
    """
    mt5 = _ensure_mt5()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)  # inclusive

    rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, start_dt, end_dt)

    if rates is None or len(rates) == 0:
        return (
            f"No data found for symbol '{symbol}' between {start_date} and {end_date}. "
            "Ensure the symbol is available in MT5 Market Watch."
        )

    # Build CSV string matching yfinance format
    lines = [
        f"# Stock data for {symbol} from {start_date} to {end_date}",
        f"# Total records: {len(rates)}",
        f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"# Source: MetaTrader5 broker feed",
        "",
        "Date,Open,High,Low,Close,Volume",
    ]

    for rate in rates:
        dt = datetime.utcfromtimestamp(rate[0]).strftime("%Y-%m-%d")
        o, h, l, c = round(rate[1], 2), round(rate[2], 2), round(rate[3], 2), round(rate[4], 2)
        vol = int(rate[5])
        lines.append(f"{dt},{o},{h},{l},{c},{vol}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Technical indicators  (matches get_stock_stats_indicators_window signature)
# ---------------------------------------------------------------------------

# Supported indicators — same set as the yfinance vendor
_SUPPORTED_INDICATORS = [
    "close_50_sma", "close_200_sma", "close_10_ema",
    "macd", "macds", "macdh",
    "rsi",
    "boll", "boll_ub", "boll_lb",
    "atr",
    "vwma",
    "mfi",
]

_INDICATOR_DESCRIPTIONS = {
    "close_50_sma": "50 SMA: A medium-term trend indicator. Price above the 50 SMA suggests an uptrend.",
    "close_200_sma": "200 SMA: A long-term trend indicator. Price above the 200 SMA suggests a long-term uptrend.",
    "close_10_ema": "10 EMA: A short-term trend indicator that reacts quickly to price changes.",
    "macd": "MACD Line: The difference between the 12-day and 26-day EMA. Positive values suggest bullish momentum.",
    "macds": "MACD Signal: The 9-day EMA of the MACD line. Crossovers with MACD are trading signals.",
    "macdh": "MACD Histogram: The difference between MACD and its signal line. Shows momentum strength.",
    "rsi": "RSI (14): Relative Strength Index. Above 70 = overbought, below 30 = oversold.",
    "boll": "Bollinger Band (Middle): 20-day SMA. The center line of the Bollinger Bands.",
    "boll_ub": "Bollinger Band (Upper): Upper band at 2 standard deviations above the middle band.",
    "boll_lb": "Bollinger Band (Lower): Lower band at 2 standard deviations below the middle band.",
    "atr": "ATR (14): Average True Range. Measures market volatility.",
    "vwma": "VWMA: Volume-Weighted Moving Average. Weighted towards periods with higher volume.",
    "mfi": "MFI (14): Money Flow Index. Like RSI but incorporates volume. Above 80 = overbought.",
}


def get_mt5_indicators(
    symbol: Annotated[str, "Ticker symbol (e.g. XAUUSD)"],
    indicator: Annotated[str, "Technical indicator name"],
    curr_date: Annotated[str, "Current trading date, YYYY-mm-dd format"],
    look_back_days: Annotated[int, "Number of days to look back"],
) -> str:
    """
    Compute a technical indicator from MT5 OHLCV data.

    Fetches daily bars from MT5, computes the indicator using stockstats,
    and returns the result in the same text format as the yfinance vendor.
    """
    if indicator not in _SUPPORTED_INDICATORS:
        raise ValueError(
            f"Indicator '{indicator}' not supported. "
            f"Choose from: {', '.join(_SUPPORTED_INDICATORS)}"
        )

    mt5 = _ensure_mt5()

    end_dt = datetime.strptime(curr_date, "%Y-%m-%d") + timedelta(days=1)
    # Fetch extra bars so indicators with long look-back (e.g. 200 SMA) have
    # enough history to converge before the visible window.
    fetch_days = look_back_days + 250
    start_dt = end_dt - timedelta(days=fetch_days)

    rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, start_dt, end_dt)

    if rates is None or len(rates) == 0:
        return f"No MT5 data available for {symbol} to compute {indicator}."

    # Convert to a pandas DataFrame compatible with stockstats
    try:
        import pandas as pd
        from stockstats import StockDataFrame
    except ImportError:
        return (
            "Required packages missing. Install with: "
            "pip install pandas stockstats"
        )

    df = pd.DataFrame(
        [
            {
                "date": datetime.utcfromtimestamp(r[0]),
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": int(r[5]),
            }
            for r in rates
        ]
    )
    df.set_index("date", inplace=True)

    stock_df = StockDataFrame.retype(df)

    try:
        values = stock_df[indicator]
    except Exception as e:
        return f"Error computing {indicator} for {symbol}: {e}"

    # Build output in the same format as yfinance vendor
    window_start = datetime.strptime(curr_date, "%Y-%m-%d") - timedelta(days=look_back_days)
    header = f"## {indicator} values from {window_start.strftime('%Y-%m-%d')} to {curr_date}:\n\n"

    lines = []
    for day_offset in range(look_back_days, -1, -1):
        day = datetime.strptime(curr_date, "%Y-%m-%d") - timedelta(days=day_offset)
        day_str = day.strftime("%Y-%m-%d")

        if day in values.index:
            val = values.loc[day]
            if pd.isna(val):
                lines.append(f"{day_str}: N/A")
            else:
                lines.append(f"{day_str}: {val:.4f}")
        else:
            lines.append(f"{day_str}: N/A: Not a trading day (weekend or holiday)")

    description = _INDICATOR_DESCRIPTIONS.get(indicator, "")
    return header + "\n".join(lines) + f"\n\n{description}"
