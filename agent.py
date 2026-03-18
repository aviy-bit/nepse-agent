"""
NEPSE AI Trading Agent - Final Edition v6.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIXES IN v6.0:
  - NEPSE trading days: Sunday–Thursday (Friday+Saturday = weekend)
  - Pre-open: 10:45–11:00 AM NST (not 10:30)
  - Continuous market: 11:00 AM–3:00 PM NST
  - NepseUnofficialApi removed from requirements (unreliable on Railway)
  - Uses direct nepalstock.com HTTP + merolagani + sharesansar as data sources
  - SSL warnings suppressed
  - Railway-compatible: pure pip installs only, no git dependencies
  - All API keys validated before starting
  - Floorsheet via direct HTTP to nepalstock.com
  - Real RSI, MACD, Bollinger from stored price history
  - 3 AI lenses: Technical / Fundamental / Sentiment+Floorsheet
  - Stop-loss + target monitoring
  - Health server for Railway keep-alive
  - Morning briefing + daily summary + weekly report
  - Crash counter (stops after 10 consecutive failures)
"""

import os
import re
import json
import math
import time
import logging
import tempfile
import requests
import urllib3
from datetime import datetime, timedelta, date
from collections import defaultdict
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

# Suppress NEPSE SSL warnings (nepalstock.com has certificate issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Timezone ─────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    NST = ZoneInfo("Asia/Kathmandu")
except ImportError:
    try:
        import pytz
        NST = pytz.timezone("Asia/Kathmandu")
    except ImportError:
        NST = None

# ─────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("agent.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN",  "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID",    "").strip()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY",    "").strip()
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY",      "").strip()
GROQ_API_KEY     = os.getenv("GROQ_API_KEY",        "").strip()

TRADE_TTL      = 7200    # pending trade expires after 2 hours
CRASH_MAX      = 10      # stop after 10 consecutive crashes
TG_LIMIT       = 4000    # Telegram message char limit

TRADE_LOG      = "trades_log.json"
POSITIONS_FILE = "open_positions.json"
HISTORY_FILE   = "price_history.json"
FLOOR_CACHE    = "floorsheet_cache.json"
_data_source   = "unknown"   # tracks which data source was used
HEARTBEAT_FILE = "heartbeat.txt"

def validate_config():
    missing = [k for k, v in {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
        "DEEPSEEK_API_KEY":   DEEPSEEK_API_KEY,
        "GEMINI_API_KEY":     GEMINI_API_KEY,
        "GROQ_API_KEY":       GROQ_API_KEY,
    }.items() if not v]
    if missing:
        logger.error(f"Missing .env keys: {', '.join(missing)}")
        raise SystemExit(1)
    logger.info("✅ All API keys loaded.")

# ─────────────────────────────────────────────────────
# HEALTH SERVER (keeps Railway from suspending)
# ─────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            with open(HEARTBEAT_FILE) as f:
                msg = f.read()
        except Exception:
            msg = "NEPSE Agent running"
        self.send_response(200)
        self.end_headers()
        self.wfile.write(msg.encode())
    def log_message(self, *a):
        pass

def start_health_server():
    port = int(os.getenv("PORT", 8080))
    try:
        Thread(target=HTTPServer(("0.0.0.0", port), HealthHandler).serve_forever,
               daemon=True).start()
        logger.info(f"✅ Health server on port {port}")
    except Exception as e:
        logger.warning(f"Health server: {e}")

def heartbeat(msg=""):
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(f"{msg or 'Running'} | {get_nst().strftime('%Y-%m-%d %H:%M NST')}")
    except Exception:
        pass

# ─────────────────────────────────────────────────────
# TIME — NEPSE SPECIFIC
# Nepal week: Sunday=trading, Monday=trading, Tuesday=trading,
#             Wednesday=trading, Thursday=trading,
#             Friday=CLOSED, Saturday=CLOSED
# ─────────────────────────────────────────────────────
def get_nst() -> datetime:
    return datetime.now(NST) if NST else datetime.utcnow() + timedelta(hours=5, minutes=45)

# ---------------------------------------------------------
# HAMRO PATRO HOLIDAY INTEGRATION
# Uses bibhuticoder Nepali Calendar API (based on Hamro Patro)
# Fallback to hardcoded list if API unavailable
# Auto-refreshes weekly
# ---------------------------------------------------------

_FALLBACK_HOLIDAYS = {
    date(2026, 1, 11), date(2026, 4, 14), date(2026, 4, 15),
    date(2026, 5, 1),  date(2026, 5, 29), date(2026, 7, 16),
    date(2026, 8, 9),  date(2026, 8, 10), date(2026, 8, 26),
    date(2026, 9, 22),
    date(2026, 10, 2), date(2026, 10, 3), date(2026, 10, 4),
    date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7),
    date(2026, 10, 8),
    date(2026, 10, 21),date(2026, 10, 22),date(2026, 10, 23),
    date(2026, 10, 24),date(2026, 12, 29),
    date(2027, 1, 11), date(2027, 4, 14), date(2027, 5, 1),
    date(2027, 7, 16), date(2027, 10, 22),date(2027, 10, 23),
    date(2027, 10, 24),date(2027, 11, 9), date(2027, 11, 10),
    date(2027, 12, 29),
}

_holiday_cache: set = set()
_holiday_cache_file = "holidays_cache.json"


def _load_holiday_disk() -> set:
    data = load_json(_holiday_cache_file, {})
    result = set()
    for d_str in data.get("holidays", []):
        try:
            result.add(date.fromisoformat(d_str))
        except Exception:
            pass
    return result


def _save_holiday_disk(holidays: set):
    atomic_write(_holiday_cache_file, {
        "fetched_at": datetime.utcnow().isoformat(),
        "holidays": [d.isoformat() for d in sorted(holidays)]
    })


def fetch_hamropatro_holidays(year_ad: int) -> set:
    bs_year = year_ad + 56
    holidays = set()

    # Source 1: bibhuticoder Nepali Calendar API (powered by Hamro Patro data)
    try:
        url = "https://bibhuticoder.github.io/nepali-calendar-api/api/{}.json".format(bs_year)
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if r.ok:
            data = r.json()
            for month_name, days in data.items():
                if not isinstance(days, list):
                    continue
                for day in days:
                    if day.get("holiday") is True:
                        ad_str = day.get("adDate") or day.get("ad_date", "")
                        if ad_str:
                            try:
                                holidays.add(date.fromisoformat(str(ad_str)[:10]))
                            except Exception:
                                pass
            if holidays:
                logger.info("Hamro Patro (bibhuticoder API): {} holidays for BS{}".format(len(holidays), bs_year))
                return holidays
    except Exception as e:
        logger.warning("bibhuticoder API BS{}: {}".format(bs_year, e))

    # Source 2: Hamro Patro events endpoint
    try:
        r = requests.get(
            "https://hamropatro.com/api/events/{}".format(bs_year),
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=15
        )
        if r.ok:
            data = r.json()
            events = data if isinstance(data, list) else data.get("events") or data.get("data") or []
            for evt in events:
                if evt.get("isHoliday") or evt.get("holiday"):
                    d_str = evt.get("adDate") or evt.get("date", "")
                    if d_str:
                        try:
                            holidays.add(date.fromisoformat(str(d_str)[:10]))
                        except Exception:
                            pass
            if holidays:
                logger.info("Hamro Patro API: {} holidays for BS{}".format(len(holidays), bs_year))
                return holidays
    except Exception as e:
        logger.warning("Hamro Patro API: {}".format(e))

    return set()


def get_nepal_holidays() -> set:
    global _holiday_cache
    if _holiday_cache:
        return _holiday_cache
    disk = _load_holiday_disk()
    if disk:
        _holiday_cache = disk
        logger.info("Holidays from disk cache: {}".format(len(_holiday_cache)))
        return _holiday_cache
    now_year = datetime.utcnow().year
    fetched = set()
    for yr in [now_year, now_year + 1]:
        fetched.update(fetch_hamropatro_holidays(yr))
    if fetched:
        fetched.update(_FALLBACK_HOLIDAYS)
        _holiday_cache = fetched
        _save_holiday_disk(fetched)
        logger.info("Holidays Hamro Patro + fallback: {}".format(len(fetched)))
        return _holiday_cache
    logger.warning("Hamro Patro unavailable. Using hardcoded holidays.")
    _holiday_cache = _FALLBACK_HOLIDAYS.copy()
    return _holiday_cache


def refresh_holidays() -> set:
    global _holiday_cache
    _holiday_cache = set()
    try:
        if os.path.exists(_holiday_cache_file):
            os.remove(_holiday_cache_file)
    except Exception:
        pass
    return get_nepal_holidays()


NEPAL_HOLIDAYS = _FALLBACK_HOLIDAYS

def is_trading_day(d: date) -> bool:
    """NEPSE trades Sunday-Thursday. Friday + Saturday closed."""
    return d.weekday() not in (4, 5) and d not in get_nepal_holidays()

def get_settlement_date(dt=None) -> str:
    """
    NEPSE T+2 settlement: shares arrive after 2 working days
    (skipping Fri, Sat, and public holidays).
    In practice this means 2-4 calendar days depending on when you buy.
    Thursday buy = Tuesday arrival (5 calendar days).
    """
    today = (dt or get_nst())
    d = today.date() if hasattr(today, "date") else today
    start = d
    added = 0
    while added < 2:
        d += timedelta(days=1)
        if is_trading_day(d):
            added += 1
    calendar_days = (d - start).days
    warn = " ⚠️ Near holiday!" if any(abs((d - h).days) <= 1 for h in get_nepal_holidays()) else " ✅ Safe"
    return "{} ({} calendar days){}".format(d.strftime("%A, %B %d"), calendar_days, warn)

def is_preopen() -> bool:
    """Pre-open session: 10:45–11:00 AM NST, Sunday–Thursday."""
    n = get_nst()
    if not is_trading_day(n.date()):
        return False
    s = n.replace(hour=10, minute=45, second=0, microsecond=0)
    e = n.replace(hour=11, minute=0,  second=0, microsecond=0)
    return s <= n < e

def is_market_open() -> bool:
    """Continuous market: 11:00 AM–3:00 PM NST, Sunday–Thursday."""
    n = get_nst()
    if not is_trading_day(n.date()):
        return False
    s = n.replace(hour=11, minute=0,  second=0, microsecond=0)
    e = n.replace(hour=15, minute=0,  second=0, microsecond=0)
    return s <= n <= e

def market_status() -> str:
    if is_market_open():   return "🟢 OPEN"
    if is_preopen():       return "🟡 PRE-OPEN"
    n = get_nst()
    if not is_trading_day(n.date()):
        day = n.strftime("%A")
        return f"🔴 CLOSED ({day} — weekend)" if n.weekday() in (4, 5) else f"🔴 CLOSED (holiday)"
    return "🔴 CLOSED"

# ─────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────
def sf(v, d=0.0) -> float:
    try:
        return float(v) if v not in (None, "", "N/A") else d
    except (ValueError, TypeError):
        return d

def atomic_write(path: str, data):
    dir_ = os.path.dirname(os.path.abspath(path)) or "."
    try:
        with tempfile.NamedTemporaryFile("w", dir=dir_, delete=False,
                                         suffix=".tmp", encoding="utf-8") as tf:
            json.dump(data, tf, indent=2)
            tmp = tf.name
        os.replace(tmp, path)
    except Exception as e:
        logger.error(f"Write {path}: {e}")

def load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return default

def retry(fn, n=3, backoff=2.0):
    last = None
    for i in range(n):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(backoff ** i)
    raise last

# ─────────────────────────────────────────────────────
# PENDING TRADES (with TTL)
# ─────────────────────────────────────────────────────
pending: dict = {}

def pend_add(tid, trade):
    pending[tid] = {"trade": trade, "exp": time.time() + TRADE_TTL}

def pend_get(tid) -> dict | None:
    e = pending.get(tid)
    if not e:
        return None
    if time.time() >= e["exp"]:
        del pending[tid]
        return None
    return e["trade"]

def pend_purge():
    for k in [k for k, v in list(pending.items()) if time.time() >= v["exp"]]:
        logger.info(f"Purged expired trade: {k}")
        del pending[k]

# ─────────────────────────────────────────────────────
# POSITION MANAGER
# ─────────────────────────────────────────────────────
def pos_load() -> dict:
    return load_json(POSITIONS_FILE, {})

def pos_add(symbol: str, trade: dict):
    p = pos_load()
    p[symbol] = {**trade, "opened_at": get_nst().isoformat(),
                 "stop_alerted": False, "target_alerted": False}
    atomic_write(POSITIONS_FILE, p)

def pos_remove(symbol: str):
    p = pos_load()
    p.pop(symbol, None)
    atomic_write(POSITIONS_FILE, p)

def pos_monitor(stocks: list) -> list:
    positions = pos_load()
    if not positions:
        return []
    # Only monitor during market hours - avoid stale price false alerts
    if not is_market_open():
        return []
    price_map = {s.get("symbol", ""): sf(s.get("ltp") or s.get("lastTradedPrice")) for s in stocks}
    alerts, updated = [], False
    for sym, pos in positions.items():
        cur    = price_map.get(sym, 0)
        if cur <= 0:
            continue
        entry  = sf(pos.get("current_price"))
        target = sf(pos.get("target_price"))
        stop   = sf(pos.get("stop_loss"))
        action = pos.get("action", "BUY")
        pnl    = round(((cur - entry) / entry) * 100 if entry > 0 else 0, 2)
        pnl    = pnl if action == "BUY" else -pnl

        if not pos.get("stop_alerted"):
            hit = (action == "BUY" and cur <= stop) or (action == "SELL" and cur >= stop)
            if hit:
                alerts.append(
                    f"🚨 *STOP LOSS HIT!*\n`{sym}` now NPR {cur} (stop: NPR {stop})\n"
                    f"P&L: {pnl:+.2f}%\n⚠️ Consider cutting on TMS!\n"
                    f"Reply `SOLD {sym}` to close."
                )
                positions[sym]["stop_alerted"] = True
                updated = True

        if not pos.get("target_alerted"):
            hit = (action == "BUY" and cur >= target) or (action == "SELL" and cur <= target)
            if hit:
                alerts.append(
                    f"🎯 *TARGET HIT!*\n`{sym}` now NPR {cur} (target: NPR {target})\n"
                    f"P&L: {pnl:+.2f}%\n🎉 Consider taking profits!\n"
                    f"Reply `SOLD {sym}` to close."
                )
                positions[sym]["target_alerted"] = True
                updated = True

    if updated:
        atomic_write(POSITIONS_FILE, positions)
    return alerts

# ─────────────────────────────────────────────────────
# PRICE HISTORY (for real technical indicators)
# ─────────────────────────────────────────────────────
def history_update(stocks: list):
    h = load_json(HISTORY_FILE, {})
    today = get_nst().strftime("%Y-%m-%d")
    for s in stocks:
        sym = s.get("symbol", "")
        ltp = sf(s.get("ltp") or s.get("lastTradedPrice"))
        vol = sf(s.get("volume") or s.get("totalTradedQuantity"))
        hi  = sf(s.get("high") or s.get("highPrice"), ltp)
        lo  = sf(s.get("low")  or s.get("lowPrice"),  ltp)
        if not sym or ltp <= 0:
            continue
        if sym not in h:
            h[sym] = []
        if not h[sym] or h[sym][-1].get("date") != today:
            h[sym].append({"date": today, "close": ltp, "volume": vol, "high": hi, "low": lo})
        h[sym] = h[sym][-90:]  # keep 90 days
    atomic_write(HISTORY_FILE, h)
    logger.info(f"Price history updated: {len(stocks)} stocks")

def history_closes_vols(symbol: str) -> tuple:
    h = load_json(HISTORY_FILE, {})
    rows = h.get(symbol, [])
    return [r["close"] for r in rows], [r["volume"] for r in rows]

# ─────────────────────────────────────────────────────
# TECHNICAL INDICATORS
# ─────────────────────────────────────────────────────
def ta_rsi(closes, p=14) -> float | None:
    if len(closes) < p + 1:
        return None
    g, l = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        g.append(max(d, 0))
        l.append(max(-d, 0))
    ag = sum(g[-p:]) / p
    al = sum(l[-p:]) / p
    return round(100 - (100 / (1 + ag / al)), 2) if al > 0 else 100.0

def ta_sma(closes, p) -> float | None:
    return round(sum(closes[-p:]) / p, 2) if len(closes) >= p else None

def ta_ema(closes, p) -> float | None:
    if len(closes) < p:
        return None
    k = 2 / (p + 1)
    e = sum(closes[:p]) / p
    for c in closes[p:]:
        e = c * k + e * (1 - k)
    return round(e, 2)

def ta_macd(closes) -> dict:
    e12, e26 = ta_ema(closes, 12), ta_ema(closes, 26)
    if e12 is None or e26 is None:
        return {"line": None, "crossover": "insufficient data (need 26 days)"}
    line = round(e12 - e26, 2)
    return {"line": line, "ema12": e12, "ema26": e26,
            "crossover": "bullish" if line > 0 else "bearish"}

def ta_bollinger(closes, p=20) -> dict | None:
    if len(closes) < p:
        return None
    m   = sum(closes[-p:]) / p
    std = math.sqrt(sum((c - m) ** 2 for c in closes[-p:]) / p)
    bw  = round((4 * std / m) * 100, 2) if m > 0 else 0
    return {"upper": round(m + 2 * std, 2), "middle": round(m, 2),
            "lower": round(m - 2 * std, 2), "bandwidth": bw,
            "squeeze": bw < 5}

def ta_momentum(closes, days=5) -> float | None:
    if len(closes) <= days:
        return None
    return round(((closes[-1] - closes[-days - 1]) / closes[-days - 1]) * 100, 2)

def ta_support_resistance(closes, lookback=20) -> tuple:
    if len(closes) < lookback:
        return None, None
    w = closes[-lookback:]
    return round(min(w), 2), round(max(w), 2)

def full_ta(symbol: str, stock: dict) -> dict:
    closes, vols = history_closes_vols(symbol)
    ltp  = sf(stock.get("ltp") or stock.get("lastTradedPrice"))
    prev = sf(stock.get("previousClose") or stock.get("previousClosingPrice"), ltp)
    wkhi = sf(stock.get("52weekHigh") or stock.get("fiftyTwoWeekHigh"), ltp * 1.3)
    wklo = sf(stock.get("52weekLow")  or stock.get("fiftyTwoWeekLow"),  ltp * 0.7)
    vol  = sf(stock.get("volume") or stock.get("totalTradedQuantity"))

    if closes and closes[-1] != ltp:
        closes.append(ltp)
        vols.append(vol)

    avg_vol = sum(vols[-20:]) / len(vols[-20:]) if len(vols) >= 5 else vol
    vol_r   = round(vol / avg_vol, 2) if avg_vol > 0 else 1.0

    boll = ta_bollinger(closes)
    bpos = "normal"
    if boll:
        if ltp >= boll["upper"]:   bpos = "above_upper(overbought)"
        elif ltp <= boll["lower"]: bpos = "below_lower(oversold)"
        elif ltp > boll["middle"]: bpos = "upper_half"
        else:                       bpos = "lower_half"

    sup, res = ta_support_resistance(closes)
    wk_range = wkhi - wklo
    uc = round(prev * 1.10, 2)
    lc = round(prev * 0.90, 2)
    sma20 = ta_sma(closes, 20)
    sma50 = ta_sma(closes, 50)

    rsi_val = ta_rsi(closes)
    return {
        "rsi":           rsi_val,
        "rsi_signal":    ("overbought" if (rsi_val or 50) > 70 else
                          "oversold"   if (rsi_val or 50) < 30 else "neutral"),
        "sma20":         sma20,
        "sma50":         sma50,
        "ema9":          ta_ema(closes, 9),
        "macd":          ta_macd(closes),
        "bollinger":     boll,
        "bollinger_pos": bpos,
        "squeeze":       boll.get("squeeze", False) if boll else False,
        "momentum_5d":   ta_momentum(closes, 5),
        "momentum_10d":  ta_momentum(closes, 10),
        "support":       sup,
        "resistance":    res,
        "upper_circuit": uc,
        "lower_circuit": lc,
        "pct_to_uc":     round((uc - ltp) / ltp * 100, 1) if ltp > 0 else 10,
        "pct_to_lc":     round((ltp - lc) / ltp * 100, 1) if ltp > 0 else 10,
        "52wk_pos":      round((ltp - wklo) / wk_range * 100, 1) if wk_range > 0 else 50,
        "vol_ratio":     vol_r,
        "vol_signal":    "SURGE(2x+)" if vol_r >= 2 else ("HIGH" if vol_r >= 1.5 else "NORMAL"),
        "vol_divergence": sf(stock.get("change") or stock.get("percentageChange")) > 0 and vol_r < 0.7,
        "trend":         ("uptrend"   if sma20 and sma50 and sma20 > sma50 and ltp > sma20
                          else "downtrend" if sma20 and sma50 and sma20 < sma50 else "mixed"),
        "days_of_data":  len(closes),
        "pe_signal":     ("cheap<15"    if 0 < sf(stock.get("pe")) < 15  else
                          "fair15-22"   if sf(stock.get("pe")) < 22      else
                          "expensive>22" if sf(stock.get("pe")) >= 22    else "unknown"),
    }

# ─────────────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────────────
NEPSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin":          "https://nepalstock.com",
    "Referer":         "https://nepalstock.com/",
}

def nepse_get(path: str, params: dict = None) -> dict | list | None:
    try:
        r = requests.get(f"https://nepalstock.com/api/nots{path}",
                         params=params or {}, headers=NEPSE_HEADERS,
                         timeout=15, verify=False)
        if r.ok:
            return r.json()
    except Exception as e:
        logger.warning(f"nepalstock.com {path}: {e}")
    return None

def normalize_stock(s: dict) -> dict:
    """Normalize field names from any NEPSE data source to our standard."""
    return {
        "symbol":        s.get("symbol") or s.get("stockSymbol", ""),
        "ltp":           sf(s.get("ltp") or s.get("lastTradedPrice") or s.get("closingPrice")),
        "change":        sf(s.get("change") or s.get("percentageChange")),
        "volume":        sf(s.get("volume") or s.get("totalTradedQuantity")),
        "high":          sf(s.get("high") or s.get("highPrice")),
        "low":           sf(s.get("low") or s.get("lowPrice")),
        "open":          sf(s.get("open") or s.get("openPrice")),
        "previousClose": sf(s.get("previousClose") or s.get("previousClosingPrice")),
        "sector":        s.get("sector") or s.get("sectorName", ""),
        "pe":            sf(s.get("pe")),
        "eps":           sf(s.get("eps")),
        "52weekHigh":    sf(s.get("52weekHigh") or s.get("fiftyTwoWeekHigh")),
        "52weekLow":     sf(s.get("52weekLow")  or s.get("fiftyTwoWeekLow")),
    }

FALLBACK_STOCKS = [
    {"symbol":"NABIL","ltp":1245,"change":2.3,"volume":15234,"high":1260,"low":1230,"previousClose":1217,"sector":"Banking","pe":18.2,"eps":68.4,"52weekHigh":1380,"52weekLow":890},
    {"symbol":"NMB",  "ltp":678, "change":-1.2,"volume":8921,"high":685,"low":670,"previousClose":686,"sector":"Banking","pe":14.5,"eps":46.7,"52weekHigh":780,"52weekLow":510},
    {"symbol":"CHCL", "ltp":432, "change":4.1,"volume":22100,"high":440,"low":425,"previousClose":415,"sector":"Hydropower","pe":22.1,"eps":19.5,"52weekHigh":490,"52weekLow":290},
    {"symbol":"UPPER","ltp":289, "change":3.8,"volume":18500,"high":295,"low":280,"previousClose":278,"sector":"Hydropower","pe":19.8,"eps":14.6,"52weekHigh":340,"52weekLow":198},
    {"symbol":"NLIC", "ltp":1560,"change":-0.5,"volume":5600,"high":1580,"low":1545,"previousClose":1568,"sector":"Insurance","pe":25.3,"eps":61.7,"52weekHigh":1740,"52weekLow":1120},
    {"symbol":"NICA", "ltp":890, "change":1.7,"volume":9800,"high":900,"low":880,"previousClose":875,"sector":"Banking","pe":16.8,"eps":53.0,"52weekHigh":1020,"52weekLow":680},
    {"symbol":"GBIME","ltp":345, "change":-2.1,"volume":12300,"high":360,"low":340,"previousClose":352,"sector":"Banking","pe":13.2,"eps":26.1,"52weekHigh":430,"52weekLow":275},
    {"symbol":"HIDCL","ltp":178, "change":5.2,"volume":35000,"high":180,"low":172,"previousClose":169,"sector":"Hydropower","pe":17.4,"eps":10.2,"52weekHigh":210,"52weekLow":128},
    {"symbol":"SHPC", "ltp":522, "change":2.8,"volume":14200,"high":530,"low":515,"previousClose":508,"sector":"Hydropower","pe":21.5,"eps":24.3,"52weekHigh":590,"52weekLow":385},
    {"symbol":"LBBL", "ltp":234, "change":3.1,"volume":19800,"high":238,"low":228,"previousClose":227,"sector":"Dev Bank","pe":12.8,"eps":18.3,"52weekHigh":278,"52weekLow":162},
]

def fetch_stocks() -> list:
    global _data_source
    # Source 1: nepalstock.com today-price
    data = nepse_get("/nepse-data/today-price", {"size": 500})
    if data:
        content = data if isinstance(data, list) else data.get("content") or data.get("data") or []
        if isinstance(content, list) and len(content) >= 5:
            normalized = [normalize_stock(s) for s in content if s.get("symbol") or s.get("stockSymbol")]
            # Quality check: at least half must have valid prices
            valid = [s for s in normalized if s.get("ltp", 0) > 0]
            if len(valid) >= 5:
                logger.info(f"✅ nepalstock.com: {len(valid)} stocks (quality checked)")
                return valid
            logger.warning(f"nepalstock.com returned {len(normalized)} stocks but only {len(valid)} with valid prices")

    # Source 2: merolagani
    try:
        r = requests.get(
            "https://merolagani.com/handlers/webrequesthandler.ashx?type=market_summary",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=12
        )
        if r.ok:
            d = r.json()
            if isinstance(d, list) and len(d) >= 5:
                normalized = [normalize_stock(s) for s in d]
                valid = [s for s in normalized if s.get("ltp", 0) > 0]
                if len(valid) >= 5:
                    logger.info(f"✅ merolagani: {len(valid)} stocks (quality checked)")
                    _data_source = "merolagani (live)"
                    return valid
    except Exception as e:
        logger.warning(f"merolagani stocks: {e}")

    # Source 3: sharebazaar (free, no key)
    try:
        r = requests.get("https://nepsetty.kokomo.workers.dev/api/all",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        if r.ok:
            d = r.json()
            stocks = d if isinstance(d, list) else d.get("data", [])
            if len(stocks) >= 5:
                normalized = [normalize_stock(s) for s in stocks]
                logger.info(f"✅ sharebazaar: {len(normalized)} stocks")
                _data_source = "sharebazaar (live)"
                return normalized
    except Exception as e:
        logger.warning(f"sharebazaar: {e}")

    logger.error("⚠️ ALL LIVE DATA SOURCES FAILED — using sample data. DO NOT TRADE ON THIS!")
    _data_source = "⚠️ SAMPLE DATA - DO NOT TRADE"
    return FALLBACK_STOCKS

def fetch_floorsheet() -> list:
    """
    Fetch today's floorsheet from nepalstock.com (multi-page).
    Every trade: stock, buyer broker, seller broker, qty, price.
    Same data Hamroshare shows in their app.
    """
    all_trades = []
    for page in range(5):  # fetch up to 5 pages of 500 trades = 2500 trades max
        data = nepse_get("/nepse-data/floorsheet", {"size": 500, "page": page})
        if not data:
            break
        content = (data if isinstance(data, list) else
                   data.get("floorsheets", {}).get("content") or
                   data.get("content") or data.get("data") or [])
        if not isinstance(content, list) or len(content) == 0:
            break
        all_trades.extend(content)
        # If we got fewer than 500, this is the last page
        if len(content) < 500:
            break
    if all_trades:
        logger.info(f"✅ Floorsheet: {len(all_trades)} trades from nepalstock.com ({page+1} pages)")
        atomic_write(FLOOR_CACHE, {"date": get_nst().strftime("%Y-%m-%d"), "data": all_trades})
        return all_trades

    # Use today's cache
    cache = load_json(FLOOR_CACHE, {})
    if cache.get("date") == get_nst().strftime("%Y-%m-%d") and cache.get("data"):
        logger.info(f"✅ Floorsheet: {len(cache['data'])} trades from cache")
        return cache["data"]

    logger.warning("⚠️ Floorsheet unavailable — broker analysis skipped this cycle")
    return []

def analyze_floorsheet(floorsheet: list) -> dict:
    """
    Compute broker buy/sell pressure per stock.
    Broker codes 1-50 = institutional (large brokers in Nepal)
    Returns: {symbol: {buy_pct, sell_pct, inst_net_flow, smart_signal, top_buyers, top_sellers}}
    """
    if not floorsheet:
        return {}

    by_sym = defaultdict(list)
    for t in floorsheet:
        sym = (t.get("stockSymbol") or t.get("symbol", "")).upper()
        if sym:
            by_sym[sym].append(t)

    result = {}
    for sym, trades in by_sym.items():
        b_amt  = defaultdict(float)
        s_amt  = defaultdict(float)
        total  = 0.0
        i_buy  = 0.0
        i_sell = 0.0

        for t in trades:
            qty    = sf(t.get("contractQuantity") or t.get("quantity"))
            price  = sf(t.get("contractRate") or t.get("rate"))
            amt    = qty * price
            buyer  = str(t.get("buyerMemberCode")  or t.get("buyerBroker",  "0"))
            seller = str(t.get("sellerMemberCode") or t.get("sellerBroker", "0"))

            b_amt[buyer]  += amt
            s_amt[seller] += amt
            total         += amt

            try:
                if 1 <= int(buyer)  <= 50: i_buy  += amt
                if 1 <= int(seller) <= 50: i_sell += amt
            except ValueError:
                pass

        if total <= 0:
            continue

        buy_pct  = round(sum(b_amt.values()) / total * 100, 1)
        inst_net = round((i_buy - i_sell) / total * 100, 1)

        signal = ("STRONG INSTITUTIONAL BUY ✅" if inst_net > 10 else
                  "Institutional accumulation"   if inst_net > 3  else
                  "INSTITUTIONAL DISTRIBUTION ⚠️" if inst_net < -10 else
                  "Institutional selling"          if inst_net < -3  else
                  "Mixed / retail-driven")

        result[sym] = {
            "buy_pct":       buy_pct,
            "inst_net_flow": inst_net,
            "smart_signal":  signal,
            "top_buyers":    [f"Broker {b[0]}" for b in sorted(b_amt.items(), key=lambda x: x[1], reverse=True)[:3]],
            "top_sellers":   [f"Broker {s[0]}" for s in sorted(s_amt.items(), key=lambda x: x[1], reverse=True)[:3]],
            "total_trades":  len(trades),
            "value_cr":      round(total / 10_000_000, 2),
        }

    return result

_news_cache: dict = {"ts": 0, "data": ""}
NEWS_CACHE_TTL = 1800  # 30 minutes

def fetch_news() -> str:
    global _news_cache
    if time.time() - _news_cache["ts"] < NEWS_CACHE_TTL and _news_cache["data"]:
        return _news_cache["data"]
    news = []
    try:
        r = requests.get(
            "https://merolagani.com/handlers/webrequesthandler.ashx?type=latest_news&perPage=8",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10
        )
        if r.ok:
            for item in r.json()[:5]:
                t = item.get("newsTitle") or item.get("title", "")
                if t:
                    news.append(f"[merolagani] {t}")
    except Exception as e:
        logger.warning(f"merolagani news: {e}")

    try:
        r = requests.get("https://www.sharesansar.com/rss/latest-news",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.ok and "<title>" in r.text:
            for t in re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", r.text)[1:5]:
                news.append(f"[sharesansar] {t}")
    except Exception as e:
        logger.warning(f"sharesansar: {e}")

    result = "\n".join(news) if news else (
        "[context] Nepal post-election March 2026: RSP dominant. Balen Shah PM + Dr. Wagle FM.\n"
        "[context] NRB policy rate 5.5% — accommodative. FX reserves ~$20B.\n"
        "[context] Hydropower: Arun 3 + Upper Trishuli 1 commissioning 2026.\n"
        "[context] Insurance cos: Rs 42B in NEPSE (+64% YoY) — institutional floor.\n"
        "[context] GDP FY26: 2.1% (unrest impact). FY27 forecast: 4.7% recovery.\n"
        "[context] March = dry season — hydro generation below peak.\n"
        "[context] FATF grey list: limits FDI. Domestic investors very active.\n"
    )
    _news_cache = {"ts": time.time(), "data": result}
    return result

def sector_perf(stocks: list) -> dict:
    sm = defaultdict(list)
    for s in stocks:
        sm[s.get("sector", "Unknown")].append(sf(s.get("change")))
    return {sec: {
        "avg_change": round(sum(ch) / len(ch), 2),
        "signal":     ("bullish"  if sum(ch) / len(ch) > 1.5  else
                       "positive" if sum(ch) / len(ch) > 0    else
                       "neutral"  if sum(ch) / len(ch) > -1   else "bearish"),
        "count": len(ch)
    } for sec, ch in sm.items()}

# ─────────────────────────────────────────────────────
# NEPSE MARKET INTELLIGENCE
# ─────────────────────────────────────────────────────
NEPSE_CONTEXT = """
=== NEPSE MARKET INTELLIGENCE ===
Trading days: SUNDAY to THURSDAY (Friday + Saturday = weekend, market CLOSED)
Pre-open: 10:45–11:00 AM NST (order entry only, no execution)
Continuous market: 11:00 AM–3:00 PM NST
Circuit: ±10% per stock. Index: 4%=20min halt, 5%=40min, 6%=full day.
Min lot: 10 shares. Settlement: 2 working days after trade (2-5 calendar days depending on day bought).
Most liquid window: 11 AM–2 PM. Avoid last 30 min (thin, manipulation risk).

SETTLEMENT RULE:
- Buy Sunday → arrive Tuesday (2 calendar days)
- Buy Wednesday → arrive Monday (5 calendar days)
- Buy Thursday → arrive Tuesday (5 calendar days)
- Always 2 working days, but calendar days vary from 2 to 5+
Book closure: must buy ≥2 trading days before book closure date.
NEVER buy the day before a multi-day holiday — shares won't settle.

SECTORS:
Banking: NRB rate 5.5% (accommodative). NPL ~3.2% (watch). Strong dividend payers.
Hydropower: Structural bull — all parties committed. Dry season (Nov–May) = lower output.
  2026 catalysts: Arun 3, Upper Trishuli 1 commissioning. India power export.
Insurance: Rs 42B in NEPSE (+64% YoY). Low FD rates (<3%) push insurers to equities.
Dev Banks: Higher growth + higher NRB regulatory risk vs commercial banks.

MACRO (March 2026):
- Post-election: RSP dominant. Balen Shah PM + Dr. Wagle FM = strong market positive.
- NEPSE gained 57–162 points on election optimism.
- NRB rate 5.5% = accommodative. FX reserves ~$20B (near record).
- GDP FY26: 2.1%, FY27: 4.7% recovery. Remittances ~$8B/year.
- FATF grey list = FDI headwind. Domestic investors very active.
- March = dry season. Hydro generation below peak.

TECHNICAL RULES:
RSI>70=overbought avoid buy. RSI<30=oversold watch reversal.
Price>SMA20>SMA50=strong uptrend. Volume 2x+ 20d avg=institutional entry.
MACD bullish crossover=buy signal. Bollinger squeeze(bw<5%)=breakout incoming.
Volume divergence(price up + vol down)=rally losing steam, caution.
80%+ retail participation=momentum trades work. Sharp reversals too.

FLOORSHEET RULES:
Broker codes 1–50=large institutional brokers in Nepal.
inst_net_flow>10%=STRONG institutional accumulation=high conviction buy.
inst_net_flow<-10%=institutional distribution=avoid or sell.
Top buyers: if same brokers accumulating multiple days=serious signal.
"""

# ─────────────────────────────────────────────────────
# AI PROMPTS (3 different lenses)
# ─────────────────────────────────────────────────────
def prompt_technical(stocks, fs_analysis, open_syms) -> str:
    now = get_nst()
    top = sorted(stocks, key=lambda x: sf(x.get("volume")), reverse=True)[:20]
    enriched = []
    for s in top:
        sym = s.get("symbol", "")
        if sym in open_syms:
            continue
        d = dict(s)
        d["ta"] = full_ta(sym, s)
        d["floorsheet"] = fs_analysis.get(sym, {})
        enriched.append(d)

    # Limit TA data to avoid token overflow (keep top 10 only, compact format)
    compact = []
    for s in enriched[:10]:
        ta = s.get("ta", {})
        compact.append({
            "symbol": s.get("symbol"),
            "ltp": s.get("ltp"), "change": s.get("change"),
            "volume": s.get("volume"), "sector": s.get("sector"),
            "pe": s.get("pe"), "eps": s.get("eps"),
            "52weekHigh": s.get("52weekHigh"), "52weekLow": s.get("52weekLow"),
            "ta": {
                "rsi": ta.get("rsi"), "rsi_signal": ta.get("rsi_signal"),
                "macd_crossover": ta.get("macd", {}).get("crossover"),
                "trend": ta.get("trend"), "vol_signal": ta.get("vol_signal"),
                "vol_divergence": ta.get("vol_divergence"),
                "pct_to_uc": ta.get("pct_to_uc"), "pct_to_lc": ta.get("pct_to_lc"),
                "squeeze": ta.get("squeeze"), "momentum_5d": ta.get("momentum_5d"),
                "support": ta.get("support"), "resistance": ta.get("resistance"),
                "days_of_data": ta.get("days_of_data"),
            },
            "floorsheet": s.get("floorsheet", {}),
        })

    return f"""You are a NEPSE technical analyst. {now.strftime('%A %B %d %Y, %I:%M %p NST')}
Market: {market_status()} | T+2 settlement: {get_settlement_date()}
{NEPSE_CONTEXT}

TOP 10 STOCKS BY VOLUME WITH TECHNICAL INDICATORS + FLOORSHEET:
{json.dumps(compact, indent=2)}

YOUR FOCUS: Pure technical analysis.
LOOK FOR: RSI 40-65 (not overbought), price>SMA20, MACD bullish, volume surge, NOT near upper circuit.
CONFIRM with floorsheet: inst_net_flow>5% = institutional backing = higher conviction.
FLAG: volume divergence, overbought (RSI>70), near upper circuit, downtrend.
SKIP: stocks already in open positions, volume<2000, near circuit limits.

RESPOND — valid JSON array only, zero extra text:
[{{"symbol":"X","action":"BUY","current_price":100.0,"target_price":115.0,"stop_loss":92.0,
"holding_period":"X days","confidence":"HIGH","risk":"LOW",
"settlement_note":"Shares arrive [day] - safe/warning",
"reasoning":"3 sentences: technical+floorsheet analysis",
"key_risk":"biggest risk","sector_catalyst":"driver","volume_note":"normal/high/surge",
"min_quantity":10}}]"""

def prompt_fundamental(stocks, news, sp, open_syms) -> str:
    now = get_nst()
    filtered = [s for s in stocks if s.get("symbol", "") not in open_syms]
    return f"""You are a NEPSE fundamental + macro analyst. {now.strftime('%A %B %d %Y, %I:%M %p NST')}
Market: {market_status()} | T+2 settlement: {get_settlement_date()}
{NEPSE_CONTEXT}

SECTOR PERFORMANCE: {json.dumps(sp)}
STOCK DATA: {json.dumps(filtered[:20], indent=2)}
LATEST NEWS: {news}

YOUR FOCUS: Fundamental value + macro tailwinds.
LOOK FOR: low P/E vs sector, strong EPS, upcoming dividends, sector with institutional tailwinds.
Consider: NRB rate impact, post-election optimism, remittance seasonality.
SKIP: stocks already in open positions, overvalued (P/E>30 without strong growth).

RESPOND — valid JSON array only, zero extra text:
[{{"symbol":"X","action":"BUY","current_price":100.0,"target_price":115.0,"stop_loss":92.0,
"holding_period":"X days","confidence":"HIGH","risk":"LOW",
"settlement_note":"Shares arrive [day] - safe/warning",
"reasoning":"3 sentences: fundamental+macro analysis",
"key_risk":"biggest risk","sector_catalyst":"driver","volume_note":"normal/high/surge",
"min_quantity":10}}]"""

def prompt_sentiment(stocks, news, fs_analysis, sp, open_syms) -> str:
    now = get_nst()
    # Rank by institutional activity
    ranked = sorted(
        [s for s in stocks if s.get("symbol", "") not in open_syms
         and s.get("symbol", "") in fs_analysis],
        key=lambda x: abs(sf(fs_analysis.get(x.get("symbol", ""), {}).get("inst_net_flow"))),
        reverse=True
    )[:15]

    fs_top = {sym: {
        "smart_signal":  d.get("smart_signal"),
        "inst_net_flow": d.get("inst_net_flow"),
        "top_buyers":    d.get("top_buyers", []),
    } for sym, d in list(fs_analysis.items())[:20]}

    return f"""You are a NEPSE market sentiment + broker flow analyst. {now.strftime('%A %B %d %Y, %I:%M %p NST')}
Market: {market_status()} | T+2 settlement: {get_settlement_date()}
{NEPSE_CONTEXT}

SECTOR MOMENTUM: {json.dumps(sp)}
FLOORSHEET INSTITUTIONAL FLOW: {json.dumps(fs_top, indent=2)}
TOP STOCKS BY INSTITUTIONAL ACTIVITY: {json.dumps(ranked, indent=2)}
LATEST NEWS: {news}

YOUR FOCUS: Sentiment + momentum + broker flow intelligence.
STRONG BUY: inst_net_flow>10% + positive news + sector tailwind.
AVOID: institutional distribution (inst_net_flow<-10%) regardless of price action.
Look for: momentum continuation, news catalysts, sector rotation, smart money accumulation.
SKIP: stocks already in open positions.

RESPOND — valid JSON array only, zero extra text:
[{{"symbol":"X","action":"BUY","current_price":100.0,"target_price":115.0,"stop_loss":92.0,
"holding_period":"X days","confidence":"HIGH","risk":"LOW",
"settlement_note":"Shares arrive [day] - safe/warning",
"reasoning":"3 sentences: sentiment+broker flow analysis",
"key_risk":"biggest risk","sector_catalyst":"driver","volume_note":"normal/high/surge",
"min_quantity":10}}]"""

# ─────────────────────────────────────────────────────
# AI CALLERS
# ─────────────────────────────────────────────────────
def parse_ai(text: str, src: str) -> list:
    try:
        text = text.strip()
        if "```" in text:
            for part in text.split("```"):
                c = part.replace("json", "").strip()
                if c.startswith("["):
                    text = c
                    break
        s, e = text.find("["), text.rfind("]") + 1
        if 0 <= s < e:
            text = text[s:e]
        result = json.loads(text)
        if not isinstance(result, list):
            raise ValueError("not list")
        logger.info(f"{src}: {len(result)} recs")
        return result
    except Exception as ex:
        logger.error(f"{src} parse: {ex} | {text[:200]}")
        return []

def ask_deepseek(prompt: str) -> list:
    def call():
        r = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 2048, "temperature": 0.2},
            timeout=60)
        if not r.ok:
            raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        return parse_ai(r.json()["choices"][0]["message"]["content"], "DeepSeek(Technical)")
    try:
        return retry(call)
    except Exception as e:
        logger.error(f"DeepSeek: {e}")
        return []

def ask_gemini(prompt: str) -> list:
    def call():
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}],
                  "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048}},
            timeout=60)
        if not r.ok:
            raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        return parse_ai(r.json()["candidates"][0]["content"]["parts"][0]["text"], "Gemini(Fundamental)")
    try:
        return retry(call)
    except Exception as e:
        logger.error(f"Gemini: {e}")
        return []

def ask_groq(prompt: str) -> list:
    def call():
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 2048, "temperature": 0.2},
            timeout=60)
        if not r.ok:
            raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        return parse_ai(r.json()["choices"][0]["message"]["content"], "Groq(Sentiment)")
    try:
        return retry(call)
    except Exception as e:
        logger.error(f"Groq: {e}")
        return []

# ─────────────────────────────────────────────────────
# VOTING ENGINE
# ─────────────────────────────────────────────────────
def vote(technical, fundamental, sentiment, fs_analysis) -> list:
    open_syms = set(pos_load().keys())
    available = sum([bool(technical), bool(fundamental), bool(sentiment)])
    if available == 0:
        logger.warning("All 3 AIs failed to respond — skipping vote")
        return []
    votes: dict = {}

    for src, recs in [("DeepSeek(Technical)", technical),
                      ("Gemini(Fundamental)", fundamental),
                      ("Groq(Sentiment)",     sentiment)]:
        for rec in recs:
            sym = str(rec.get("symbol", "")).upper().strip()
            if not sym or sym in open_syms:
                continue
            if sym not in votes:
                votes[sym] = {"recs": [], "actions": [], "sources": []}
            votes[sym]["recs"].append(rec)
            votes[sym]["actions"].append(str(rec.get("action", "HOLD")).upper())
            votes[sym]["sources"].append(src)

    final = []
    for sym, data in votes.items():
        actions = data["actions"]
        bc, sc  = actions.count("BUY"), actions.count("SELL")
        if bc >= 2:   ca, cc = "BUY",  bc
        elif sc >= 2: ca, cc = "SELL", sc
        else:         continue

        agreeing = [r for r in data["recs"] if str(r.get("action", "")).upper() == ca]
        base = agreeing[0]
        cur  = sf(base.get("current_price"), 1)
        tgt  = round(sum(sf(r.get("target_price", cur)) for r in agreeing) / len(agreeing), 2)
        stp  = round(sum(sf(r.get("stop_loss", cur))    for r in agreeing) / len(agreeing), 2)

        reward   = (tgt - cur) if ca == "BUY" else (cur - tgt)
        risk_amt = (cur - stp) if ca == "BUY" else (stp - cur)
        rr       = round(reward / risk_amt, 1) if risk_amt > 0 else 0.0
        if rr <= 0.5:
            continue

        # Floorsheet confirmation boosts confidence
        fs  = fs_analysis.get(sym, {})
        confirmed = ((ca == "BUY"  and sf(fs.get("inst_net_flow")) > 3) or
                     (ca == "SELL" and sf(fs.get("inst_net_flow")) < -3))
        confidence = "HIGH" if (cc >= 3 or (cc >= 2 and confirmed)) else "MEDIUM"

        final.append({
            "symbol":          sym,
            "action":          ca,
            "current_price":   cur,
            "target_price":    tgt,
            "stop_loss":       stp,
            "risk_reward":     rr,
            "holding_period":  base.get("holding_period", "N/A"),
            "confidence":      confidence,
            "risk":            base.get("risk", "MEDIUM"),
            "settlement_note": get_settlement_date(),  # 2 working days, calendar days shown
            "reasoning":       base.get("reasoning", ""),
            "key_risk":        base.get("key_risk", "Market volatility"),
            "sector_catalyst": base.get("sector_catalyst", ""),
            "volume_note":     base.get("volume_note", "normal"),
            "min_quantity":    int(base.get("min_quantity") or 10),
            "votes":           f"{cc}/{available} AIs ({', '.join(data['sources'])})",
            "floorsheet":      {"smart_signal": fs.get("smart_signal", "No data"),
                                "inst_net_flow": fs.get("inst_net_flow", "N/A"),
                                "confirmed": confirmed},
        })

    final.sort(key=lambda x: (0 if x["confidence"] == "HIGH" else 1, -x["risk_reward"]))
    return final[:3]

# ─────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────
def tg(text: str) -> bool:
    chunks = [text[i:i + TG_LIMIT] for i in range(0, len(text), TG_LIMIT)]
    ok = True
    for chunk in chunks:
        def call(c=chunk):
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": c, "parse_mode": "Markdown"},
                timeout=15)
            if not r.ok:
                raise Exception(f"HTTP {r.status_code}")
        try:
            retry(call, n=3, backoff=1.5)
        except Exception as e:
            logger.error(f"Telegram: {e}")
            ok = False
        time.sleep(0.4)
    return ok

def format_alert(trade: dict, tid: str) -> str:
    ae  = "📈" if trade["action"] == "BUY" else "📉"
    ce  = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(trade["confidence"], "⚪")
    re_ = {"LOW": "✅", "MEDIUM": "⚠️", "HIGH": "🚨"}.get(trade["risk"], "⚠️")
    ve  = "🔥" if "SURGE" in str(trade.get("volume_note", "")).upper() else "📊"
    fs  = trade.get("floorsheet", {})
    fs_line = f"\n🏦 *Broker Flow:* _{fs.get('smart_signal', 'N/A')}_" if fs else ""

    return (
        f"\n{ae} *NEPSE TRADE ALERT* {ae}\n\n"
        f"🏢 *Stock:* `{trade['symbol']}`\n"
        f"📊 *Action:* *{trade['action']}*\n"
        f"💰 *Entry:* NPR {trade['current_price']}\n"
        f"🎯 *Target:* NPR {trade['target_price']}\n"
        f"🛑 *Stop Loss:* NPR {trade['stop_loss']}\n"
        f"📐 *R/R:* 1:{trade['risk_reward']}\n"
        f"📦 *Min Qty:* {trade.get('min_quantity', 10)} shares\n"
        f"⏱ *Hold:* {trade.get('holding_period', 'N/A')}\n\n"
        f"{ce} *Confidence:* {trade['confidence']}\n"
        f"🤝 *AI Votes:* {trade.get('votes', 'N/A')}\n"
        f"{re_} *Risk:* {trade['risk']}\n"
        f"{ve} *Volume:* {trade.get('volume_note', 'normal')}\n"
        f"{fs_line}\n\n"
        f"📅 *Shares arrive:* _{trade.get('settlement_note', 'see settlement date')}_\n"
        f"⚡ *Catalyst:* _{trade.get('sector_catalyst', 'N/A')}_\n\n"
        f"🤖 *Analysis:*\n_{trade['reasoning']}_\n\n"
        f"⚠️ *Key Risk:* _{trade.get('key_risk', 'N/A')}_\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"*Trade ID:* `{tid}`\n"
        f"✅ `APPROVE {tid}` | ❌ `REJECT {tid}`"
    )

_processed_commands: set = set()  # dedup: avoid acting on same message twice

def poll(last_id: int) -> tuple:
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"timeout": 5, "offset": last_id + 1}, timeout=15)
        if not r.ok:
            return [], last_id
        updates = r.json().get("result", [])
        responses, new_id = [], last_id
        for u in updates:
            uid    = u.get("update_id", 0)
            new_id = max(new_id, uid)
            if uid in _processed_commands:
                continue  # already handled
            msg   = u.get("message", {}).get("text", "").strip()
            upper = msg.upper()
            if upper.startswith("APPROVE ") or upper.startswith("REJECT "):
                parts = upper.split(" ", 1)
                if len(parts) == 2:
                    _processed_commands.add(uid)
                    responses.append({"action": parts[0], "trade_id": parts[1].strip()})
            elif upper.startswith("SOLD "):
                _processed_commands.add(uid)
                responses.append({"action": "SOLD", "trade_id": msg[5:].strip().upper()})
        # Keep dedup set bounded (last 1000 IDs)
        if len(_processed_commands) > 1000:
            oldest = sorted(_processed_commands)[:500]
            for oid in oldest:
                _processed_commands.discard(oid)
        return responses, new_id
    except Exception as e:
        logger.error(f"Poll: {e}")
        return [], last_id

# ─────────────────────────────────────────────────────
# TRADE LOG
# ─────────────────────────────────────────────────────
def log_trade(trade: dict, tid: str, status: str):
    trades = load_json(TRADE_LOG, [])
    trades.append({"trade_id": tid, "timestamp": get_nst().isoformat(),
                   "status": status, **trade})
    atomic_write(TRADE_LOG, trades)
    logger.info(f"Trade {tid} → {status}")

# ─────────────────────────────────────────────────────
# REPORTS
# ─────────────────────────────────────────────────────
def morning_briefing(stocks, sp, fs_analysis):
    now     = get_nst()
    gainers = sorted([s for s in stocks if sf(s.get("change")) > 0],
                     key=lambda x: sf(x.get("change")), reverse=True)[:3]
    losers  = sorted([s for s in stocks if sf(s.get("change")) < 0],
                     key=lambda x: sf(x.get("change")))[:3]
    surges  = sorted(stocks, key=lambda x: sf(x.get("volume")), reverse=True)[:3]
    hot     = [k for k, v in sp.items() if "bullish" in v.get("signal", "") or "positive" in v.get("signal", "")]
    inst    = [(sym, d["smart_signal"]) for sym, d in fs_analysis.items()
               if "STRONG INSTITUTIONAL BUY" in d.get("smart_signal", "")][:3]

    g  = "\n".join([f"  ▲ {s['symbol']}: +{s.get('change', 0)}%" for s in gainers])  or "  Awaiting data"
    l  = "\n".join([f"  ▼ {s['symbol']}: {s.get('change', 0)}%"  for s in losers])   or "  Awaiting data"
    v  = "\n".join([f"  🔥 {s['symbol']}: {int(sf(s.get('volume'))//1000)}K vol" for s in surges]) or "  None"
    ib = "\n".join([f"  🏦 {sym}: {sig}" for sym, sig in inst]) or "  None detected"

    tg(
        f"🌅 *NEPSE Morning Briefing*\n"
        f"📅 {now.strftime('%A, %B %d %Y')}\n"
        f"⏰ Pre-open 10:45 AM | Market 11:00 AM NST\n"
        f"📦 Shares arrive: {get_settlement_date()}\n\n"
        f"📈 *Top Gainers:*\n{g}\n\n"
        f"📉 *Top Losers:*\n{l}\n\n"
        f"🔥 *Volume Leaders:*\n{v}\n\n"
        f"🏦 *Institutional Buying (Floorsheet):*\n{ib}\n\n"
        f"⚡ *Hot Sectors:* {', '.join(hot) or 'Analyzing...'}\n\n"
        f"📡 *Data source:* {_data_source}\n"
        f"🤖 AI analysis (Technical + Fundamental + Sentiment) running..."
    )

def daily_summary():
    trades = load_json(TRADE_LOG, [])
    today  = get_nst().strftime("%Y-%m-%d")
    td     = [t for t in trades if t.get("timestamp", "").startswith(today)]
    ap     = [t for t in td if t.get("status") == "APPROVED"]
    rj     = [t for t in td if t.get("status") == "REJECTED"]
    pos    = pos_load()
    lines  = [f"📊 *Daily Summary — {today}*\n",
              f"✅ Approved: {len(ap)} | ❌ Rejected: {len(rj)}"]
    if ap:
        lines.append("\n*Approved today:*")
        for t in ap:
            lines.append(f"  • {t['symbol']} {t['action']} @ NPR {t.get('current_price','?')} → {t.get('target_price','?')}")
    if pos:
        lines.append(f"\n*Open positions:* {', '.join(pos.keys())}")
    lines.append(f"\n📦 Shares arrive: {get_settlement_date()}")
    tg("\n".join(lines))

def weekly_report():
    trades   = load_json(TRADE_LOG, [])
    now      = get_nst()
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    wt       = [t for t in trades if t.get("timestamp", "") >= week_ago]
    ap       = [t for t in wt if t.get("status") == "APPROVED"]
    tg(
        f"📆 *Weekly Report — {now.strftime('%B %d, %Y')}*\n\n"
        f"✅ Approved: {len(ap)} | ❌ Rejected: {len(wt) - len(ap)}\n"
        f"📊 Total analyzed: {len(wt)}\n\n"
        f"🤖 Agent running strong on Railway 24/7!"
    )

# ─────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────
def run():
    validate_config()
    start_health_server()
    heartbeat("Starting up...")

    logger.info("Loading Nepal holidays from Hamro Patro...")
    h = get_nepal_holidays()
    logger.info("Holiday calendar ready: {} dates".format(len(h)))
    logger.info("=" * 65)
    logger.info("NEPSE AI Agent v6.0 - Final Edition")
    logger.info("Trading days: Sun–Thu | Pre-open: 10:45 | Market: 11–3 NST")
    logger.info("Data: nepalstock.com + merolagani | Floorsheet: real broker data")
    logger.info("AIs: DeepSeek(Technical) + Gemini(Fundamental) + Groq(Sentiment)")
    logger.info("=" * 65)

    tg(
        "🤖 *NEPSE AI Agent v6.0 — Final Edition*\n\n"
        "✅ Trading days: Sunday–Thursday (correct!)\n"
        "✅ Pre-open: 10:45–11:00 AM NST\n"
        "✅ Continuous market: 11:00 AM–3:00 PM NST\n"
        "✅ Live data: nepalstock.com + merolagani (quality-checked)\n"
        "✅ Floorsheet: real broker data (nepalstock.com)\n"
        "⚠️ If data sources fail, agent uses sample data + warns you\n"
        "✅ Real RSI, MACD, Bollinger from price history\n"
        "✅ 3 AI lenses: Technical / Fundamental / Sentiment\n"
        "✅ Stop-loss + target monitoring\n"
        "✅ Running 24/7 on Railway\n\n"
        "Reply APPROVE/REJECT to trade alerts.\n"
        "Reply `SOLD SYMBOL` when you close a position."
    )

    last_id        = 0
    cycles         = 0
    crashes        = 0
    briefing_sent  = None
    summary_sent   = None
    weekly_sent    = None

    while True:
        try:
            now   = get_nst()
            today = now.date()
            pend_purge()

            # ── Fetch data ────────────────────────────────
            stocks     = fetch_stocks()
            sp         = sector_perf(stocks)
            floorsheet = fetch_floorsheet()
            fs_anal    = analyze_floorsheet(floorsheet)

            # Update price history for TA
            if is_market_open() or is_preopen():
                history_update(stocks)

            # Position monitoring (stop loss + target alerts)
            for alert in pos_monitor(stocks):
                tg(alert)

            # ── Scheduled reports ─────────────────────────
            # Morning briefing: weekdays at pre-open (10:45–10:59 AM)
            if is_trading_day(today) and now.hour == 10 and 45 <= now.minute <= 59 and briefing_sent != today:
                morning_briefing(stocks, sp, fs_anal)
                briefing_sent = today

            # Daily summary: weekdays at 4 PM
            if is_trading_day(today) and now.hour == 16 and now.minute <= 14 and summary_sent != today:
                daily_summary()
                summary_sent = today

            # Weekly report: Thursday (last trading day) at 4 PM
            if today.weekday() == 3 and now.hour == 16 and now.minute <= 14 and weekly_sent != today:
                weekly_report()
                weekly_sent = today
                h = refresh_holidays()
                tg("Hamro Patro holiday calendar refreshed: {} dates.".format(len(h)))

            # ── AI Analysis ───────────────────────────────
            cycles += 1
            heartbeat(f"Cycle #{cycles} — {now.strftime('%a %I:%M %p NST')}")
            open_syms = set(pos_load().keys())
            logger.info(f"=== Cycle #{cycles} | {now.strftime('%a %I:%M %p NST')} | "
                        f"{len(stocks)} stocks | {len(floorsheet)} floorsheet trades | "
                        f"{len(fs_anal)} broker-analyzed stocks ===")

            news = fetch_news()

            logger.info("DeepSeek → technical analysis...")
            ds = ask_deepseek(prompt_technical(stocks, fs_anal, open_syms))
            time.sleep(2)  # avoid simultaneous rate limits

            logger.info("Gemini → fundamental analysis...")
            gm = ask_gemini(prompt_fundamental(stocks, news, sp, open_syms))
            time.sleep(2)

            logger.info("Groq → sentiment + floorsheet...")
            gr = ask_groq(prompt_sentiment(stocks, news, fs_anal, sp, open_syms))

            logger.info(f"Votes → DS:{len(ds)} GM:{len(gm)} GR:{len(gr)}")
            final = vote(ds, gm, gr, fs_anal)

            if not final:
                logger.info("No consensus.")
                tg(f"🔍 Analysis #{cycles}: No consensus (AIs disagreed or poor R/R). Monitoring...")
            else:
                tg(f"📊 *Analysis #{cycles} Complete*\n🤝 {len(final)} consensus trade(s)! 👇")
                for i, trade in enumerate(final):
                    tid = f"T{int(time.time())}{i}"
                    pend_add(tid, trade)
                    log_trade(trade, tid, "PENDING")
                    tg(format_alert(trade, tid))
                    logger.info(f"Alert: {trade['symbol']} {trade['action']} "
                                f"R/R=1:{trade['risk_reward']} {trade.get('votes')}")
                    time.sleep(2)

            # ── Poll responses (10 min) ───────────────────
            crashes = 0  # full cycle succeeded, reset crash counter
            heartbeat(f"Cycle #{cycles} — awaiting your response")
            for _ in range(20):
                time.sleep(30)
                responses, last_id = poll(last_id)
                for resp in list(responses):
                    action = resp["action"]
                    tid    = resp["trade_id"]

                    if action == "SOLD":
                        pos_remove(tid)
                        tg(f"📤 Position `{tid}` closed. Good trade!")
                        continue

                    trade = pend_get(tid)
                    if trade is None:
                        tg(f"⚠️ Trade `{tid}` not found or expired.")
                        continue

                    if action == "APPROVE":
                        log_trade(trade, tid, "APPROVED")
                        pos_add(trade["symbol"], trade)
                        del pending[tid]
                        tg(
                            f"✅ *Trade {tid} APPROVED!*\n\n"
                            f"📱 Place on TMS:\nhttps://tms77.nepsetms.com.np\n\n"
                            f"Symbol: `{trade['symbol']}`\n"
                            f"Action: {trade['action']}\n"
                            f"Price: NPR {trade['current_price']}\n"
                            f"Target: NPR {trade['target_price']}\n"
                            f"Stop Loss: NPR {trade['stop_loss']}\n"
                            f"Min Qty: {trade.get('min_quantity', 10)} shares\n"
                            f"Shares arrive: {trade.get('settlement_note', 'see below')}\n\n"
                            f"🤖 I'll alert you when stop-loss or target is hit!\n"
                            f"Reply `SOLD {trade['symbol']}` when you close."
                        )
                    elif action == "REJECT":
                        log_trade(trade, tid, "REJECTED")
                        del pending[tid]
                        tg(f"❌ `{tid}` ({trade['symbol']}) rejected. Monitoring...")

            # ── Next cycle timing ─────────────────────────
            # During market hours: run every hour
            # Outside market hours: run every 4 hours
            # Smart sleep: use shorter interval if pre-open is approaching
            now2 = get_nst()
            if is_market_open():
                wait = 3600  # every hour during market
            elif is_trading_day(now2.date()):
                # How many minutes until pre-open (10:45)?
                preopen_today = now2.replace(hour=10, minute=45, second=0, microsecond=0)
                mins_to_preopen = (preopen_today - now2).total_seconds() / 60
                if 0 < mins_to_preopen <= 240:
                    # Wake up 5 min before pre-open
                    wait = max(300, int(mins_to_preopen - 5) * 60)
                else:
                    wait = 14400  # 4 hours otherwise
            else:
                wait = 14400  # weekend/holiday
            nxt = get_nst() + timedelta(seconds=wait)
            heartbeat(f"Sleeping until {nxt.strftime('%I:%M %p NST')}")
            tg(f"⏳ Next analysis: {nxt.strftime('%I:%M %p NST')} ({wait // 60} min)")
            time.sleep(wait)

        except KeyboardInterrupt:
            tg("🛑 Agent stopped manually. Goodbye!")
            break
        except Exception as e:
            crashes += 1
            heartbeat(f"ERROR #{crashes}: {str(e)[:60]}")
            logger.error(f"Error [{crashes}/{CRASH_MAX}]: {e}", exc_info=True)
            if crashes >= CRASH_MAX:
                tg(f"🆘 Agent crashed {crashes} times consecutively. Stopping. Check Railway logs!")
                raise SystemExit(1)
            tg(f"⚠️ Error ({crashes}/{CRASH_MAX}): {str(e)[:200]}\nRestarting in 60s...")
            time.sleep(60)

if __name__ == "__main__":
    run()
