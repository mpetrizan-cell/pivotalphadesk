"""
PivotAlphaDesk - GAIA NVDA Backend v1
ts_gaia_nvda.py

Options flow processor for NVDA — single stock.
Designed for earnings events with elevated IV.

Layers:
  Layer 1: 0DTE/nearest  → stream cada 5s
  Layer 2: Weekly        → REST cada 60s
  Layer 3: Monthly       → REST cada 300s

JSON output: gaia_nvda_live.json
Railway push: /push_nvda
"""

import json, os, time, urllib.parse, urllib.request
import http.client, ssl, logging
from datetime import datetime, timedelta
from collections import deque

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
TS_CLIENT_ID     = "HMVux6j6ncGeYOVFbWVXyB0lSVL4WWWe"
TS_CLIENT_SECRET = "2Y4SKDlCN0PMX6wbwWLRvcPNeaA7Zl1ygJoSFO9XWWvsCP37xXrF9RzCUBjaddIx"
TOKEN_FILE       = "ts_tokens.json"          # comparte tokens con SPX backend
TOKEN_URL        = "https://signin.tradestation.com/oauth/token"
API_BASE         = "https://api.tradestation.com/v3"
OUTPUT_FILE      = "gaia_nvda_live.json"
HISTORY_FILE     = "gaia_nvda_history_intraday.json"
LOG_FILE         = "gaia_nvda_live.log"

NVDA_SYMBOL      = "NVDA"                    # opciones sobre acción NVDA
STOCK_SYMBOL     = "NVDA"                    # precio spot del subyacente
STRIKE_PROXIMITY = 25                        # ±25 strikes alrededor del spot
                                             # NVDA ~$221, strikes cada $2.50/$5

# Refresh diferenciado por capa
REFRESH_0DTE     = 5
REFRESH_WEEKLY   = 60
REFRESH_MONTHLY  = 300

DHP_HISTORY_SIZE = 10

# ── RAILWAY ───────────────────────────────────────────────────────────────────
RAILWAY_URL   = "https://web-production-49e7.up.railway.app"
RAILWAY_TOKEN = "gaia_push_secret_2026"
PUSH_ENDPOINT = "/push_nvda"                 # endpoint dedicado NVDA

# ── CONSTANTES ────────────────────────────────────────────────────────────────
MAX_TOKEN_RETRIES = 5
TOKEN_RETRY_WAIT  = 60
TOKEN_LONGWAIT    = 300
MAX_CYCLE_ERRORS  = 10
CYCLE_ERROR_WAIT  = 120

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NVDA][%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── TOKEN MANAGEMENT (comparte con SPX) ───────────────────────────────────────
def load_tokens():
    try:
        with open(TOKEN_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}

def save_tokens(tokens):
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump(tokens, f)
    except Exception as e:
        log.warning(f"No se pudo guardar token: {e}")

def is_token_valid(tokens):
    exp = tokens.get("expires_at", 0)
    return time.time() < exp - 60

def refresh_token(tokens):
    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "client_id":     TS_CLIENT_ID,
        "client_secret": TS_CLIENT_SECRET,
        "refresh_token": tokens.get("refresh_token", "")
    }).encode()
    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as r:
        new_tokens = json.loads(r.read())
    new_tokens["expires_at"] = time.time() + new_tokens.get("expires_in", 1200) - 60
    save_tokens(new_tokens)
    log.info("Token NVDA refreshed OK")
    return new_tokens

def get_token_with_retry():
    for attempt in range(MAX_TOKEN_RETRIES):
        try:
            tokens = load_tokens()
            if is_token_valid(tokens):
                return tokens.get("access_token")
            log.info(f"Refreshing token (attempt {attempt+1})")
            tokens = refresh_token(tokens)
            return tokens.get("access_token")
        except Exception as e:
            log.error(f"Token error attempt {attempt+1}: {e}")
            wait = TOKEN_RETRY_WAIT if attempt < 3 else TOKEN_LONGWAIT
            time.sleep(wait)
    return None

def safe_sleep(secs):
    try:
        time.sleep(secs)
    except KeyboardInterrupt:
        raise

# ── SPOT PRICE — NVDA stock ───────────────────────────────────────────────────
def get_nvda_spot(token):
    """Get NVDA stock price via TradeStation quotes API."""
    try:
        url = f"{API_BASE}/marketdata/quotes/{STOCK_SYMBOL}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        quotes = data.get("Quotes", [])
        if quotes:
            price = float(quotes[0].get("Last", 0) or quotes[0].get("Close", 0))
            log.info(f"NVDA spot: {price}")
            return price
    except Exception as e:
        log.warning(f"Error getting NVDA spot: {e}")
    return 0.0

# ── EXPIRATIONS ───────────────────────────────────────────────────────────────
def get_nvda_expirations(token):
    layers = {"0dte": "", "weekly": "", "monthly": ""}
    try:
        sym_enc = urllib.parse.quote(NVDA_SYMBOL, safe="")
        url = f"{API_BASE}/marketdata/options/expirations/{sym_enc}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())

        expirations = data.get("Expirations", [])
        today       = datetime.utcnow().date()
        week_end    = today + timedelta(days=7)
        month_end   = today + timedelta(days=45)

        for exp in expirations:
            date_str = exp.get("Date", "")[:10]
            try:
                exp_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if exp_date >= today and not layers["0dte"]:
                layers["0dte"] = date_str
                log.info(f"NVDA nearest exp: {date_str}")
            elif exp_date <= week_end and exp_date > today and not layers["weekly"]:
                layers["weekly"] = date_str
                log.info(f"NVDA weekly exp: {date_str}")
            elif exp_date <= month_end and exp_date > week_end and not layers["monthly"]:
                layers["monthly"] = date_str

            if all(layers.values()):
                break

        # Fallbacks
        if not layers["0dte"] and expirations:
            layers["0dte"] = expirations[0].get("Date", "")[:10]
        if not layers["weekly"] and len(expirations) > 1:
            layers["weekly"] = expirations[1].get("Date", "")[:10]
        if not layers["monthly"] and len(expirations) > 2:
            layers["monthly"] = expirations[2].get("Date", "")[:10]

    except Exception as e:
        log.error(f"Error getting NVDA expirations: {e}")

    return layers

# ── STREAM OPTIONS CHAIN ──────────────────────────────────────────────────────
def read_stream_nvda(token, expiration, spot):
    """Stream NVDA options chain for a given expiration."""
    strikes = {}
    if not expiration:
        return strikes

    params = "?" + urllib.parse.urlencode({
        "expiration":      expiration,
        "strikeProximity": STRIKE_PROXIMITY,
    })
    sym_enc = urllib.parse.quote(NVDA_SYMBOL, safe="")
    url = f"/v3/marketdata/stream/options/chains/{sym_enc}{params}"
    conn = None

    try:
        conn = http.client.HTTPSConnection(
            "api.tradestation.com",
            context=ssl.create_default_context(),
            timeout=20
        )
        conn.request("GET", url, headers={
            "Authorization": "Bearer " + token,
            "Accept":        "application/json"
        })
        resp = conn.getresponse()
        if resp.status != 200:
            log.error(f"Stream NVDA status: {resp.status}")
            return strikes

        lines_read    = 0
        max_contracts = STRIKE_PROXIMITY * 2 * 2 + 10
        heartbeats    = 0
        max_heartbeat = 8

        while lines_read < max_contracts and heartbeats < max_heartbeat:
            try:
                raw = resp.readline().decode("utf-8").strip()
            except Exception as e:
                log.warning(f"Stream NVDA readline error: {e}")
                break
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            if "Heartbeat" in data:
                heartbeats += 1
                continue

            strikes = _parse_option_line(data, strikes)
            if lines_read == 0:
                log.info(f"NVDA stream sample fields: {list(data.keys())}")
                log.info(f"NVDA OI fields: DailyOI={data.get('DailyOpenInterest')} OI={data.get('OpenInterest')} TotalVol={data.get('TotalVolume')}")
            lines_read += 1

    except Exception as e:
        log.error(f"Stream NVDA error: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return strikes

def _parse_option_line(data, strikes):
    side   = data.get("Side", "")
    volume = int(data.get("Volume", 0) or 0)
    # OI fallback chain — NVDA may use different field name
    oi = int(data.get("DailyOpenInterest", 0) or 0)
    if oi == 0:
        oi = int(data.get("OpenInterest", 0) or 0)
    if oi == 0:
        oi = int(data.get("TotalVolume", 0) or 0)
    gamma  = float(data.get("Gamma", 0) or 0)
    delta  = float(data.get("Delta", 0) or 0)
    iv     = float(data.get("ImpliedVolatility", 0) or 0)

    legs = data.get("Legs", [])
    if not legs:
        return strikes
    try:
        strike = float(legs[0].get("StrikePrice", "0"))
        strike = round(strike, 2)
    except Exception:
        return strikes

    if strike not in strikes:
        strikes[strike] = {
            "call_oi": 0, "put_oi": 0,
            "call_gamma": 0, "put_gamma": 0,
            "call_delta": 0, "put_delta": 0,
            "call_volume": 0, "put_volume": 0,
            "call_iv": 0, "put_iv": 0
        }

    if side == "Call":
        strikes[strike].update({
            "call_oi": oi, "call_gamma": gamma,
            "call_delta": delta, "call_volume": volume, "call_iv": iv
        })
    elif side == "Put":
        strikes[strike].update({
            "put_oi": oi, "put_gamma": abs(gamma),
            "put_delta": abs(delta), "put_volume": volume, "put_iv": iv
        })
    return strikes

# ── GEX / DHP CALCULATION ─────────────────────────────────────────────────────
def calculate_gaia(strikes, spot):
    spot2 = spot * spot
    results = []
    total_call_dhp = 0.0
    total_put_dhp  = 0.0

    for strike in sorted(strikes.keys()):
        s = strikes[strike]

        call_gex = s["call_oi"] * s["call_gamma"] * spot2 * 100
        put_gex  = s["put_oi"]  * s["put_gamma"]  * spot2 * 100 * -1
        net_gex  = call_gex + put_gex

        # DHP = volume × delta × spot (GFS signal)
        call_dhp = s["call_volume"] * s["call_delta"] * spot
        put_dhp  = s["put_volume"]  * s["put_delta"]  * spot * -1
        net_dhp  = call_dhp + put_dhp

        call_delta_oi = s["call_oi"] * s["call_delta"]
        put_delta_oi  = s["put_oi"]  * s["put_delta"] * -1

        total_call_dhp += call_dhp
        total_put_dhp  += put_dhp

        results.append({
            "strike":        strike,
            "call_oi":       s["call_oi"],
            "put_oi":        s["put_oi"],
            "call_gex":      round(call_gex, 2),
            "put_gex":       round(put_gex, 2),
            "net_gex":       round(net_gex, 2),
            "call_dhp":      round(call_dhp, 4),
            "put_dhp":       round(put_dhp, 4),
            "net_dhp":       round(net_dhp, 4),
            "call_delta_oi": round(call_delta_oi, 2),
            "put_delta_oi":  round(put_delta_oi, 2),
            "call_iv":       round(s["call_iv"] * 100, 2),
            "put_iv":        round(s["put_iv"]  * 100, 2),
            "call_gamma":    s["call_gamma"],
            "put_gamma":     s["put_gamma"],
            "call_volume":   s["call_volume"],
            "put_volume":    s["put_volume"],
        })

    total_dhp = total_call_dhp + total_put_dhp
    return results, round(total_dhp, 2), round(total_call_dhp, 2), round(total_put_dhp, 2)

# ── KEY LEVELS FROM GEX ───────────────────────────────────────────────────────
def calculate_levels(strikes_data, spot):
    if not strikes_data:
        return {}

    # Call Wall = strike with highest call GEX
    call_wall = max(strikes_data, key=lambda s: s["call_gex"], default=None)
    # Put Wall = strike with most negative put GEX
    put_wall  = min(strikes_data, key=lambda s: s["put_gex"], default=None)
    # Gamma Flip = where net_gex crosses zero (closest to spot)
    near_spot = sorted(strikes_data, key=lambda s: abs(s["strike"] - spot))
    gamma_flip = None
    for s in near_spot:
        if s["net_gex"] < 0:
            gamma_flip = s
            break
    # Gamma Node = highest absolute net_gex near spot (within ±20)
    near = [s for s in strikes_data if abs(s["strike"] - spot) <= 20]
    gamma_node = max(near, key=lambda s: abs(s["net_gex"]), default=None) if near else None

    levels = {}
    if call_wall:  levels["call_wall"]  = call_wall["strike"]
    if put_wall:   levels["put_wall"]   = put_wall["strike"]
    if gamma_flip: levels["gamma_flip"] = gamma_flip["strike"]
    if gamma_node: levels["gamma_node"] = gamma_node["strike"]
    return levels

# ── DHP MOMENTUM ──────────────────────────────────────────────────────────────
dhp_history = deque(maxlen=DHP_HISTORY_SIZE)

def calculate_dhp_momentum(current_dhp):
    dhp_history.append(current_dhp)
    if len(dhp_history) < 2:
        return 0, "NEUTRAL"
    momentum = current_dhp - dhp_history[0]
    if momentum > 50:   direction = "ACCELERATING_BULL"
    elif momentum > 10: direction = "BUILDING_BULL"
    elif momentum < -50: direction = "ACCELERATING_BEAR"
    elif momentum < -10: direction = "BUILDING_BEAR"
    else:               direction = "NEUTRAL"
    return round(momentum, 2), direction

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
def push_to_railway(payload):
    try:
        body = json.dumps(payload).encode("utf-8")
        conn = http.client.HTTPSConnection(
            RAILWAY_URL.replace("https://", ""),
            context=ssl.create_default_context(),
            timeout=10
        )
        conn.request("POST", PUSH_ENDPOINT, body=body, headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {RAILWAY_TOKEN}"
        })
        resp = conn.getresponse()
        resp.read()
        if resp.status == 200:
            log.info(f"Push NVDA OK → {PUSH_ENDPOINT}")
        else:
            log.warning(f"Push NVDA status: {resp.status}")
        conn.close()
    except Exception as e:
        log.warning(f"Push NVDA error: {e}")

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    log.info("=== GAIA NVDA Backend v1 iniciando ===")
    log.info(f"Symbol: {NVDA_SYMBOL} | Proximity: ±{STRIKE_PROXIMITY} strikes")

    token = get_token_with_retry()
    if not token:
        log.error("No se pudo obtener token. Abortando.")
        return

    expirations = get_nvda_expirations(token)
    log.info(f"Expirations: {expirations}")

    cache = {
        "0dte":    {"strikes": {}, "strikes_data": [], "levels": {}},
        "weekly":  {"strikes": {}, "strikes_data": [], "levels": {}},
        "monthly": {"strikes": {}, "strikes_data": [], "levels": {}},
    }

    # HIRO accumulators — reset diario
    import datetime as _dt
    hiro_call_accum = 0.0
    hiro_put_accum  = 0.0
    hiro_reset_date = None

    # Historia intraday
    intraday_history = []
    _today_str = _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%d')
    try:
        with open(HISTORY_FILE, 'r') as _f:
            _saved = json.load(_f)
            if _saved.get("date") == _today_str:
                intraday_history = _saved.get("history", [])
                log.info(f"Historia NVDA recuperada: {len(intraday_history)} puntos")
    except Exception:
        log.info("Historia NVDA: arrancando limpio")

    last_weekly  = 0
    last_monthly = 0
    cycle        = 0
    consecutive_errors = 0

    while True:
        try:
            cycle += 1
            now = time.time()

            # Token refresh
            tokens = load_tokens()
            if not is_token_valid(tokens):
                log.info("Token expirado — refrescando...")
                try:
                    tokens = refresh_token(tokens)
                    token  = tokens.get("access_token")
                except Exception as e:
                    log.error(f"Token refresh error: {e}")
                    safe_sleep(TOKEN_RETRY_WAIT)
                    continue
            else:
                token = tokens.get("access_token")

            # Daily HIRO reset
            today = _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%d')
            if hiro_reset_date != today:
                hiro_call_accum = 0.0
                hiro_put_accum  = 0.0
                hiro_reset_date = today
                intraday_history = []
                log.info(f"HIRO NVDA + historia reset para {today}")

            # Spot price
            spot = get_nvda_spot(token)
            if spot <= 0:
                log.warning("Sin spot NVDA — reintentando...")
                safe_sleep(REFRESH_0DTE)
                continue

            # 0DTE stream — cada ciclo
            try:
                raw_strikes = read_stream_nvda(token, expirations.get("0dte",""), spot)
                if raw_strikes:
                    cache["0dte"]["strikes"] = raw_strikes
                    strikes_data, total_dhp, call_dhp, put_dhp = calculate_gaia(raw_strikes, spot)
                    cache["0dte"]["strikes_data"] = strikes_data
                    cache["0dte"]["levels"]       = calculate_levels(strikes_data, spot)

                    # Accumulate HIRO
                    cycle_call = sum(s.get("call_dhp", 0) for s in strikes_data)
                    cycle_put  = sum(s.get("put_dhp",  0) for s in strikes_data)
                    hiro_call_accum += cycle_call
                    hiro_put_accum  += cycle_put
            except Exception as e:
                log.error(f"Error 0DTE NVDA: {e}")
                total_dhp = 0.0

            # Weekly — cada 60s
            if now - last_weekly >= REFRESH_WEEKLY:
                try:
                    raw_w = read_stream_nvda(token, expirations.get("weekly",""), spot)
                    if raw_w:
                        wd, _, _, _ = calculate_gaia(raw_w, spot)
                        cache["weekly"]["strikes_data"] = wd
                        cache["weekly"]["levels"]       = calculate_levels(wd, spot)
                    last_weekly = now
                except Exception as e:
                    log.error(f"Error Weekly NVDA: {e}")

            # Monthly — cada 300s
            if now - last_monthly >= REFRESH_MONTHLY:
                try:
                    raw_m = read_stream_nvda(token, expirations.get("monthly",""), spot)
                    if raw_m:
                        md, _, _, _ = calculate_gaia(raw_m, spot)
                        cache["monthly"]["strikes_data"] = md
                        cache["monthly"]["levels"]       = calculate_levels(md, spot)
                    last_monthly = now
                except Exception as e:
                    log.error(f"Error Monthly NVDA: {e}")

            # DHP momentum
            momentum, momentum_dir = calculate_dhp_momentum(
                sum(s.get("net_dhp", 0) for s in cache["0dte"]["strikes_data"])
            )

            # Push si hay datos
            if spot > 0 and cache["0dte"]["strikes_data"]:
                try:
                    _snap = {
                        "t": int(datetime.utcnow().timestamp()),
                        "s": round(spot, 2),
                        "c": round(hiro_call_accum, 2),
                        "p": round(hiro_put_accum,  2),
                        "n": round(hiro_call_accum + hiro_put_accum, 2),
                        "d": round(total_dhp, 2)
                    }
                    intraday_history.append(_snap)
                    if len(intraday_history) > 480:
                        intraday_history = intraday_history[-480:]
                    try:
                        with open(HISTORY_FILE, 'w') as _hf:
                            json.dump({"date": today, "history": intraday_history}, _hf)
                    except Exception as _he:
                        log.warning(f"No se pudo guardar historia NVDA: {_he}")

                    payload = {
                        "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "symbol":        "NVDA",
                        "spot":          spot,
                        "expiration":    expirations.get("0dte", ""),
                        "total_dhp":     total_dhp,
                        "hiro_call":     round(hiro_call_accum, 2),
                        "hiro_put":      round(hiro_put_accum,  2),
                        "hiro_total":    round(hiro_call_accum + hiro_put_accum, 2),
                        "dhp_momentum":  momentum,
                        "dhp_direction": momentum_dir,
                        "levels":        cache["0dte"]["levels"],
                        "strikes":       cache["0dte"]["strikes_data"],
                        "layers": {
                            "0dte":    {"expiration": expirations.get("0dte"),    "levels": cache["0dte"]["levels"]},
                            "weekly":  {"expiration": expirations.get("weekly"),  "levels": cache["weekly"]["levels"]},
                            "monthly": {"expiration": expirations.get("monthly"), "levels": cache["monthly"]["levels"]},
                        },
                        "status":  "live",
                        "history": intraday_history[-480:]
                    }
                    push_to_railway(payload)
                    try:
                        with open(OUTPUT_FILE, 'w') as _jf:
                            json.dump(payload, _jf)
                    except Exception as _je:
                        log.warning(f"No se pudo escribir JSON local: {_je}")
                    consecutive_errors = 0
                    log.info(f"Cycle {cycle} | NVDA ${spot:.2f} | DHP {total_dhp:.0f} | {momentum_dir}")
                except Exception as e:
                    log.error(f"Error push NVDA ciclo {cycle}: {e}")
                    consecutive_errors += 1
            else:
                log.warning(f"Sin datos NVDA — spot:{spot}")
                consecutive_errors += 1

            if consecutive_errors >= MAX_CYCLE_ERRORS:
                log.error(f"{consecutive_errors} errores NVDA — pausa {CYCLE_ERROR_WAIT}s")
                safe_sleep(CYCLE_ERROR_WAIT)
                consecutive_errors = 0
                try:
                    token = get_token_with_retry() or token
                    expirations = get_nvda_expirations(token) or expirations
                except Exception:
                    pass

        except KeyboardInterrupt:
            log.info("NVDA backend detenido por usuario.")
            break
        except Exception as e:
            log.error(f"Excepcion no prevista ciclo {cycle}: {e}")
            consecutive_errors += 1

        try:
            time.sleep(REFRESH_0DTE)
        except KeyboardInterrupt:
            log.info("NVDA backend detenido.")
            break

if __name__ == "__main__":
    main()
