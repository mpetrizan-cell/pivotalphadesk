"""
PivotAlphaDesk - GAIA ETF Backend v1
ts_gaia_etf.py

Parser SPY + QQQ — flujo cruzado vs SPX/NDX
  SPY → referencia ES/SPX  (SPY×10 ≈ SPX)
  QQQ → referencia NQ/NDX  (QQQ×~58 ≈ NDX)

Pipeline:
  - Streams secuenciales (SPY → QQQ) para no saturar TS
  - Refresh 0DTE cada 5s por instrumento → ~10s ciclo total
  - JSON output: gaia_etf_live.json
  - Railway push: /push_etf
"""

import json, os, time, urllib.parse, urllib.request
import http.client, ssl, logging
from datetime import datetime, timedelta
from collections import deque

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
TS_CLIENT_ID     = "HMVux6j6ncGeYOVFbWVXyB0lSVL4WWWe"
TS_CLIENT_SECRET = "2Y4SKDlCN0PMX6wbwWLRvcPNeaA7Zl1ygJoSFO9XWWvsCP37xXrF9RzCUBjaddIx"
TOKEN_FILE       = "ts_tokens.json"
TOKEN_URL        = "https://signin.tradestation.com/oauth/token"
API_BASE         = "https://api.tradestation.com/v3"
OUTPUT_FILE      = "gaia_etf_live.json"
LOG_FILE         = "gaia_etf_live.log"

# Símbolos ETF — sin $ directo
SPY_SYMBOL       = "SPY"
QQQ_SYMBOL       = "QQQ"
ES_SYMBOL        = "ESM26"
NQ_SYMBOL        = "NQM26"

STRIKE_PROXIMITY = 10    # ETFs tienen strikes más juntos
REFRESH_0DTE     = 5     # segundos por instrumento → ~10s ciclo total
REFRESH_WEEKLY   = 60
REFRESH_MONTHLY  = 300
DHP_HISTORY_SIZE = 10

# Ratios de conversión ETF → índice
SPY_MULTIPLIER   = 10.0   # SPY × 10 ≈ SPX
QQQ_MULTIPLIER   = 58.0   # QQQ × ~58 ≈ NDX (aproximado — verificar en vivo)

# ── RAILWAY ───────────────────────────────────────────────────────────────────
RAILWAY_URL   = "https://web-production-49e7.up.railway.app"
RAILWAY_TOKEN = "gaia_push_secret_2026"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ETF][%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
log = logging.getLogger("GAIA_ETF")

# ── DHP HISTORY por instrumento ───────────────────────────────────────────────
dhp_history_spy = deque(maxlen=DHP_HISTORY_SIZE)
dhp_history_qqq = deque(maxlen=DHP_HISTORY_SIZE)

# ── TOKEN — comparte con SPX y NDX backends ───────────────────────────────────
def load_tokens():
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE) as f:
                return json.load(f)
    except Exception as e:
        log.error(f"Error leyendo token: {e}")
    return None

def save_tokens(tokens):
    try:
        if not tokens.get("refresh_token"):
            existing = load_tokens()
            if existing and existing.get("refresh_token"):
                tokens["refresh_token"] = existing["refresh_token"]
        tokens["saved_at"] = time.time()
        with open(TOKEN_FILE, "w") as f:
            json.dump(tokens, f, indent=2)
    except Exception as e:
        log.error(f"Error guardando token: {e}")

def refresh_token(refresh_tok):
    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "refresh_token": refresh_tok,
        "client_id":     TS_CLIENT_ID,
        "client_secret": TS_CLIENT_SECRET,
    }).encode()
    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())

def get_valid_token():
    tokens = load_tokens()
    if not tokens:
        log.error("No hay token. Corre ts_auth.py primero.")
        return None
    elapsed = time.time() - tokens.get("saved_at", 0)
    if elapsed < 900:
        access = tokens.get("access_token")
        if access:
            return access
    log.info("Refrescando token ETF...")
    refresh_tok = tokens.get("refresh_token")
    if not refresh_tok:
        log.error("Sin refresh_token — corre ts_auth.py.")
        return None
    try:
        new_tokens = refresh_token(refresh_tok)
        save_tokens(new_tokens)
        return new_tokens.get("access_token")
    except Exception as e:
        log.error(f"Error refresh token: {e}")
        return None

# ── API GET ───────────────────────────────────────────────────────────────────
def api_get(endpoint, token, timeout=10):
    url = API_BASE + endpoint
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())

# ── PRECIOS ───────────────────────────────────────────────────────────────────
def get_price(symbol, token):
    try:
        sym_enc = urllib.parse.quote(symbol, safe="")
        result  = api_get(f"/marketdata/quotes/{sym_enc}", token, timeout=20)
        quotes  = result.get("Quotes", [])
        if quotes:
            last = quotes[0].get("Last", 0)
            return float(last) if last else 0.0
    except Exception as e:
        log.warning(f"Error precio {symbol}: {e}")
    return 0.0

# ── EXPIRATIONS ───────────────────────────────────────────────────────────────
def get_expirations(symbol, token):
    """Clasifica expirations en 3 capas: 0dte / weekly / monthly"""
    layers = {"0dte": None, "weekly": None, "monthly": None}
    try:
        sym_enc = urllib.parse.quote(symbol, safe="")
        result  = api_get(f"/marketdata/options/expirations/{sym_enc}", token)
        expirations = result.get("Expirations", [])
        if not expirations:
            return layers

        today     = datetime.now().date()
        week_end  = today + timedelta(days=(4 - today.weekday()) % 7)
        month_end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        for exp in expirations:
            exp_date_str = exp.get("Date", "")[:10]
            try:
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if exp_date == today and not layers["0dte"]:
                layers["0dte"] = exp_date_str
            elif exp_date <= week_end and exp_date > today and not layers["weekly"]:
                layers["weekly"] = exp_date_str
            elif exp_date <= month_end and exp_date > week_end and not layers["monthly"]:
                layers["monthly"] = exp_date_str

            if all(layers.values()):
                break

        # Fallbacks
        if not layers["0dte"] and expirations:
            layers["0dte"] = expirations[0].get("Date", "")[:10]
        if not layers["weekly"] and len(expirations) > 1:
            layers["weekly"] = expirations[1].get("Date", "")[:10]
        if not layers["monthly"] and len(expirations) > 2:
            layers["monthly"] = expirations[2].get("Date", "")[:10]

        log.info(f"{symbol} expirations: {layers}")
    except Exception as e:
        log.error(f"Error expirations {symbol}: {e}")
    return layers

# ── STREAM ────────────────────────────────────────────────────────────────────
def read_stream(symbol, expiration, spot, token):
    strikes = {}
    params  = "?" + urllib.parse.urlencode({
        "expiration":      expiration,
        "strikeProximity": STRIKE_PROXIMITY,
    })
    sym_enc = urllib.parse.quote(symbol, safe="")
    url     = f"/v3/marketdata/stream/options/chains/{sym_enc}{params}"
    conn    = None
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
            log.error(f"Stream {symbol} status: {resp.status}")
            return strikes

        lines_read    = 0
        max_contracts = STRIKE_PROXIMITY * 2 * 2 + 5
        heartbeats    = 0
        max_heartbeat = 8

        while lines_read < max_contracts and heartbeats < max_heartbeat:
            try:
                raw = resp.readline().decode("utf-8").strip()
            except Exception as e:
                log.warning(f"Stream {symbol} readline error: {e}")
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
            lines_read += 1

    except Exception as e:
        log.error(f"Stream {symbol} error: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
    return strikes

# ── PARSER ────────────────────────────────────────────────────────────────────
def _parse_option_line(data, strikes):
    side   = data.get("Side", "")
    volume = int(data.get("Volume", 0) or 0)
    oi     = int(data.get("DailyOpenInterest", 0) or 0)
    gamma  = float(data.get("Gamma", 0) or 0)
    delta  = float(data.get("Delta", 0) or 0)
    iv     = float(data.get("ImpliedVolatility", 0) or 0)

    legs = data.get("Legs", [])
    if not legs:
        return strikes
    try:
        strike = float(legs[0].get("StrikePrice", "0"))
        # ETF strikes en decimales — redondear a 1 decimal
        strike = round(strike, 1)
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

# ── GEX / DHP ─────────────────────────────────────────────────────────────────
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
        call_dhp = s["call_volume"] * s["call_delta"] * spot
        put_dhp  = s["put_volume"]  * s["put_delta"]  * spot * -1
        net_dhp  = call_dhp + put_dhp
        total_call_dhp += call_dhp
        total_put_dhp  += put_dhp
        results.append({
            "strike":   strike,
            "call_gex": round(call_gex / 1e6, 4),
            "put_gex":  round(put_gex  / 1e6, 4),
            "net_gex":  round(net_gex  / 1e6, 4),
            "call_dhp": round(call_dhp / 1e6, 4),
            "put_dhp":  round(put_dhp  / 1e6, 4),
            "net_dhp":  round(net_dhp  / 1e6, 4),
            "call_oi":  s["call_oi"],
            "put_oi":   s["put_oi"],
            "call_iv":  s["call_iv"],
            "put_iv":   s["put_iv"],
        })

    total_dhp      = round((total_call_dhp + total_put_dhp) / 1e6, 4)
    total_call_dhp = round(total_call_dhp / 1e6, 4)
    total_put_dhp  = round(total_put_dhp  / 1e6, 4)
    return results, total_dhp, total_call_dhp, total_put_dhp

# ── NIVELES PAD ───────────────────────────────────────────────────────────────
def calculate_levels(strikes_data, spot):
    if not strikes_data:
        return {}

    above = [s for s in strikes_data if s["strike"] >= spot]
    below = [s for s in strikes_data if s["strike"] <  spot]

    call_wall   = max(above, key=lambda s: s["call_gex"]) if above else max(strikes_data, key=lambda s: s["call_gex"])
    put_wall    = min(below, key=lambda s: s["put_gex"])  if below else min(strikes_data, key=lambda s: s["put_gex"])
    gamma_node  = max(strikes_data, key=lambda s: s["call_gex"] + abs(s["put_gex"]))
    gravity_pin = max(strikes_data, key=lambda s: s["call_oi"] + s["put_oi"])

    sorted_s    = sorted(strikes_data, key=lambda s: s["strike"])
    flip_strike = None
    for i in range(1, len(sorted_s)):
        if sorted_s[i-1]["net_gex"] < 0 and sorted_s[i]["net_gex"] >= 0:
            flip_strike = sorted_s[i]["strike"]
            break
    if not flip_strike:
        flip_strike = min(sorted_s, key=lambda s: abs(s["net_gex"]))["strike"]

    return {
        "call_wall":   call_wall["strike"],
        "put_wall":    put_wall["strike"],
        "gamma_node":  gamma_node["strike"],
        "gamma_flip":  flip_strike,
        "gravity_pin": gravity_pin["strike"],
    }

# ── CONFLUENCIA CRUZADA SPY vs QQQ ───────────────────────────────────────────
def calculate_cross_confluence(levels_spy, levels_qqq, spot_spy, spot_qqq):
    """
    Detecta alineación direccional entre SPY y QQQ.
    Retorna señal de convicción cruzada.
    """
    signal = {"direction": "NEUTRAL", "conviction": 0, "notes": []}

    if not levels_spy or not levels_qqq:
        return signal

    spy_above_flip = spot_spy > levels_spy.get("gamma_flip", 0)
    qqq_above_flip = spot_qqq > levels_qqq.get("gamma_flip", 0)

    spy_above_node = spot_spy > levels_spy.get("gamma_node", 0)
    qqq_above_node = spot_qqq > levels_qqq.get("gamma_node", 0)

    conviction = 0

    if spy_above_flip and qqq_above_flip:
        conviction += 2
        signal["notes"].append("Both above Gamma Flip — BULL regime")
    elif not spy_above_flip and not qqq_above_flip:
        conviction -= 2
        signal["notes"].append("Both below Gamma Flip — BEAR regime")
    else:
        signal["notes"].append("SPY/QQQ divergence at Gamma Flip")

    if spy_above_node and qqq_above_node:
        conviction += 1
        signal["notes"].append("Both above Gamma Node")
    elif not spy_above_node and not qqq_above_node:
        conviction -= 1
        signal["notes"].append("Both below Gamma Node")

    signal["conviction"] = conviction
    if conviction >= 2:
        signal["direction"] = "BULL_CONFIRMED"
    elif conviction <= -2:
        signal["direction"] = "BEAR_CONFIRMED"
    elif conviction > 0:
        signal["direction"] = "BULL_WEAK"
    elif conviction < 0:
        signal["direction"] = "BEAR_WEAK"

    return signal

# ── DHP MOMENTUM ──────────────────────────────────────────────────────────────
def calculate_dhp_momentum(current_dhp, history):
    history.append(current_dhp)
    if len(history) < 2:
        return 0.0, "NEUTRAL"

    recent   = list(history)[-3:]
    older    = list(history)[:-3]
    avg_r    = sum(recent) / len(recent)
    avg_o    = sum(older) / len(older) if older else avg_r
    momentum = round(avg_r - avg_o, 4)

    if momentum > 5:     direction = "ACCELERATING_BULL"
    elif momentum > 1:   direction = "BUILDING_BULL"
    elif momentum < -5:  direction = "ACCELERATING_BEAR"
    elif momentum < -1:  direction = "BUILDING_BEAR"
    else:                direction = "NEUTRAL"

    return momentum, direction

# ── GUARDAR JSON ──────────────────────────────────────────────────────────────
def save_etf_json(spy_data, qqq_data, cross_signal):
    try:
        output = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "spy":       spy_data,
            "qqq":       qqq_data,
            "cross":     cross_signal,
            "status":    "live"
        }
        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(output, f, indent=2)
        os.replace(tmp, OUTPUT_FILE)

        spy_dir  = spy_data.get("dhp_direction", "—")
        qqq_dir  = qqq_data.get("dhp_direction", "—")
        cross    = cross_signal.get("direction", "—")
        log.info(
            f"SPY:{spy_data.get('spot',0):.2f} DHP:{spy_data.get('total_dhp',0)} [{spy_dir}] | "
            f"QQQ:{qqq_data.get('spot',0):.2f} DHP:{qqq_data.get('total_dhp',0)} [{qqq_dir}] | "
            f"CROSS:{cross}"
        )
    except Exception as e:
        log.error(f"Error guardando ETF JSON: {e}")

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
def push_to_railway(data: dict):
    try:
        body = json.dumps(data).encode("utf-8")
        req  = urllib.request.Request(
            RAILWAY_URL + "/push_etf",
            data=body,
            method="POST"
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Push-Token", RAILWAY_TOKEN)
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status != 200:
                log.warning(f"Railway ETF push status: {resp.status}")
    except Exception as e:
        log.warning(f"Railway ETF push error: {e}")

# ── SAFE SLEEP ────────────────────────────────────────────────────────────────
def safe_sleep(seconds):
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise
    except Exception:
        pass

# ── PROCESAR UN INSTRUMENTO ───────────────────────────────────────────────────
def process_instrument(symbol, expirations, spot, token, dhp_history, cache):
    """Procesa un instrumento completo — stream 0DTE + cache Weekly/Monthly."""
    result = {
        "symbol":        symbol,
        "spot":          spot,
        "total_dhp":     0.0,
        "dhp_direction": "NEUTRAL",
        "dhp_momentum":  0.0,
        "levels":        {},
        "strikes":       [],
        "expirations":   expirations,
    }

    if not expirations.get("0dte") or spot <= 0:
        return result

    try:
        raw = read_stream(symbol, expirations["0dte"], spot, token)
        if raw:
            strikes_data, total_dhp, call_dhp, put_dhp = calculate_gaia(raw, spot)
            levels = calculate_levels(strikes_data, spot)
            cache["0dte"] = {"strikes_data": strikes_data, "levels": levels}
            momentum, direction = calculate_dhp_momentum(total_dhp, dhp_history)
            result.update({
                "total_dhp":     total_dhp,
                "call_dhp":      call_dhp,
                "put_dhp":       put_dhp,
                "dhp_direction": direction,
                "dhp_momentum":  momentum,
                "levels":        levels,
                "strikes":       strikes_data,
            })
            log.info(f"{symbol} 0DTE — {len(raw)} strikes DHP:{total_dhp} [{direction}]")
    except Exception as e:
        log.error(f"Error procesando {symbol}: {e}")

    return result

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA ETF Backend v1")
    log.info("  SPY + QQQ · Flujo Cruzado · Validación")
    log.info("=" * 60)

    # ── Token inicial
    token = None
    while not token:
        try:
            token = get_valid_token()
            if not token:
                log.warning("Sin token — reintentando en 30s...")
                safe_sleep(30)
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error token: {e}")
            safe_sleep(30)

    # ── Expirations iniciales
    exp_spy = {"0dte": None, "weekly": None, "monthly": None}
    exp_qqq = {"0dte": None, "weekly": None, "monthly": None}

    while not any(exp_spy.values()):
        try:
            exp_spy = get_expirations(SPY_SYMBOL, token)
            if not any(exp_spy.values()):
                safe_sleep(30)
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error exp SPY: {e}")
            safe_sleep(30)

    while not any(exp_qqq.values()):
        try:
            exp_qqq = get_expirations(QQQ_SYMBOL, token)
            if not any(exp_qqq.values()):
                safe_sleep(30)
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error exp QQQ: {e}")
            safe_sleep(30)

    log.info(f"SPY expirations: {exp_spy}")
    log.info(f"QQQ expirations: {exp_qqq}")

    # ── Cache y timers
    cache_spy = {"0dte": {}, "weekly": {}, "monthly": {}}
    cache_qqq = {"0dte": {}, "weekly": {}, "monthly": {}}
    last_exp_check = 0.0

    cycle = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        now = time.time()
        log.info(f"--- ETF Ciclo {cycle} ---")

        try:
            # ── Token
            try:
                new_token = get_valid_token()
                if new_token:
                    token = new_token
            except Exception as e:
                log.warning(f"Token ciclo {cycle}: {e}")

            # ── Refresh expirations cada 10 min
            if now - last_exp_check > 600:
                try:
                    new_spy = get_expirations(SPY_SYMBOL, token)
                    new_qqq = get_expirations(QQQ_SYMBOL, token)
                    if any(new_spy.values()): exp_spy = new_spy
                    if any(new_qqq.values()): exp_qqq = new_qqq
                    last_exp_check = now
                except Exception as e:
                    log.warning(f"Error refresh expirations: {e}")

            # ── Precios
            spot_spy = get_price(SPY_SYMBOL, token)
            spot_qqq = get_price(QQQ_SYMBOL, token)
            spot_es  = get_price(ES_SYMBOL, token)
            spot_nq  = get_price(NQ_SYMBOL, token)

            # ── Procesar SPY
            spy_data = process_instrument(
                SPY_SYMBOL, exp_spy, spot_spy, token,
                dhp_history_spy, cache_spy
            )
            spy_data["spot_es"]  = spot_es
            spy_data["basis_es"] = round(spot_es - spot_spy * SPY_MULTIPLIER, 2) if spot_es > 0 and spot_spy > 0 else 0.0

            # ── Procesar QQQ (secuencial — después de SPY)
            qqq_data = process_instrument(
                QQQ_SYMBOL, exp_qqq, spot_qqq, token,
                dhp_history_qqq, cache_qqq
            )
            qqq_data["spot_nq"]  = spot_nq
            # Ratio dinamico NDX/QQQ — calculado en tiempo real
            ndx_spot = spot_nq - 150 if spot_nq > 0 else 0  # NDX cash approx
            ndx_ratio = round(ndx_spot / spot_qqq, 4) if spot_qqq > 0 and ndx_spot > 0 else 41.0
            qqq_data["ndx_ratio"]   = ndx_ratio
            qqq_data["basis_nq"]    = round(spot_nq - spot_qqq * ndx_ratio, 2) if spot_nq > 0 and spot_qqq > 0 else 0.0
            # Proxy strikes — NDX PAD levels translated to QQQ strikes
            lv = qqq_data.get("levels", {})
            qqq_data["proxy_strikes"] = {
                "call_wall": round(lv.get("call_wall", 0) * ndx_ratio, 0) if lv.get("call_wall") else 0,
                "put_wall":  round(lv.get("put_wall",  0) * ndx_ratio, 0) if lv.get("put_wall")  else 0,
                "gamma_flip":round(lv.get("gamma_flip",0) * ndx_ratio, 0) if lv.get("gamma_flip") else 0,
            }

            # ── Confluencia cruzada SPY vs QQQ
            cross_signal = calculate_cross_confluence(
                spy_data.get("levels", {}),
                qqq_data.get("levels", {}),
                spot_spy, spot_qqq
            )

            # ── Guardar + Push
            if spot_spy > 0 or spot_qqq > 0:
                save_etf_json(spy_data, qqq_data, cross_signal)
                push_to_railway({
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "spy":       spy_data,
                    "qqq":       qqq_data,
                    "cross":     cross_signal,
                    "status":    "live"
                })
                consecutive_errors = 0
            else:
                log.warning("Sin precios SPY/QQQ")
                consecutive_errors += 1

            if consecutive_errors >= 10:
                log.error("10 errores — pausa 120s...")
                safe_sleep(120)
                consecutive_errors = 0

        except KeyboardInterrupt:
            log.info("ETF backend detenido por usuario.")
            break
        except Exception as e:
            log.error(f"Excepcion no prevista ciclo {cycle}: {e}")
            consecutive_errors += 1

        # ── Sleep al final
        try:
            time.sleep(REFRESH_0DTE)
        except KeyboardInterrupt:
            log.info("ETF backend detenido.")
            break

if __name__ == "__main__":
    main()
