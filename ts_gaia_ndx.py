"""
PivotAlphaDesk - GAIA NDX Chart Backend v1
ts_gaia_ndx.py

Parser NDX — tres capas:
  Layer 1: 0DTE        → stream cada 5s   (NDXP, vence hoy)
  Layer 2: Weekly      → REST cada 60s    (NDXP, vence esta semana)
  Layer 3: Monthly/Q   → REST cada 300s   (NDXP, vence este mes o trimestre)

Basis NQ/NDX: ~150 pts (verificar en vivo cada día)
JSON output: gaia_ndx_live.json
Railway push: misma URL, endpoint /push_ndx
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
OUTPUT_FILE      = "gaia_ndx_live.json"
LOG_FILE         = "gaia_ndx_live.log"

NDX_SYMBOL       = "$NDXP.X"                 # opciones NDX CBOE
NQ_SYMBOL        = "NQM26"                   # futuro activo Jun 2026
STRIKE_PROXIMITY = 20                         # strikes alrededor del spot NDX

# Refresh diferenciado por capa (segundos)
REFRESH_0DTE     = 5
REFRESH_WEEKLY   = 60
REFRESH_MONTHLY  = 300

# Basis NQ/NDX empírico — se actualiza en vivo
BASIS_NQ_NDX_DEFAULT = 150.0

DHP_HISTORY_SIZE = 10
FLOW_HISTORY_SIZE = 1500  # rolling history for GAIA Flow Signal

# ── RAILWAY ───────────────────────────────────────────────────────────────────
RAILWAY_URL   = "https://web-production-49e7.up.railway.app"
RAILWAY_TOKEN = "gaia_push_secret_2026"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NDX][%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
log = logging.getLogger("GAIA_NDX")

# ── DHP HISTORY ───────────────────────────────────────────────────────────────
dhp_history = deque(maxlen=DHP_HISTORY_SIZE)
flow_history = deque(maxlen=FLOW_HISTORY_SIZE)

# ── TOKEN — comparte con SPX backend ─────────────────────────────────────────
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
    log.info("Refrescando token NDX...")
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

# ── API GET REST ───────────────────────────────────────────────────────────────
def api_get(endpoint, token, timeout=10):
    url = API_BASE + endpoint
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())

# ── PRECIO NDX (cash) ─────────────────────────────────────────────────────────
def get_ndx_price(token):
    try:
        result = api_get("/marketdata/quotes/%24NDX.X", token)
        quotes = result.get("Quotes", [])
        if quotes:
            last = quotes[0].get("Last", 0)
            return float(last) if last else 0.0
    except Exception as e:
        log.warning(f"Error precio NDX: {e}")
    return 0.0

# ── PRECIO NQ (futuro) ────────────────────────────────────────────────────────
def get_nq_price(token):
    try:
        result = api_get(f"/marketdata/quotes/{NQ_SYMBOL}", token)
        quotes = result.get("Quotes", [])
        if quotes:
            last = quotes[0].get("Last", 0)
            return float(last) if last else 0.0
    except Exception as e:
        log.warning(f"Error precio NQ: {e}")
    return 0.0

# ── EXPIRATIONS NDX — clasificadas por capa ───────────────────────────────────
def get_ndx_expirations(token):
    """
    Retorna dict con tres capas:
      {
        "0dte":    "2026-04-23",   # vence hoy o None
        "weekly":  "2026-04-25",   # vence esta semana (viernes)
        "monthly": "2026-05-15",   # vence este mes/trimestre
      }
    """
    layers = {"0dte": None, "weekly": None, "monthly": None}
    try:
        sym_enc = urllib.parse.quote(NDX_SYMBOL, safe="")
        result = api_get(f"/marketdata/options/expirations/{sym_enc}", token)
        expirations = result.get("Expirations", [])
        if not expirations:
            return layers

        today      = datetime.now().date()
        week_end   = today + timedelta(days=(4 - today.weekday()) % 7)  # viernes
        month_end  = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        for exp in expirations:
            exp_date_str = exp.get("Date", "")[:10]
            try:
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if exp_date == today and not layers["0dte"]:
                layers["0dte"] = exp_date_str
                log.info(f"NDX 0DTE detectado: {exp_date_str}")
            elif exp_date <= week_end and exp_date > today and not layers["weekly"]:
                layers["weekly"] = exp_date_str
                log.info(f"NDX Weekly detectado: {exp_date_str}")
            elif exp_date <= month_end and exp_date > week_end and not layers["monthly"]:
                layers["monthly"] = exp_date_str
                log.info(f"NDX Monthly detectado: {exp_date_str}")

            if all(layers.values()):
                break

        # Fallbacks si no hay 0DTE (fin de semana / holiday)
        if not layers["0dte"] and expirations:
            layers["0dte"] = expirations[0].get("Date", "")[:10]
            log.info(f"NDX 0DTE fallback: {layers['0dte']}")
        if not layers["weekly"] and len(expirations) > 1:
            layers["weekly"] = expirations[1].get("Date", "")[:10]
        if not layers["monthly"] and len(expirations) > 2:
            layers["monthly"] = expirations[2].get("Date", "")[:10]

    except Exception as e:
        log.error(f"Error obteniendo expirations NDX: {e}")

    return layers

# ── STREAM NDX — 0DTE (igual pipeline que SPX) ───────────────────────────────
def read_stream_ndx(token, expiration, spot):
    """Stream HTTPs para 0DTE — misma lógica que SPX."""
    strikes = {}
    params = "?" + urllib.parse.urlencode({
        "expiration":      expiration,
        "strikeProximity": STRIKE_PROXIMITY,
    })
    sym_enc = urllib.parse.quote(NDX_SYMBOL, safe="")
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
            log.error(f"Stream NDX status: {resp.status}")
            return strikes

        lines_read    = 0
        max_contracts = STRIKE_PROXIMITY * 2 * 2 + 5
        heartbeats    = 0
        max_heartbeat = 8

        while lines_read < max_contracts and heartbeats < max_heartbeat:
            try:
                raw = resp.readline().decode("utf-8").strip()
            except Exception as e:
                log.warning(f"Stream NDX readline error: {e}")
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
        log.error(f"Stream NDX error: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
    return strikes

# ── REST SNAPSHOT — Weekly / Monthly ─────────────────────────────────────────
def read_rest_ndx(token, expiration, spot):
    """
    TS API v3 no tiene endpoint REST /chains — usa el mismo stream con expiration diferente.
    Refresh diferenciado controlado por timer en el main loop.
    """
    return read_stream_ndx(token, expiration, spot)

# ── PARSER COMÚN PARA STREAM Y REST ──────────────────────────────────────────
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
        strike = int(float(legs[0].get("StrikePrice", "0")))
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
            "call_gex": round(call_gex / 1e6, 2),
            "put_gex":  round(put_gex  / 1e6, 2),
            "net_gex":  round(net_gex  / 1e6, 2),
            "call_dhp": round(call_dhp / 1e6, 2),
            "put_dhp":  round(put_dhp  / 1e6, 2),
            "net_dhp":  round(net_dhp  / 1e6, 2),
            "call_oi":  s["call_oi"],
            "put_oi":   s["put_oi"],
            "call_iv":  s["call_iv"],
            "put_iv":   s["put_iv"],
        })

    total_call_dhp_m = round(total_call_dhp / 1e6, 2)
    total_put_dhp_m  = round(total_put_dhp  / 1e6, 2)
    total_dhp = round(total_call_dhp_m + total_put_dhp_m, 2)
    return results, total_dhp, total_call_dhp_m, total_put_dhp_m

# ── NIVELES PAD ───────────────────────────────────────────────────────────────
def calculate_levels(strikes_data, spot):
    if not strikes_data:
        return {}

    above = [s for s in strikes_data if s["strike"] >= spot]
    below = [s for s in strikes_data if s["strike"] <  spot]

    call_wall  = max(above, key=lambda s: s["call_gex"]) if above else max(strikes_data, key=lambda s: s["call_gex"])
    put_wall   = min(below, key=lambda s: s["put_gex"])  if below else min(strikes_data, key=lambda s: s["put_gex"])
    gamma_node = max(strikes_data, key=lambda s: s["call_gex"] + abs(s["put_gex"]))
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

# ── CONFLUENCIA — niveles que aparecen en 2+ capas ───────────────────────────
def calculate_confluence(levels_0dte, levels_weekly, levels_monthly):
    """
    Detecta strikes que son nivel PAD en 2 o más capas.
    Retorna lista de {strike, layers, type} ordenada por strike.
    """
    all_levels = {}

    def add(levels, layer_name):
        if not levels:
            return
        for key, strike in levels.items():
            if strike not in all_levels:
                all_levels[strike] = {"layers": [], "types": []}
            all_levels[strike]["layers"].append(layer_name)
            all_levels[strike]["types"].append(key)

    add(levels_0dte,    "0dte")
    add(levels_weekly,  "weekly")
    add(levels_monthly, "monthly")

    confluence = []
    for strike, info in sorted(all_levels.items()):
        if len(info["layers"]) >= 2:
            confluence.append({
                "strike": strike,
                "layers": info["layers"],
                "types":  list(set(info["types"])),
                "strength": len(info["layers"])   # 2 = fuerte, 3 = muy fuerte
            })

    return sorted(confluence, key=lambda x: x["strike"])

# ── DHP MOMENTUM ──────────────────────────────────────────────────────────────
def calculate_dhp_momentum(current_dhp):
    dhp_history.append(current_dhp)
    if len(dhp_history) < 2:
        return 0.0, "NEUTRAL"

    recent   = list(dhp_history)[-3:]
    older    = list(dhp_history)[:-3]
    avg_r    = sum(recent) / len(recent)
    avg_o    = sum(older) / len(older) if older else avg_r
    momentum = round(avg_r - avg_o, 2)

    if momentum > 50:    direction = "ACCELERATING_BULL"
    elif momentum > 10:  direction = "BUILDING_BULL"
    elif momentum < -50: direction = "ACCELERATING_BEAR"
    elif momentum < -10: direction = "BUILDING_BEAR"
    else:                direction = "NEUTRAL"

    return momentum, direction


# ── GAIA FLOW SIGNAL HISTORY ──────────────────────────────────────────────────
def append_flow_history(spot_ndx, spot_nq, total_dhp, call_pressure, put_pressure, momentum):
    """Rolling temporal memory used by the frontend to build GAIA Flow Signal.

    This is intentionally stored in the backend output so the chart never has to
    invent a synthetic HIRO-like line. Each point is one real backend cycle.
    """
    t = int(time.time())
    last = flow_history[-1] if flow_history else None
    flow_delta = round(total_dhp - float(last.get("total_dhp", total_dhp)), 4) if last else 0.0
    price_delta = round(spot_ndx - float(last.get("spot", spot_ndx)), 4) if last else 0.0

    flow_history.append({
        "t": t,
        "spot": round(float(spot_ndx), 4),
        "spot_nq": round(float(spot_nq), 4) if spot_nq else None,
        "total_dhp": round(float(total_dhp), 4),
        "call_pressure": round(float(call_pressure), 4),
        "put_pressure": round(float(put_pressure), 4),
        "dhp_momentum": round(float(momentum), 4),
        "flow_delta": flow_delta,
        "price_delta": price_delta,
    })
    return list(flow_history)

# ── GUARDAR JSON ──────────────────────────────────────────────────────────────
def save_ndx_json(layers_data, spot_ndx, spot_nq, basis, confluence,
                   total_dhp, momentum, momentum_dir, flow_history_out):
    try:
        output = {
            "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "spot_ndx":      spot_ndx,
            "spot_nq":       spot_nq if spot_nq > 0 else spot_ndx + basis,
            "basis":         basis,
            "total_dhp":     total_dhp,
            "dhp_momentum":  momentum,
            "dhp_direction": momentum_dir,
            "confluence":    confluence,
            "history":       flow_history_out,
            "layers": {
                "0dte":    layers_data.get("0dte",    {}),
                "weekly":  layers_data.get("weekly",  {}),
                "monthly": layers_data.get("monthly", {}),
            },
            "status": "live"
        }
        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(output, f, indent=2)
        os.replace(tmp, OUTPUT_FILE)

        conf_str = " | ".join([f"{c['strike']}({c['strength']}x)" for c in confluence]) or "none"
        log.info(
            f"NDX:{spot_ndx} NQ:{spot_nq} BASIS:{basis} DHP:{total_dhp}M [{momentum_dir}] "
            f"Confluencia: {conf_str}"
        )
    except Exception as e:
        log.error(f"Error guardando NDX JSON: {e}")

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
def push_to_railway(data: dict):
    try:
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(
            RAILWAY_URL + "/push_ndx",
            data=body,
            method="POST"
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Push-Token", RAILWAY_TOKEN)
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status != 200:
                log.warning(f"Railway NDX push status: {resp.status}")
    except Exception as e:
        log.warning(f"Railway NDX push error: {e}")

# ── SAFE SLEEP ────────────────────────────────────────────────────────────────
def safe_sleep(seconds):
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise
    except Exception:
        pass

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA NDX Backend v1")
    log.info("  3 capas: 0DTE(5s) / Weekly(60s) / Monthly(300s)")
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
            log.error(f"Error token arranque: {e}")
            safe_sleep(30)

    # ── Expirations iniciales
    expirations = {"0dte": None, "weekly": None, "monthly": None}
    while not any(expirations.values()):
        try:
            expirations = get_ndx_expirations(token)
            if not any(expirations.values()):
                log.warning("Sin expirations NDX — reintentando en 30s...")
                safe_sleep(30)
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error expirations: {e}")
            safe_sleep(30)

    log.info(f"Expirations NDX: {expirations}")

    # ── Timers por capa
    last_weekly  = 0.0
    last_monthly = 0.0
    last_exp_check = 0.0

    # Cache de capas — persiste entre ciclos
    cache = {
        "0dte":    {"strikes": {}, "levels": {}, "strikes_data": []},
        "weekly":  {"strikes": {}, "levels": {}, "strikes_data": []},
        "monthly": {"strikes": {}, "levels": {}, "strikes_data": []},
    }

    cycle = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        now = time.time()
        log.info(f"--- NDX Ciclo {cycle} ---")

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
                    new_exp = get_ndx_expirations(token)
                    if any(new_exp.values()):
                        expirations = new_exp
                        log.info(f"Expirations actualizadas: {expirations}")
                    last_exp_check = now
                except Exception as e:
                    log.warning(f"Error refresh expirations: {e}")

            # ── Precios
            spot_ndx = 0.0
            spot_nq  = 0.0
            try:
                spot_ndx = get_ndx_price(token)
            except Exception as e:
                log.warning(f"Error precio NDX: {e}")
            try:
                spot_nq = get_nq_price(token)
            except Exception as e:
                log.warning(f"Error precio NQ: {e}")

            basis = round(spot_nq - spot_ndx, 2) if spot_nq > 0 and spot_ndx > 0 else BASIS_NQ_NDX_DEFAULT

            # ── CAPA 0DTE — stream cada ciclo (5s)
            if expirations["0dte"] and spot_ndx > 0:
                try:
                    raw = read_stream_ndx(token, expirations["0dte"], spot_ndx)
                    if raw:
                        strikes_data, total_dhp, call_pressure, put_pressure = calculate_gaia(raw, spot_ndx)
                        levels = calculate_levels(strikes_data, spot_ndx)
                        cache["0dte"] = {"strikes": raw, "levels": levels, "strikes_data": strikes_data}
                        log.info(f"0DTE actualizado — {len(raw)} strikes, DHP:{total_dhp}M")
                    else:
                        total_dhp = 0.0
                        call_pressure = 0.0
                        put_pressure = 0.0
                        log.warning("0DTE stream vacío")
                except Exception as e:
                    log.error(f"Error 0DTE stream: {e}")
                    total_dhp = 0.0
                    call_pressure = 0.0
                    put_pressure = 0.0
            else:
                total_dhp = 0.0
                call_pressure = 0.0
                put_pressure = 0.0

            # ── CAPA WEEKLY — REST cada 60s
            if expirations["weekly"] and spot_ndx > 0 and (now - last_weekly) >= REFRESH_WEEKLY:
                try:
                    raw_w = read_rest_ndx(token, expirations["weekly"], spot_ndx)
                    if raw_w:
                        sd_w, _, _, _ = calculate_gaia(raw_w, spot_ndx)
                        lv_w    = calculate_levels(sd_w, spot_ndx)
                        cache["weekly"] = {"strikes": raw_w, "levels": lv_w, "strikes_data": sd_w}
                        log.info(f"Weekly actualizado — {len(raw_w)} strikes")
                    last_weekly = now
                except Exception as e:
                    log.error(f"Error Weekly REST: {e}")

            # ── CAPA MONTHLY — REST cada 300s
            if expirations["monthly"] and spot_ndx > 0 and (now - last_monthly) >= REFRESH_MONTHLY:
                try:
                    raw_m = read_rest_ndx(token, expirations["monthly"], spot_ndx)
                    if raw_m:
                        sd_m, _, _, _ = calculate_gaia(raw_m, spot_ndx)
                        lv_m    = calculate_levels(sd_m, spot_ndx)
                        cache["monthly"] = {"strikes": raw_m, "levels": lv_m, "strikes_data": sd_m}
                        log.info(f"Monthly actualizado — {len(raw_m)} strikes")
                    last_monthly = now
                except Exception as e:
                    log.error(f"Error Monthly REST: {e}")

            # ── CONFLUENCIA — cruza las tres capas
            confluence = calculate_confluence(
                cache["0dte"]["levels"],
                cache["weekly"]["levels"],
                cache["monthly"]["levels"]
            )

            # ── DHP MOMENTUM
            momentum, momentum_dir = calculate_dhp_momentum(total_dhp)

            # ── GAIA FLOW SIGNAL HISTORY
            if spot_ndx > 0:
                flow_history_out = append_flow_history(
                    spot_ndx=spot_ndx,
                    spot_nq=spot_nq,
                    total_dhp=total_dhp,
                    call_pressure=locals().get("call_pressure", 0.0),
                    put_pressure=locals().get("put_pressure", 0.0),
                    momentum=momentum,
                )
            else:
                flow_history_out = []

            # ── GUARDAR JSON + PUSH
            if spot_ndx > 0:
                layers_output = {
                    "0dte":    {
                        "expiration": expirations["0dte"],
                        "levels":     cache["0dte"]["levels"],
                        "strikes":    cache["0dte"]["strikes_data"],
                        "total_dhp":  total_dhp,
                    },
                    "weekly":  {
                        "expiration": expirations["weekly"],
                        "levels":     cache["weekly"]["levels"],
                        "strikes":    cache["weekly"]["strikes_data"],
                    },
                    "monthly": {
                        "expiration": expirations["monthly"],
                        "levels":     cache["monthly"]["levels"],
                        "strikes":    cache["monthly"]["strikes_data"],
                    },
                }
                save_ndx_json(layers_output, spot_ndx, spot_nq, basis,
                              confluence, total_dhp, momentum, momentum_dir, flow_history_out)
                push_to_railway({
                    "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "spot_ndx":      spot_ndx,
                    "spot_nq":       spot_nq,
                    "basis":         basis,
                    "total_dhp":     total_dhp,
                    "dhp_momentum":  momentum,
                    "dhp_direction": momentum_dir,
                    "confluence":    confluence,
                    "history":       flow_history_out,
                    "layers":        layers_output,
                    "status":        "live"
                })
                consecutive_errors = 0
            else:
                log.warning(f"Sin precio NDX — ciclo {cycle} sin output")
                consecutive_errors += 1

            # ── Pausa larga si muchos errores
            if consecutive_errors >= 10:
                log.error("10 errores consecutivos — pausa 120s...")
                safe_sleep(120)
                consecutive_errors = 0

        except KeyboardInterrupt:
            log.info("NDX backend detenido por usuario.")
            break
        except Exception as e:
            log.error(f"Excepcion no prevista ciclo {cycle}: {e}")
            consecutive_errors += 1

        # ── Sleep al final — siempre
        try:
            time.sleep(REFRESH_0DTE)
        except KeyboardInterrupt:
            log.info("NDX backend detenido.")
            break

if __name__ == "__main__":
    main()
