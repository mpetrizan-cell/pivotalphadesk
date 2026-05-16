"""
PivotAlphaDesk - GAIA SPX Chart Backend v5
ts_gaia_chart.py

Arquitectura 3 capas — igual que NDX:
  Layer 1: 0DTE        → stream cada 5s   (SPXW, vence hoy)
  Layer 2: Weekly      → REST cada 60s    (SPXW, vence esta semana)
  Layer 3: Monthly     → REST cada 300s   (SPXW, vence este mes)

Nuevas capas por strike:
  - Call OI / Put OI separados
  - Call GEX / Put GEX / NET GEX separados
  - Call IV / Put IV separados
  - Call Gamma / Put Gamma separados
  - Call Delta×OI / Put Delta×OI separados
  - Confluencia entre las 3 capas
  - Score de predicción por strike

JSON output: gaia_live.json
Railway push: /push
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
OUTPUT_FILE      = "gaia_live.json"
LOG_FILE         = "gaia_live.log"

SPX_SYMBOL       = "$SPXW.X"
ES_SYMBOL        = "ESM26"
STRIKE_PROXIMITY = 15

# Refresh diferenciado por capa
REFRESH_0DTE     = 5
REFRESH_WEEKLY   = 60
REFRESH_MONTHLY  = 300

DHP_HISTORY_SIZE = 10

# ── RAILWAY ───────────────────────────────────────────────────────────────────
RAILWAY_URL   = "https://web-production-49e7.up.railway.app"
RAILWAY_TOKEN = "gaia_push_secret_2026"

# ── CONSTANTES DE RECUPERACION ────────────────────────────────────────────────
MAX_TOKEN_RETRIES = 5
TOKEN_RETRY_WAIT  = 60
TOKEN_LONGWAIT    = 300
MAX_CYCLE_ERRORS  = 10
CYCLE_ERROR_WAIT  = 120

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
log = logging.getLogger("GAIA_SPX")

dhp_history = deque(maxlen=DHP_HISTORY_SIZE)

# ── TOKEN ─────────────────────────────────────────────────────────────────────
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

def reauth_via_script():
    import subprocess, sys
    log.warning("Lanzando ts_auth.py para re-autenticacion automatica...")
    try:
        script_dir  = os.path.dirname(os.path.abspath(__file__))
        auth_script = os.path.join(script_dir, "ts_auth.py")
        proc = subprocess.Popen([sys.executable, auth_script])
        deadline = time.time() + 180
        while time.time() < deadline:
            time.sleep(5)
            tokens = load_tokens()
            if tokens and tokens.get("access_token") and tokens.get("saved_at", 0) > time.time() - 30:
                log.info("Re-autenticacion completada.")
                proc.terminate()
                return tokens["access_token"]
        log.error("Re-autenticacion timeout.")
        proc.terminate()
    except Exception as e:
        log.error(f"Error en reauth_via_script: {e}")
    return None

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
    log.info("Refrescando token SPX...")
    refresh_tok = tokens.get("refresh_token")
    if not refresh_tok:
        log.warning("Sin refresh_token — iniciando re-autenticacion.")
        return reauth_via_script()
    try:
        new_tokens = refresh_token(refresh_tok)
        if not new_tokens.get("access_token"):
            return None
        # Push fresh token to Railway so /bars works
        try:
            push_to_railway({
                "_token_update": True,
                "access_token": new_tokens.get("access_token"),
                "refresh_token": new_tokens.get("refresh_token", refresh_tok),
                "saved_at": new_tokens.get("saved_at", 0),
                "expires_in": new_tokens.get("expires_in", 1200),
            })
            log.info("Token pusheado a Railway OK")
        except Exception as e:
            log.warning(f"Token push error: {e}")
        save_tokens(new_tokens)
        log.info("Token refrescado OK.")
        return new_tokens["access_token"]
    except Exception as e:
        log.error(f"Error refrescando token: {e}")
        if "401" in str(e):
            return reauth_via_script()
        return None

def safe_sleep(seconds):
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise
    except Exception:
        pass

def get_token_with_retry():
    for attempt in range(1, MAX_TOKEN_RETRIES + 1):
        try:
            token = get_valid_token()
            if token:
                return token
            log.warning(f"Token intento {attempt}/{MAX_TOKEN_RETRIES} fallido...")
        except Exception as e:
            log.error(f"Token excepcion intento {attempt}: {e}")
        if attempt < MAX_TOKEN_RETRIES:
            safe_sleep(TOKEN_RETRY_WAIT)
    log.error(f"Token no disponible — esperando {TOKEN_LONGWAIT}s...")
    safe_sleep(TOKEN_LONGWAIT)
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
def get_spx_price(token):
    try:
        result = api_get("/marketdata/quotes/%24SPXW.X", token)
        quotes = result.get("Quotes", [])
        if quotes:
            last = quotes[0].get("Last", 0)
            return float(last) if last else 0.0
    except Exception as e:
        log.warning(f"Error precio SPX: {e}")
    return 0.0

def get_es_price(token):
    try:
        result = api_get(f"/marketdata/quotes/{ES_SYMBOL}", token)
        quotes = result.get("Quotes", [])
        if quotes:
            last = quotes[0].get("Last", 0)
            return float(last) if last else 0.0
    except Exception as e:
        log.warning(f"Error precio ES: {e}")
    return 0.0

# ── EXPIRATIONS — 3 CAPAS ─────────────────────────────────────────────────────
def get_spx_expirations(token):
    """
    Retorna dict con tres capas:
      { "0dte": "2026-05-08", "weekly": "2026-05-09", "monthly": "2026-05-15" }
    """
    layers = {"0dte": None, "weekly": None, "monthly": None}
    try:
        sym_enc = urllib.parse.quote(SPX_SYMBOL, safe="")
        result  = api_get(f"/marketdata/options/expirations/{sym_enc}", token)
        expirations = result.get("Expirations", [])
        if not expirations:
            return layers

        today     = datetime.now().date()
        week_end  = today + timedelta(days=(4 - today.weekday()) % 7)  # viernes
        month_end = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        for exp in expirations:
            exp_date_str = exp.get("Date", "")[:10]
            try:
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if exp_date == today and not layers["0dte"]:
                layers["0dte"] = exp_date_str
                log.info(f"SPX 0DTE detectado: {exp_date_str}")
            elif exp_date <= week_end and exp_date > today and not layers["weekly"]:
                layers["weekly"] = exp_date_str
                log.info(f"SPX Weekly detectado: {exp_date_str}")
            elif exp_date <= month_end and exp_date > week_end and not layers["monthly"]:
                layers["monthly"] = exp_date_str
                log.info(f"SPX Monthly detectado: {exp_date_str}")

            if all(layers.values()):
                break

        # Fallbacks si no hay 0DTE (fin de semana / holiday)
        if not layers["0dte"] and expirations:
            layers["0dte"] = expirations[0].get("Date", "")[:10]
            log.info(f"SPX 0DTE fallback: {layers['0dte']}")
        if not layers["weekly"] and len(expirations) > 1:
            layers["weekly"] = expirations[1].get("Date", "")[:10]
        if not layers["monthly"] and len(expirations) > 2:
            layers["monthly"] = expirations[2].get("Date", "")[:10]

    except Exception as e:
        log.error(f"Error obteniendo expirations SPX: {e}")

    return layers

# ── PARSER COMUN — igual que NDX ─────────────────────────────────────────────
def _parse_option_line(data, strikes):
    """Parser de una línea del stream — extrae Call/Put por separado."""
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

# ── STREAM SPX ────────────────────────────────────────────────────────────────
def read_stream_spx(token, expiration, spot):
    strikes = {}
    params = "?" + urllib.parse.urlencode({
        "expiration":      expiration,
        "strikeProximity": STRIKE_PROXIMITY,
    })
    sym_enc = urllib.parse.quote(SPX_SYMBOL, safe="")
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
            log.error(f"Stream SPX status: {resp.status}")
            return strikes

        lines_read    = 0
        max_contracts = STRIKE_PROXIMITY * 2 * 2 + 5
        heartbeats    = 0
        max_heartbeat = 8

        while lines_read < max_contracts and heartbeats < max_heartbeat:
            try:
                raw = resp.readline().decode("utf-8").strip()
            except Exception as e:
                log.warning(f"Stream readline error: {e}")
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
        log.error(f"Stream SPX error: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
    return strikes

# Weekly/Monthly usan el mismo stream con expiration diferente
def read_rest_spx(token, expiration, spot):
    return read_stream_spx(token, expiration, spot)

# ── CALCULOS GEX / DHP ────────────────────────────────────────────────────────
def calculate_gaia(strikes, spot):
    """
    Calcula GEX, DHP, Delta×OI por strike.
    Retorna (strikes_data, total_dhp, total_call_dhp, total_put_dhp)
    """
    spot2  = spot * spot
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

        # Delta × OI = presión real de hedging
        call_delta_oi = round(s["call_delta"] * s["call_oi"], 2)
        put_delta_oi  = round(s["put_delta"]  * s["put_oi"],  2)

        results.append({
            "strike":        strike,
            # GEX separado
            "call_gex":      round(call_gex / 1e6, 2),
            "put_gex":       round(put_gex  / 1e6, 2),
            "net_gex":       round(net_gex  / 1e6, 2),
            # DHP separado
            "call_dhp":      round(call_dhp / 1e6, 2),
            "put_dhp":       round(put_dhp  / 1e6, 2),
            "net_dhp":       round(net_dhp  / 1e6, 2),
            # OI separado
            "call_oi":       s["call_oi"],
            "put_oi":        s["put_oi"],
            # Delta separado
            "call_delta":    round(s["call_delta"], 4),
            "put_delta":     round(s["put_delta"],  4),
            # Gamma separado
            "call_gamma":    round(s["call_gamma"], 6),
            "put_gamma":     round(s["put_gamma"],  6),
            # Delta×OI = presión real
            "call_delta_oi": call_delta_oi,
            "put_delta_oi":  put_delta_oi,
            # IV separado (en %)
            "call_iv":       round(s["call_iv"] * 100, 2),
            "put_iv":        round(s["put_iv"]  * 100, 2),
        })

    total_dhp = round((total_call_dhp + total_put_dhp) / 1e6, 2)
    return results, total_dhp

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

# ── CONFLUENCIA — igual que NDX ───────────────────────────────────────────────
def calculate_confluence(levels_0dte, levels_weekly, levels_monthly):
    """
    Detecta strikes que son nivel PAD en 2+ capas.
    strength = número de capas que coinciden (2 = fuerte, 3 = muy fuerte)
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
                "strike":   strike,
                "layers":   info["layers"],
                "types":    list(set(info["types"])),
                "strength": len(info["layers"])
            })

    return sorted(confluence, key=lambda x: x["strike"])

# ── PREDICTION SCORE por strike ───────────────────────────────────────────────
def calculate_prediction_score(strikes_data, spot, confluence):
    """
    Score de 0-100 por strike indicando probabilidad de reacción.
    Basado en: Gamma alto + Δ×OI alto + IV skew + confluencia de capas.
    """
    if not strikes_data:
        return {}

    confluence_strikes = {c["strike"]: c["strength"] for c in confluence}

    max_gamma = max((max(s["call_gamma"], s["put_gamma"]) for s in strikes_data), default=0.00001) or 0.00001
    max_doi   = max((max(abs(s["call_delta_oi"]), abs(s["put_delta_oi"])) for s in strikes_data), default=1) or 1
    max_oi    = max((s["call_oi"] + s["put_oi"] for s in strikes_data), default=1) or 1

    scores = {}
    for s in strikes_data:
        strike = s["strike"]

        # Gamma score (0-30) — qué tan explosivo
        gamma_max  = max(s["call_gamma"], s["put_gamma"])
        gamma_score = (gamma_max / max_gamma) * 30

        # Delta×OI score (0-25) — presión real de hedging
        doi_max  = max(abs(s["call_delta_oi"]), abs(s["put_delta_oi"]))
        doi_score = (doi_max / max_doi) * 25

        # IV skew score (0-20) — flujo nuevo vs viejo
        call_iv = s["call_iv"] or 0
        put_iv  = s["put_iv"]  or 0
        iv_skew = abs(call_iv - put_iv)
        iv_score = min(iv_skew * 2, 20)  # cada 1% de skew = 2 pts, max 20

        # OI score (0-15) — posición estructural
        oi_total  = s["call_oi"] + s["put_oi"]
        oi_score  = (oi_total / max_oi) * 15

        # Confluencia score (0-10) — capas que coinciden
        conf_strength = confluence_strikes.get(strike, 0)
        conf_score    = min(conf_strength * 5, 10)

        total = round(gamma_score + doi_score + iv_score + oi_score + conf_score, 1)
        scores[strike] = min(total, 100)

    return scores

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

# ── GUARDAR JSON ATOMICO ──────────────────────────────────────────────────────
def save_gaia_json(layers_output, spot, spot_es, confluence,
                   total_dhp, momentum, momentum_dir,
                   levels_0dte, expirations):
    try:
        basis    = round(spot_es - spot, 2) if spot_es > 0 else 0.0
        levels_es = {k: round(v + basis, 2) for k, v in levels_0dte.items()} if basis != 0 else levels_0dte.copy()

        # Strikes activos = 0DTE (principal)
        strikes_0dte = layers_output.get("0dte", {}).get("strikes_data", [])

        # Prediction scores
        scores = calculate_prediction_score(strikes_0dte, spot, confluence)

        # Agregar score a cada strike
        strikes_with_score = []
        for s in strikes_0dte:
            s_copy = dict(s)
            s_copy["prediction_score"] = scores.get(s["strike"], 0)
            strikes_with_score.append(s_copy)

        output = {
            "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "expiration":    expirations.get("0dte", ""),
            "spot_spx":      spot,
            "spot_es":       spot_es if spot_es > 0 else spot,
            "basis":         basis,
            "total_dhp":     total_dhp,
            "dhp_momentum":  momentum,
            "dhp_direction": momentum_dir,
            "hiro_call":     round(hiro_call_accum / 1e6, 2),
            "hiro_put":      round(hiro_put_accum  / 1e6, 2),
            "hiro_total":    round((hiro_call_accum + hiro_put_accum) / 1e6, 2),
            "levels":        levels_0dte,
            "levels_es":     levels_es,
            "confluence":    confluence,
            "strikes":       strikes_with_score,   # 0DTE con scores
            "layers": {
                "0dte":    {
                    "expiration": expirations.get("0dte"),
                    "levels":     levels_0dte,
                    "strikes":    strikes_with_score,
                    "total_dhp":  total_dhp,
                },
                "weekly":  {
                    "expiration": expirations.get("weekly"),
                    "levels":     layers_output.get("weekly", {}).get("levels", {}),
                    "strikes":    layers_output.get("weekly", {}).get("strikes_data", []),
                },
                "monthly": {
                    "expiration": expirations.get("monthly"),
                    "levels":     layers_output.get("monthly", {}).get("levels", {}),
                    "strikes":    layers_output.get("monthly", {}).get("strikes_data", []),
                },
            },
            "status": "live"
        }

        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(output, f, indent=2)
        os.replace(tmp, OUTPUT_FILE)

        conf_str = " | ".join([f"{c['strike']}({c['strength']}x)" for c in confluence]) or "none"
        log.info(
            f"SPX:{spot} ES:{spot_es} BASIS:{basis} DHP:{total_dhp}M [{momentum_dir}] "
            f"CW:{levels_0dte.get('call_wall')} PW:{levels_0dte.get('put_wall')} "
            f"Node:{levels_0dte.get('gamma_node')} Strikes:{len(strikes_0dte)} "
            f"Confluencia: {conf_str}"
        )
    except Exception as e:
        log.error(f"Error guardando JSON: {e}")

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
def push_to_railway(data: dict):
    try:
        body = json.dumps(data).encode("utf-8")
        req  = urllib.request.Request(RAILWAY_URL + "/push", data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Push-Token", RAILWAY_TOKEN)
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status != 200:
                log.warning(f"Railway push status: {resp.status}")
    except Exception as e:
        log.warning(f"Railway push error: {e}")

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA SPX Backend v5")
    log.info("  3 capas: 0DTE(5s) / Weekly(60s) / Monthly(300s)")
    log.info("  Nuevas capas: Call/Put OI/GEX/IV/Gamma + Confluencia + Score")
    log.info("=" * 60)

    # ── Token inicial
    token = None
    while not token:
        try:
            token = get_token_with_retry()
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error critico arranque: {e}")
            safe_sleep(60)

    # ── Expirations iniciales
    expirations = {"0dte": None, "weekly": None, "monthly": None}
    while not any(expirations.values()):
        try:
            expirations = get_spx_expirations(token)
            if not any(expirations.values()):
                log.warning("Sin expirations SPX — reintentando en 30s...")
                safe_sleep(30)
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error expirations: {e}")
            safe_sleep(30)

    log.info(f"Expirations SPX: {expirations}")

    # ── Timers por capa
    last_weekly    = 0.0
    last_monthly   = 0.0
    last_exp_check = 0.0

    # Cache de capas — persiste entre ciclos
    cache = {
        "0dte":    {"strikes": {}, "levels": {}, "strikes_data": []},
        "weekly":  {"strikes": {}, "levels": {}, "strikes_data": []},
        "monthly": {"strikes": {}, "levels": {}, "strikes_data": []},
    }

    # ── HIRO accumulators — reset at session start (9:30 ET)
    import datetime as _dt
    hiro_call_accum = 0.0
    hiro_put_accum  = 0.0
    hiro_reset_date = None  # tracks which trading day we're on

    cycle = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        now = time.time()
        log.info(f"--- Ciclo {cycle} ---")

        # ── HIRO daily reset at 9:30 ET
        try:
            today = _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%d')
            if hiro_reset_date != today:
                hiro_call_accum = 0.0
                hiro_put_accum  = 0.0
                hiro_reset_date = today
                log.info(f"HIRO reset for {today}")
        except Exception:
            pass

        try:
            # ── Token
            try:
                new_token = get_valid_token()
                if new_token:
                    token = new_token
                else:
                    recovered = get_token_with_retry()
                    if recovered:
                        token = recovered
                    else:
                        consecutive_errors += 1
                        safe_sleep(REFRESH_0DTE)
                        continue
            except Exception as e:
                log.error(f"Excepcion token ciclo {cycle}: {e}")
                consecutive_errors += 1
                safe_sleep(REFRESH_0DTE)
                continue

            # ── Refresh expirations cada 10 min
            if now - last_exp_check > 600:
                try:
                    new_exp = get_spx_expirations(token)
                    if any(new_exp.values()):
                        expirations = new_exp
                        log.info(f"Expirations actualizadas: {expirations}")
                    last_exp_check = now
                except Exception as e:
                    log.warning(f"Error refresh expirations: {e}")

            # ── Precios
            spot    = 0.0
            spot_es = 0.0
            try:
                spot    = get_spx_price(token)
                spot_es = get_es_price(token)
            except Exception as e:
                log.warning(f"Error precios: {e}")

            # ── CAPA 0DTE — stream cada ciclo (5s)
            total_dhp = 0.0
            if expirations["0dte"] and spot > 0:
                try:
                    raw = read_stream_spx(token, expirations["0dte"], spot)
                    if raw:
                        strikes_data, total_dhp = calculate_gaia(raw, spot)
                        levels = calculate_levels(strikes_data, spot)
                        cache["0dte"] = {"strikes": raw, "levels": levels, "strikes_data": strikes_data}
                        # ── HIRO accumulation (intraday, like SpotGamma)
                        cycle_call = sum(s.get("call_dhp",0) for s in strikes_data)
                        cycle_put  = sum(s.get("put_dhp",0) for s in strikes_data)
                        hiro_call_accum += cycle_call
                        hiro_put_accum  += cycle_put
                        log.info(f"0DTE actualizado — {len(raw)} strikes, DHP:{total_dhp}M")
                    else:
                        log.warning("0DTE stream vacío")
                except Exception as e:
                    log.error(f"Error 0DTE stream: {e}")

            # ── CAPA WEEKLY — REST cada 60s
            if expirations["weekly"] and spot > 0 and (now - last_weekly) >= REFRESH_WEEKLY:
                try:
                    raw_w = read_rest_spx(token, expirations["weekly"], spot)
                    if raw_w:
                        sd_w, _ = calculate_gaia(raw_w, spot)
                        lv_w    = calculate_levels(sd_w, spot)
                        cache["weekly"] = {"strikes": raw_w, "levels": lv_w, "strikes_data": sd_w}
                        log.info(f"Weekly actualizado — {len(raw_w)} strikes")
                    last_weekly = now
                except Exception as e:
                    log.error(f"Error Weekly REST: {e}")

            # ── CAPA MONTHLY — REST cada 300s
            if expirations["monthly"] and spot > 0 and (now - last_monthly) >= REFRESH_MONTHLY:
                try:
                    raw_m = read_rest_spx(token, expirations["monthly"], spot)
                    if raw_m:
                        sd_m, _ = calculate_gaia(raw_m, spot)
                        lv_m    = calculate_levels(sd_m, spot)
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

            # ── GUARDAR JSON + PUSH
            if spot > 0 and cache["0dte"]["strikes_data"]:
                try:
                    save_gaia_json(
                        cache, spot, spot_es, confluence,
                        total_dhp, momentum, momentum_dir,
                        cache["0dte"]["levels"], expirations
                    )
                    basis     = round(spot_es - spot, 2) if spot_es > 0 else 0.0
                    levels_es = {k: round(v + basis, 2) for k, v in cache["0dte"]["levels"].items()} if basis else cache["0dte"]["levels"].copy()

                    push_to_railway({
                        "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "expiration":    expirations.get("0dte", ""),
                        "spot_spx":      spot,
                        "spot_es":       spot_es if spot_es > 0 else spot,
                        "basis":         basis,
                        "total_dhp":     total_dhp,
                        "dhp_momentum":  momentum,
                        "dhp_direction": momentum_dir,
                        "levels":        cache["0dte"]["levels"],
                        "levels_es":     levels_es,
                        "confluence":    confluence,
                        "strikes":       cache["0dte"]["strikes_data"],
                        "layers": {
                            "0dte":    {"expiration": expirations.get("0dte"),    "levels": cache["0dte"]["levels"]},
                            "weekly":  {"expiration": expirations.get("weekly"),  "levels": cache["weekly"]["levels"]},
                            "monthly": {"expiration": expirations.get("monthly"), "levels": cache["monthly"]["levels"]},
                        },
                        "status": "live"
                    })
                    consecutive_errors = 0
                except Exception as e:
                    log.error(f"Error guardando/push ciclo {cycle}: {e}")
                    consecutive_errors += 1
            else:
                log.warning(f"Sin datos útiles — spot:{spot} strikes:{len(cache['0dte']['strikes_data'])}")
                consecutive_errors += 1

            # ── Pausa larga si muchos errores
            if consecutive_errors >= MAX_CYCLE_ERRORS:
                log.error(f"{consecutive_errors} errores — pausa {CYCLE_ERROR_WAIT}s...")
                safe_sleep(CYCLE_ERROR_WAIT)
                consecutive_errors = 0
                try:
                    token = get_token_with_retry() or token
                    expirations = get_spx_expirations(token) or expirations
                except Exception:
                    pass

        except KeyboardInterrupt:
            log.info("SPX backend detenido por usuario.")
            break
        except Exception as e:
            log.error(f"Excepcion no prevista ciclo {cycle}: {e}")
            consecutive_errors += 1

        # ── Sleep al final — siempre
        try:
            time.sleep(REFRESH_0DTE)
        except KeyboardInterrupt:
            log.info("SPX backend detenido.")
            break

if __name__ == "__main__":
    main()
