"""
PivotAlphaDesk - GAIA Live Chart Backend v4
ts_gaia_chart.py

Fixes v4 — loop verdaderamente indestructible:
- safe_sleep(): nunca muere en el sleep
- get_token_with_retry(): 5 reintentos con espera, luego pausa larga de 5min
- Arranque bloqueante: espera token y expiracion sin salir nunca
- consecutive_errors: si 10 ciclos seguidos fallan -> pausa 2min + reset token
- time.sleep() al final del loop siempre se ejecuta (no hay continue sin sleep)
- Log de cada ciclo con numero y estado claro
"""

import json, os, time, urllib.parse, urllib.request
import http.client, ssl, logging
from datetime import datetime
from collections import deque

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
TS_CLIENT_ID     = "HMVux6j6ncGeYOVFbWVXyB0lSVL4WWWe"
TS_CLIENT_SECRET = "2Y4SKDlCN0PMX6wbwWLRvcPNeaA7Zl1ygJoSFO9XWWvsCP37xXrF9RzCUBjaddIx"
TOKEN_FILE       = "ts_tokens.json"
TOKEN_URL        = "https://signin.tradestation.com/oauth/token"
API_BASE         = "https://api.tradestation.com/v3"
OUTPUT_FILE      = "gaia_live.json"
LOG_FILE         = "gaia_live.log"
STRIKE_PROXIMITY = 15
REFRESH_SECONDS  = 5
DHP_HISTORY_SIZE = 10

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
RAILWAY_URL   = "https://web-production-49e7.up.railway.app"
RAILWAY_TOKEN = "gaia_push_secret_2026"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
log = logging.getLogger("GAIA")

# ── DHP HISTORY para momentum ─────────────────────────────────────────────────
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
        # Preservar refresh_token anterior si la respuesta nueva no trae uno
        if not tokens.get("refresh_token"):
            existing = load_tokens()
            if existing and existing.get("refresh_token"):
                tokens["refresh_token"] = existing["refresh_token"]
                log.info("refresh_token preservado del token anterior.")
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
    """Lanza ts_auth.py en background y espera hasta que ts_tokens.json tenga token fresco."""
    import subprocess, sys
    log.warning("Lanzando ts_auth.py para re-autenticacion automatica...")
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        auth_script = os.path.join(script_dir, "ts_auth.py")
        proc = subprocess.Popen([sys.executable, auth_script])
        # Espera hasta 3 minutos a que aparezca token nuevo
        deadline = time.time() + 180
        while time.time() < deadline:
            time.sleep(5)
            tokens = load_tokens()
            if tokens and tokens.get("access_token") and tokens.get("saved_at", 0) > time.time() - 30:
                log.info("Re-autenticacion completada — token fresco detectado.")
                proc.terminate()
                return tokens["access_token"]
        log.error("Re-autenticacion timeout — no se detecto token fresco en 3 minutos.")
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
    if elapsed < 900:  # refresca a los 15min — proactivo antes de expirar
        access = tokens.get("access_token")
        if access:
            return access
    log.info("Refrescando token...")
    refresh_tok = tokens.get("refresh_token")
    if not refresh_tok:
        log.warning("refresh_token no encontrado — iniciando re-autenticacion automatica.")
        return reauth_via_script()
    try:
        new_tokens = refresh_token(refresh_tok)
        if not new_tokens.get("access_token"):
            log.error("Token refrescado pero sin access_token en respuesta.")
            return None
        save_tokens(new_tokens)
        log.info("Token refrescado OK.")
        return new_tokens["access_token"]
    except Exception as e:
        log.error(f"Error refrescando token: {e}")
        # Si el error es 401, el refresh_token expiró — re-autenticar automaticamente
        if "401" in str(e):
            log.warning("401 en refresh — refresh_token expirado. Iniciando re-autenticacion automatica.")
            return reauth_via_script()
        return None

# ── API GET ───────────────────────────────────────────────────────────────────
def api_get(endpoint, token, timeout=10):
    url = API_BASE + endpoint
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())

# ── PRECIO SPX ────────────────────────────────────────────────────────────────
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

# ── PRECIO ES ─────────────────────────────────────────────────────────────────
def get_es_price(token):
    try:
        result = api_get("/marketdata/quotes/ESM26", token)
        quotes = result.get("Quotes", [])
        if quotes:
            last = quotes[0].get("Last", 0)
            return float(last) if last else 0.0
    except Exception as e:
        log.warning(f"Error precio ES: {e}")
    return 0.0

# ── EXPIRATIONS ───────────────────────────────────────────────────────────────
def get_best_expiration(token):
    try:
        result = api_get("/marketdata/options/expirations/%24SPXW.X", token)
        expirations = result.get("Expirations", [])
        if not expirations:
            return ""
        today = datetime.now().strftime("%Y-%m-%d")
        for exp in expirations:
            exp_date = exp.get("Date", "")[:10]
            if exp_date == today:
                log.info(f"0DTE detectado: {exp_date}")
                return exp_date
        first = expirations[0].get("Date", "")[:10]
        log.info(f"Usando proxima expiracion: {first}")
        return first
    except Exception as e:
        log.error(f"Error expirations: {e}")
    return ""

# ── STREAM ────────────────────────────────────────────────────────────────────
def read_stream(token, expiration, spot):
    strikes = {}
    params = "?" + urllib.parse.urlencode({
        "expiration":      expiration,
        "strikeProximity": STRIKE_PROXIMITY,
    })
    url = f"/v3/marketdata/stream/options/chains/%24SPXW.X{params}"
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
            log.error(f"Stream status: {resp.status}")
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

            side   = data.get("Side", "")
            volume = int(data.get("Volume", 0) or 0)
            oi     = int(data.get("DailyOpenInterest", 0) or 0)
            gamma  = float(data.get("Gamma", 0) or 0)
            delta  = float(data.get("Delta", 0) or 0)
            iv     = float(data.get("ImpliedVolatility", 0) or 0)

            legs = data.get("Legs", [])
            if not legs:
                lines_read += 1
                continue
            try:
                strike = int(float(legs[0].get("StrikePrice", "0")))
            except Exception:
                lines_read += 1
                continue

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
            lines_read += 1

    except Exception as e:
        log.error(f"Stream error: {e}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
    return strikes

# ── CALCULOS GEX Y DHP ────────────────────────────────────────────────────────
def calculate_gaia(strikes, spot):
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

    total_dhp = round((total_call_dhp + total_put_dhp) / 1e6, 2)
    return results, total_dhp

# ── NIVELES MEJORADOS ─────────────────────────────────────────────────────────
def calculate_levels(strikes_data, spot):
    if not strikes_data:
        return {}

    above = [s for s in strikes_data if s["strike"] >= spot]
    below = [s for s in strikes_data if s["strike"] <  spot]

    # Call Wall: mayor call_gex ARRIBA del precio
    call_wall = max(above, key=lambda s: s["call_gex"]) if above else max(strikes_data, key=lambda s: s["call_gex"])

    # Put Wall: mayor abs(put_gex) ABAJO del precio
    put_wall = min(below, key=lambda s: s["put_gex"]) if below else min(strikes_data, key=lambda s: s["put_gex"])

    # Gamma Node: mayor liquidez bilateral (call + put combinados)
    gamma_node = max(strikes_data, key=lambda s: s["call_gex"] + abs(s["put_gex"]))

    # Gamma Flip: primer cruce de net_gex negativo a positivo
    sorted_s = sorted(strikes_data, key=lambda s: s["strike"])
    flip_strike = None
    for i in range(1, len(sorted_s)):
        if sorted_s[i-1]["net_gex"] < 0 and sorted_s[i]["net_gex"] >= 0:
            flip_strike = sorted_s[i]["strike"]
            break
    if not flip_strike:
        flip_strike = min(sorted_s, key=lambda s: abs(s["net_gex"]))["strike"]

    # Gravity Pin: mayor OI total
    gravity_pin = max(strikes_data, key=lambda s: s["call_oi"] + s["put_oi"])

    return {
        "call_wall":   call_wall["strike"],
        "put_wall":    put_wall["strike"],
        "gamma_node":  gamma_node["strike"],
        "gamma_flip":  flip_strike,
        "gravity_pin": gravity_pin["strike"],
    }

# ── DHP MOMENTUM ──────────────────────────────────────────────────────────────
def calculate_dhp_momentum(current_dhp):
    dhp_history.append(current_dhp)
    if len(dhp_history) < 2:
        return 0.0, "NEUTRAL"

    recent  = list(dhp_history)[-3:]
    older   = list(dhp_history)[:-3]
    avg_r   = sum(recent) / len(recent)
    avg_o   = sum(older) / len(older) if older else avg_r
    momentum = round(avg_r - avg_o, 2)

    if momentum > 50:       direction = "ACCELERATING_BULL"
    elif momentum > 10:     direction = "BUILDING_BULL"
    elif momentum < -50:    direction = "ACCELERATING_BEAR"
    elif momentum < -10:    direction = "BUILDING_BEAR"
    else:                   direction = "NEUTRAL"

    return momentum, direction

# ── GUARDAR JSON ATOMICO ──────────────────────────────────────────────────────
def save_gaia_json(strikes_data, total_dhp, spot, spot_es, expiration, levels, momentum, momentum_dir):
    try:
        # Basis empírico en tiempo real
        basis = round(spot_es - spot, 2) if spot_es > 0 else 0.0

        # Niveles ES = niveles SPX + basis
        levels_es = {k: round(v + basis, 2) for k, v in levels.items()} if basis != 0 else levels.copy()

        output = {
            "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "expiration":    expiration,
            "spot_spx":      spot,
            "spot_es":       spot_es if spot_es > 0 else spot,
            "basis":         basis,
            "total_dhp":     total_dhp,
            "dhp_momentum":  momentum,
            "dhp_direction": momentum_dir,
            "levels":        levels,
            "levels_es":     levels_es,
            "strikes":       strikes_data,
            "status":        "live"
        }
        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(output, f, indent=2)
        os.replace(tmp, OUTPUT_FILE)
        log.info(
            f"SPX:{spot} ES:{spot_es} BASIS:{basis} DHP:{total_dhp}M [{momentum_dir}] "
            f"CW:{levels.get('call_wall')} PW:{levels.get('put_wall')} "
            f"Node:{levels.get('gamma_node')} Pin:{levels.get('gravity_pin')} "
            f"Strikes:{len(strikes_data)}"
        )
    except Exception as e:
        log.error(f"Error guardando JSON: {e}")

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
def push_to_railway(data: dict):
    """Empuja gaia_live.json a Railway vía HTTP POST cada ciclo."""
    try:
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(
            RAILWAY_URL + "/push",
            data=body,
            method="POST"
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Push-Token", RAILWAY_TOKEN)
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                pass  # silencioso en éxito
            else:
                log.warning(f"Railway push status: {resp.status}")
    except Exception as e:
        log.warning(f"Railway push error: {e}")

# ── CONSTANTES DE RECUPERACION ────────────────────────────────────────────────
MAX_TOKEN_RETRIES    = 5       # reintentos antes de espera larga
TOKEN_RETRY_WAIT     = 60      # segundos entre reintentos de token
TOKEN_LONGWAIT       = 300     # espera larga si todos los reintentos fallan
MAX_CYCLE_ERRORS     = 10      # errores consecutivos antes de pausa larga
CYCLE_ERROR_WAIT     = 120     # pausa si hay demasiados errores seguidos

# ── SAFE SLEEP — nunca muere en sleep ─────────────────────────────────────────
def safe_sleep(seconds):
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise
    except Exception:
        pass

# ── TOKEN CON REINTENTO AUTOMATICO ────────────────────────────────────────────
def get_token_with_retry():
    """Intenta obtener token hasta MAX_TOKEN_RETRIES veces antes de rendirse."""
    for attempt in range(1, MAX_TOKEN_RETRIES + 1):
        try:
            token = get_valid_token()
            if token:
                return token
            log.warning(f"Token intento {attempt}/{MAX_TOKEN_RETRIES} fallido — esperando {TOKEN_RETRY_WAIT}s...")
        except Exception as e:
            log.error(f"Token excepcion intento {attempt}/{MAX_TOKEN_RETRIES}: {e}")
        if attempt < MAX_TOKEN_RETRIES:
            safe_sleep(TOKEN_RETRY_WAIT)
    log.error(f"Token no disponible tras {MAX_TOKEN_RETRIES} intentos — esperando {TOKEN_LONGWAIT}s antes de reintentar...")
    safe_sleep(TOKEN_LONGWAIT)
    return None

# ── MAIN LOOP INDESTRUCTIBLE v4 ───────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA Live Chart Backend v4")
    log.info("  Loop auto-recuperable — nunca muere solo")
    log.info("=" * 60)

    # Arranque inicial — espera hasta tener token
    token = None
    while not token:
        try:
            token = get_token_with_retry()
        except KeyboardInterrupt:
            log.info("Detenido por el usuario en arranque.")
            return
        except Exception as e:
            log.error(f"Error critico en arranque: {e} — reintentando en 60s...")
            safe_sleep(60)

    # Expiracion inicial — espera hasta obtenerla
    expiration = ""
    while not expiration:
        try:
            expiration = get_best_expiration(token)
            if not expiration:
                log.warning("No se pudo obtener expiracion — reintentando en 30s...")
                safe_sleep(30)
        except KeyboardInterrupt:
            log.info("Detenido por el usuario.")
            return
        except Exception as e:
            log.error(f"Error obteniendo expiracion: {e} — reintentando en 30s...")
            safe_sleep(30)

    log.info(f"Expiracion activa: {expiration}")
    log.info(f"Loop cada {REFRESH_SECONDS}s. Ctrl+C para detener.")

    cycle          = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        log.info(f"--- Ciclo {cycle} ---")

        try:
            # ── TOKEN ──────────────────────────────────────────────────────────
            try:
                new_token = get_valid_token()
                if new_token:
                    token = new_token
                else:
                    log.warning("Token expiro — iniciando recuperacion...")
                    recovered = get_token_with_retry()
                    if recovered:
                        token = recovered
                        log.info("Token recuperado OK.")
                    else:
                        log.error("No se pudo recuperar token — ciclo saltado.")
                        consecutive_errors += 1
                        safe_sleep(REFRESH_SECONDS)
                        continue
            except Exception as e:
                log.error(f"Excepcion en token ciclo {cycle}: {e}")
                consecutive_errors += 1
                safe_sleep(REFRESH_SECONDS)
                continue

            # ── EXPIRACION (cada 10 ciclos) ────────────────────────────────────
            if cycle % 10 == 0:
                try:
                    new_exp = get_best_expiration(token)
                    if new_exp and new_exp != expiration:
                        log.info(f"Expiracion actualizada: {expiration} -> {new_exp}")
                        expiration = new_exp
                except Exception as e:
                    log.warning(f"No se pudo actualizar expiracion: {e} — usando {expiration}")

            # ── PRECIO SPX ─────────────────────────────────────────────────────
            spot = 0.0
            try:
                spot = get_spx_price(token)
                if spot <= 0:
                    log.warning("Precio SPX = 0 o invalido.")
            except Exception as e:
                log.warning(f"Error precio SPX: {e}")

            # ── PRECIO ES ──────────────────────────────────────────────────────
            spot_es = 0.0
            try:
                spot_es = get_es_price(token)
                if spot_es <= 0:
                    log.warning("Precio ES = 0 o invalido — usando SPX como fallback.")
            except Exception as e:
                log.warning(f"Error precio ES: {e}")

            # ── STREAM ─────────────────────────────────────────────────────────
            raw_strikes = {}
            try:
                raw_strikes = read_stream(token, expiration, spot)
            except Exception as e:
                log.error(f"Error stream ciclo {cycle}: {e}")

            # ── CALCULOS Y GUARDADO ────────────────────────────────────────────
            if raw_strikes and spot > 0:
                try:
                    strikes_data, total_dhp = calculate_gaia(raw_strikes, spot)
                    levels                  = calculate_levels(strikes_data, spot)
                    momentum, momentum_dir  = calculate_dhp_momentum(total_dhp)
                    save_gaia_json(strikes_data, total_dhp, spot, spot_es, expiration,
                                   levels, momentum, momentum_dir)
                    basis    = round(spot_es - spot, 2) if spot_es > 0 else 0.0
                    levels_es = {k: round(v + basis, 2) for k, v in levels.items()} if basis != 0 else levels.copy()
                    push_to_railway({
                        "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "expiration":    expiration,
                        "spot_spx":      spot,
                        "spot_es":       spot_es if spot_es > 0 else spot,
                        "basis":         basis,
                        "total_dhp":     total_dhp,
                        "dhp_momentum":  momentum,
                        "dhp_direction": momentum_dir,
                        "levels":        levels,
                        "levels_es":     levels_es,
                        "strikes":       strikes_data,
                        "status":        "live"
                    })
                    consecutive_errors = 0  # reset en exito
                except Exception as e:
                    log.error(f"Error calculos ciclo {cycle}: {e}")
                    consecutive_errors += 1
            else:
                log.warning(f"Sin datos utiles — strikes:{len(raw_strikes)} spot:{spot}")
                consecutive_errors += 1

            # ── PAUSA LARGA SI MUCHOS ERRORES SEGUIDOS ─────────────────────────
            if consecutive_errors >= MAX_CYCLE_ERRORS:
                log.error(f"{consecutive_errors} errores consecutivos — pausa {CYCLE_ERROR_WAIT}s para estabilizar...")
                safe_sleep(CYCLE_ERROR_WAIT)
                consecutive_errors = 0
                # Forzar refresh de token y expiracion
                try:
                    token = get_token_with_retry() or token
                    expiration = get_best_expiration(token) or expiration
                except Exception:
                    pass

        except KeyboardInterrupt:
            log.info("Detenido por el usuario.")
            break
        except Exception as e:
            log.error(f"Excepcion no prevista ciclo {cycle}: {e}")
            consecutive_errors += 1

        # ── SLEEP AL FINAL — siempre se ejecuta ───────────────────────────────
        try:
            time.sleep(REFRESH_SECONDS)
        except KeyboardInterrupt:
            log.info("Detenido por el usuario.")
            break

if __name__ == "__main__":
    main()
