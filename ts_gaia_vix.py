"""
PivotAlphaDesk - GAIA VIX Backend v1
ts_gaia_vix.py

Fase 1: solo pipeline de VIX (Call Wall, Put Wall, Gamma Flip, DHP propio).
NO incluye todavía la relación cruzada VIX <-> SPX — eso se evalúa después
de tener el pipeline corriendo unos días con datos reales.

Fase 1A (20-ago): agregado feed propio de VIX1D.X (last/change/change_pct/
bid/ask) + ratio VIX1D/VIX, según Plan de Desarrollo GAIA Vol Engine V1.
No se calcula todavía ninguna interpretación (BUILDING/AMPLIFYING/etc.) —
eso es Fase 5, pendiente de validación con datos reales primero.

Fix 20-ago: VIX_SYMBOL estaba guardado pre-codificado ("%24VIX.X"), lo que
causaba doble-encoding en get_price()/get_expirations()/read_stream() y
probablemente hacía fallar el fetch del propio $VIX.X. Corregido a símbolo
plano ("$VIX.X").

Pipeline:
  - Stream 0DTE-equivalente cada 5s (VIX no tiene 0DTE real — usa el
    vencimiento más próximo disponible, típicamente semanal)
  - Quote plano cada 5s para $VIX.X y VIX1D.X (last/change/change_pct/bid/ask)
  - JSON output: gaia_vix_live.json
  - Railway push: /push_vix  (endpoint agregado a gaia_server.py — 20-ago)

IMPORTANTE — VERIFICAR ANTES DE CORRER:
  - VX_SYMBOL: confirmado por Miguel (19-ago-2026) que el contrato frente
    real es VXU2026 (CBOE:VX1!, vencimiento septiembre). Se usa acá en
    formato de 2 dígitos de año ("VXU26"), igual que ESU26/NQU26 que ya
    funcionan con la API de TradeStation. Si la API lo rechaza, probar
    con el formato de 4 dígitos ("VXU2026") en su lugar.
"""

import json, os, time, urllib.parse, urllib.request
import http.client, ssl, logging, certifi
from datetime import datetime, timedelta
from collections import deque

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
TS_CLIENT_ID     = "HMVux6j6ncGeYOVFbWVXyB0lSVL4WWWe"
TS_CLIENT_SECRET = "2Y4SKDlCN0PMX6wbwWLRvcPNeaA7Zl1ygJoSFO9XWWvsCP37xXrF9RzCUBjaddIx"
TOKEN_FILE       = "ts_tokens.json"
TOKEN_URL        = "https://signin.tradestation.com/oauth/token"
API_BASE         = "https://api.tradestation.com/v3"
OUTPUT_FILE      = "gaia_vix_live.json"
LOG_FILE         = "gaia_vix_live.log"

VIX_SYMBOL       = "$VIX.X"     # VIX cash index — mismo patrón que $SPX.X / $NDX.X.
                                 # IMPORTANTE: símbolo SIN codificar acá — get_price(),
                                 # get_expirations() y read_stream() ya hacen
                                 # urllib.parse.quote() antes de pegarlo a la URL.
                                 # (Fix 20-ago: antes estaba guardado pre-codificado
                                 # como "%24VIX.X", lo que causaba doble-encoding
                                 # -> "%2524VIX.X", un símbolo inválido para la API.)
VIX1D_SYMBOL     = "VIX1D.X"    # Confirmado en Plan de Desarrollo GAIA Vol Engine V1
                                 # (Fase 1A) — SIN prefijo "$", a diferencia de $VIX.X.
VX_SYMBOL        = "VXU26"      # Confirmado por Miguel (19-ago): contrato frente
                                 # real es VXU2026 (CBOE:VX1!, Sep 2026). Formato
                                 # acá en 2 dígitos de año, igual que ESU26/NQU26
                                 # que ya funcionan con la API de TradeStation —
                                 # si TradeStation lo rechaza, verificar si espera
                                 # 4 dígitos ("VXU2026") en su lugar.

STRIKE_PROXIMITY = 15    # VIX cotiza en incrementos más anchos que SPX/ETFs
REFRESH_0DTE     = 5     # segundos por ciclo
DHP_HISTORY_SIZE = 10

# ── RAILWAY ───────────────────────────────────────────────────────────────────
RAILWAY_URL   = "https://web-production-49e7.up.railway.app"
RAILWAY_TOKEN = "gaia_push_secret_2026"

# ── SSL ───────────────────────────────────────────────────────────────────────
# Mismo fix aplicado en ts_gaia_etf.py y ts_gaia_ndx_v2.py — fuerza el bundle
# de certifi en vez del almacén de certificados de Windows.
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VIX][%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
log = logging.getLogger("GAIA_VIX")

# ── DHP HISTORY ───────────────────────────────────────────────────────────────
dhp_history_vix = deque(maxlen=DHP_HISTORY_SIZE)

# ── TOKEN — comparte con SPX / NDX / ETF backends ─────────────────────────────
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
    with urllib.request.urlopen(req, timeout=15, context=SSL_CONTEXT) as resp:
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
    log.info("Refrescando token VIX...")
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
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CONTEXT) as resp:
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

# ── QUOTE COMPLETO (VIX1D — Fase 1A GAIA Vol Engine) ──────────────────────────
def get_quote(symbol, token):
    """Trae last/change/change_pct/bid/ask para un símbolo. Usado para $VIX.X y
    VIX1D.X (Fase 1A del Plan de Desarrollo GAIA Vol Engine V1: 'timestamp, last,
    change, change_pct' como mínimo, 'idealmente también bid, ask')."""
    quote = {
        "last": 0.0, "change": 0.0, "change_pct": 0.0,
        "bid": None, "ask": None, "timestamp": None,
    }
    try:
        sym_enc = urllib.parse.quote(symbol, safe="")
        result  = api_get(f"/marketdata/quotes/{sym_enc}", token, timeout=20)
        quotes  = result.get("Quotes", [])
        if not quotes:
            return quote
        q = quotes[0]
        last = q.get("Last", 0)
        quote["last"] = float(last) if last else 0.0
        # TradeStation puede exponer el cambio directo o solo el close previo —
        # cubrir ambos casos y calcular change_pct si hace falta.
        net_change = q.get("NetChange")
        if net_change is not None:
            quote["change"] = float(net_change)
        prev_close = q.get("PreviousClose") or q.get("Close")
        if net_change is None and prev_close:
            try:
                quote["change"] = round(quote["last"] - float(prev_close), 4)
            except Exception:
                pass
        net_change_pct = q.get("NetChangePct")
        if net_change_pct is not None:
            try:
                quote["change_pct"] = float(net_change_pct)
            except Exception:
                pass
        elif prev_close and float(prev_close) != 0:
            try:
                quote["change_pct"] = round((quote["change"] / float(prev_close)) * 100, 4)
            except Exception:
                pass
        bid = q.get("Bid")
        ask = q.get("Ask")
        quote["bid"] = float(bid) if bid not in (None, "") else None
        quote["ask"] = float(ask) if ask not in (None, "") else None
        quote["timestamp"] = q.get("TradeTime") or q.get("LastUpdated")
    except Exception as e:
        log.warning(f"Error quote {symbol}: {e}")
    return quote

# ── EXPIRATIONS ───────────────────────────────────────────────────────────────
def get_expirations(symbol, token):
    """Clasifica expirations en 3 capas: proximo / weekly / monthly.
    Nota: VIX no tiene 0DTE real como SPX — "0dte" acá significa
    "el vencimiento más próximo disponible", que suele ser semanal."""
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

            if exp_date >= today and not layers["0dte"]:
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
            context=SSL_CONTEXT,
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

# ── PROCESAR VIX ──────────────────────────────────────────────────────────────
def process_vix(expirations, spot, token, dhp_history, cache):
    result = {
        "symbol":        "VIX",
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
        raw = read_stream(VIX_SYMBOL, expirations["0dte"], spot, token)
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
            log.info(f"VIX — {len(raw)} strikes DHP:{total_dhp} [{direction}] levels:{levels}")
    except Exception as e:
        log.error(f"Error procesando VIX: {e}")

    return result

# ── GUARDAR JSON ──────────────────────────────────────────────────────────────
def save_vix_json(vix_data):
    try:
        output = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "vix":       vix_data,
            "status":    "live"
        }
        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(output, f, indent=2)
        os.replace(tmp, OUTPUT_FILE)

        vix_dir = vix_data.get("dhp_direction", "—")
        log.info(
            f"VIX:{vix_data.get('spot',0):.2f} DHP:{vix_data.get('total_dhp',0)} [{vix_dir}] | "
            f"CW:{vix_data.get('levels',{}).get('call_wall','—')} "
            f"PW:{vix_data.get('levels',{}).get('put_wall','—')} "
            f"Flip:{vix_data.get('levels',{}).get('gamma_flip','—')}"
        )
    except Exception as e:
        log.error(f"Error guardando VIX JSON: {e}")

# ── RAILWAY PUSH ──────────────────────────────────────────────────────────────
def push_to_railway(data: dict):
    try:
        body = json.dumps(data).encode("utf-8")
        req  = urllib.request.Request(
            RAILWAY_URL + "/push_vix",
            data=body,
            method="POST"
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Push-Token", RAILWAY_TOKEN)
        with urllib.request.urlopen(req, timeout=3, context=SSL_CONTEXT) as resp:
            if resp.status != 200:
                log.warning(f"Railway VIX push status: {resp.status}")
    except Exception as e:
        log.warning(f"Railway VIX push error: {e}")

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
    log.info("  PivotAlphaDesk — GAIA VIX Backend v1")
    log.info("  Fase 1 — solo pipeline propio, sin relacion con SPX todavia")
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
    exp_vix = {"0dte": None, "weekly": None, "monthly": None}
    while not any(exp_vix.values()):
        try:
            exp_vix = get_expirations(VIX_SYMBOL, token)
            if not any(exp_vix.values()):
                safe_sleep(30)
        except KeyboardInterrupt:
            return
        except Exception as e:
            log.error(f"Error exp VIX: {e}")
            safe_sleep(30)

    log.info(f"VIX expirations: {exp_vix}")

    cache_vix = {"0dte": {}, "weekly": {}, "monthly": {}}
    last_exp_check = 0.0
    cycle = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        now = time.time()
        log.info(f"--- VIX Ciclo {cycle} ---")

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
                    new_exp = get_expirations(VIX_SYMBOL, token)
                    if any(new_exp.values()): exp_vix = new_exp
                    last_exp_check = now
                except Exception as e:
                    log.warning(f"Error refresh expirations: {e}")

            # ── Precios
            quote_vix   = get_quote(VIX_SYMBOL, token)      # last/change/change_pct/bid/ask
            quote_vix1d = get_quote(VIX1D_SYMBOL, token)    # idem, Fase 1A Vol Engine
            spot_vix    = quote_vix["last"]
            spot_vx     = get_price(VX_SYMBOL, token)        # fuera de la ruta crítica (ver plan)

            # ── Procesar VIX (niveles/DHP siguen calculándose solo sobre $VIX.X)
            vix_data = process_vix(exp_vix, spot_vix, token, dhp_history_vix, cache_vix)
            vix_data["spot_vx"] = spot_vx
            vix_data["basis_vx"] = round(spot_vx - spot_vix, 2) if spot_vx > 0 and spot_vix > 0 else 0.0

            # ── VIX1D (Fase 1A) — feed propio, sin relación calculada con SPX todavía
            vix1d_last = quote_vix1d["last"]
            vix_data["vix1d"] = {
                "last":       vix1d_last,
                "change":     quote_vix1d["change"],
                "change_pct": quote_vix1d["change_pct"],
                "bid":        quote_vix1d["bid"],
                "ask":        quote_vix1d["ask"],
                "timestamp":  quote_vix1d["timestamp"],
            }
            vix_data["vix_change"]     = quote_vix["change"]
            vix_data["vix_change_pct"] = quote_vix["change_pct"]
            vix_data["vix1d_vix_ratio"] = (
                round(vix1d_last / spot_vix, 4) if vix1d_last > 0 and spot_vix > 0 else None
            )

            # ── Guardar + Push
            if spot_vix > 0:
                save_vix_json(vix_data)
                push_to_railway({
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "vix":       vix_data,
                    "status":    "live"
                })
                ratio_str = vix_data["vix1d_vix_ratio"] if vix_data["vix1d_vix_ratio"] is not None else "—"
                log.info(f"VIX1D: {vix1d_last} (chg {quote_vix1d['change_pct']}%) | 1D/VIX: {ratio_str}")
                consecutive_errors = 0
            else:
                log.warning("Sin precio VIX")
                consecutive_errors += 1

            if vix1d_last <= 0:
                log.warning("Sin precio VIX1D — revisar símbolo VIX1D.X")

            if consecutive_errors >= 10:
                log.error("10 errores — pausa 120s...")
                safe_sleep(120)
                consecutive_errors = 0

        except KeyboardInterrupt:
            log.info("VIX backend detenido por usuario.")
            break
        except Exception as e:
            log.error(f"Excepcion no prevista ciclo {cycle}: {e}")
            consecutive_errors += 1

        try:
            time.sleep(REFRESH_0DTE)
        except KeyboardInterrupt:
            log.info("VIX backend detenido.")
            break

if __name__ == "__main__":
    main()
