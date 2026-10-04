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
VIX1D_SYMBOL     = "VIX1D.X"    # Formato asumido originalmente por el Plan de
                                 # Desarrollo GAIA Vol Engine V1 (Fase 1A) — no
                                 # confirmado en la práctica (devolvió 0 en el
                                 # primer test 20-ago). Superado por
                                 # VIX1D_SYMBOL_CANDIDATES / resolve_vix1d_symbol()
                                 # más abajo, que prueba varios formatos al
                                 # arrancar y usa el que responda. Se deja esta
                                 # constante solo como referencia documental.
VX_SYMBOL        = "VXU26"      # Confirmado por Miguel (19-ago): contrato frente
                                 # real es VXU2026 (CBOE:VX1!, Sep 2026). Formato
                                 # acá en 2 dígitos de año, igual que ESU26/NQU26
                                 # que ya funcionan con la API de TradeStation —
                                 # si TradeStation lo rechaza, verificar si espera
                                 # 4 dígitos ("VXU2026") en su lugar.

STRIKE_PROXIMITY = 15    # VIX cotiza en incrementos más anchos que SPX/ETFs
REFRESH_0DTE     = 5     # segundos por ciclo
DHP_HISTORY_SIZE = 10

# ── GAIA VOL ENGINE V1 (validado 04-oct-2026) ────────────────────────────────
# Mide estabilidad/inestabilidad del entorno VOL; NO predice dirección SPX.
VOL_WINDOW_SECONDS = 300
VOL_CONFIRM_SECONDS = 30
VOL_VIX_STABLE_PCT = 0.20
VOL_VIX_UNSTABLE_PCT = 0.39
VOL_RATIO_STABLE_PCT = 0.33
VOL_RATIO_UNSTABLE_PCT = 1.02

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
def get_quote(symbol, token, debug=False):
    """Trae last/change/change_pct/bid/ask para un símbolo. Usado para $VIX.X y
    VIX1D.X (Fase 1A del Plan de Desarrollo GAIA Vol Engine V1: 'timestamp, last,
    change, change_pct' como mínimo, 'idealmente también bid, ask').

    TradeStation no siempre tira excepción con un símbolo inválido — muchas veces
    responde 200 con la lista 'Errors' poblada y 'Quotes' vacía (o con el campo
    'Last' ausente). Por eso, si debug=True, logueamos la respuesta cruda cuando
    no se pudo sacar un precio — así se ve el motivo real en vez de solo '0.0'."""
    quote = {
        "last": 0.0, "change": 0.0, "change_pct": 0.0,
        "bid": None, "ask": None, "timestamp": None,
    }
    try:
        sym_enc = urllib.parse.quote(symbol, safe="")
        result  = api_get(f"/marketdata/quotes/{sym_enc}", token, timeout=20)
        quotes  = result.get("Quotes", [])
        errors  = result.get("Errors", [])
        if not quotes:
            if debug:
                log.warning(f"Quote vacío para '{symbol}' — respuesta cruda: {result}")
            return quote
        q = quotes[0]
        last = q.get("Last", 0)
        quote["last"] = float(last) if last else 0.0
        if debug and quote["last"] == 0.0:
            log.warning(f"Quote sin 'Last' para '{symbol}' — entrada cruda: {q} | Errors: {errors}")
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

VIX1D_SYMBOL_CANDIDATES = ["$VIX1D.X", "VIX1D.X", "VIX1D"]
# El plan de desarrollo asumía "VIX1D.X" sin "$", pero $VIX.X sí necesitaba el
# prefijo — probamos varios formatos una sola vez al arrancar (no en cada ciclo,
# para no gastar 3 llamadas de más cada 5s) y nos quedamos con el que responda.

def resolve_vix1d_symbol(token):
    """Prueba los formatos candidatos de VIX1D y devuelve el primero que responda
    con un precio válido. Loguea la respuesta cruda de cada intento fallido para
    diagnóstico. Devuelve None si ninguno funcionó (no bloquea el pipeline —
    VIX1D queda en 0 hasta que se resuelva, VIX y niveles siguen corriendo)."""
    for candidate in VIX1D_SYMBOL_CANDIDATES:
        log.info(f"Probando símbolo VIX1D candidato: '{candidate}'...")
        q = get_quote(candidate, token, debug=True)
        if q["last"] > 0:
            log.info(f"VIX1D resuelto: símbolo '{candidate}' -> last={q['last']}")
            return candidate
        log.warning(f"Candidato '{candidate}' no devolvió precio válido.")
    log.error(
        "Ningún formato de símbolo VIX1D funcionó "
        f"({VIX1D_SYMBOL_CANDIDATES}). Revisar el diagnóstico arriba (Errors/"
        "respuesta cruda de TradeStation) — puede que VIX1D no esté habilitado "
        "en esta cuenta/plan, o que el símbolo real sea otro. VIX y niveles "
        "siguen corriendo normalmente sin esto."
    )
    return None

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
        empty_reads   = 0
        MAX_EMPTY_READS = 50  # BUGFIX 21-ago + 26-ago (ver detalle abajo)
        # 21-ago: si TradeStation cierra la conexión (ej. fin de sesión de
        # opciones VIX tras el cierre de RTH), readline() deja de bloquear y
        # empieza a devolver "" instantáneamente en loop — sin esto,
        # lines_read/heartbeats nunca avanzan y el while de abajo gira a
        # velocidad de CPU para siempre, sin excepción ni mensaje, dejando
        # el proceso "vivo" pero mudo.
        #
        # 26-ago: el fix del 21-ago solo contaba líneas VACÍAS. En vivo se
        # confirmó un segundo camino de cuelgue: una conexión cerrada a veces
        # manda basura NO vacía (bytes que no son JSON válido) en vez de
        # líneas vacías limpias. Como el contador se reseteaba apenas 'raw'
        # no estaba vacío —antes de intentar parsearlo— ese caso caía en el
        # 'except: continue' del json.loads() sin incrementar nada, girando
        # para siempre a máxima velocidad de CPU (confirmado en vivo con
        # Task Manager: 16.5% CPU sostenido, proceso vivo pero mudo). Ahora
        # CUALQUIER lectura que no sea un heartbeat o un dato real de opción
        # cuenta como improductiva — solo un mensaje válido y útil resetea
        # el contador.

        while lines_read < max_contracts and heartbeats < max_heartbeat:
            try:
                raw = resp.readline().decode("utf-8").strip()
            except Exception as e:
                log.warning(f"Stream {symbol} readline error: {e}")
                break

            if not raw:
                empty_reads += 1
                if empty_reads > MAX_EMPTY_READS:
                    log.warning(
                        f"Stream {symbol} sin datos tras {empty_reads} lecturas vacías "
                        f"seguidas — conexión probablemente cerrada (¿mercado cerrado?). "
                        f"Cortando este ciclo, se reintenta en el próximo."
                    )
                    break
                continue

            try:
                data = json.loads(raw)
            except Exception:
                empty_reads += 1  # basura no vacía cuenta igual que vacío
                if empty_reads > MAX_EMPTY_READS:
                    log.warning(
                        f"Stream {symbol} sin datos utilizables tras {empty_reads} lecturas "
                        f"seguidas (basura no-JSON) — conexión probablemente cerrada. "
                        f"Cortando este ciclo, se reintenta en el próximo."
                    )
                    break
                continue

            empty_reads = 0  # recién acá: un JSON válido de verdad resetea el contador

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

# ── ESTRUCTURA VIX OPTIONS (Fase 3 GAIA Vol Engine) ────────────────────────────
# IMPORTANTE: nombres deliberadamente NEUTRALES (MAJOR CALL OI, MAJOR PUT OI,
# MAJOR GAMMA NODE, NEAREST STRUCTURAL NODE, EXPIRY CONCENTRATION) — el plan
# suspende explícitamente "VIX Call Wall/Put Wall/Gamma Flip" hasta validar con
# datos reales si existe una equivalencia sólida con los conceptos de SPX. Esta
# capa NO genera señales, solo variables observables.
#
# El "gamma" acá se aproxima con call_gex/put_gex (ya calculados por
# calculate_gaia = OI × gamma × spot² × 100) en vez de la gamma cruda por
# strike — es la forma correcta de medir "concentración" de gamma en dólares,
# no solo el coeficiente. Se documenta la asunción para que quede trazable.
def compute_vix_structure(cache, spot, expirations, top_n=3):
    structure = {
        "total_call_oi": None, "total_put_oi": None,
        "major_call_oi_strikes": [], "major_put_oi_strikes": [],
        "nearest_structural_oi_strike": None, "distance_to_oi_node": None,
        "major_gamma_call_strikes": [], "major_gamma_put_strikes": [],
        "nearest_gamma_node": None, "distance_to_gamma_node": None,
        "expiry_concentration": [],
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # ── OI y Gamma sobre el vencimiento más próximo (0dte) ──────────────────
    strikes_0dte = cache.get("0dte", {}).get("strikes_data", [])
    if strikes_0dte:
        total_call_oi = sum(s["call_oi"] for s in strikes_0dte)
        total_put_oi  = sum(s["put_oi"]  for s in strikes_0dte)

        top_call_oi = sorted(strikes_0dte, key=lambda s: s["call_oi"], reverse=True)[:top_n]
        top_put_oi  = sorted(strikes_0dte, key=lambda s: s["put_oi"],  reverse=True)[:top_n]

        max_oi_strike = max(strikes_0dte, key=lambda s: s["call_oi"] + s["put_oi"])
        nearest_oi_strike = max_oi_strike["strike"]

        top_gamma_call = sorted(strikes_0dte, key=lambda s: s["call_gex"], reverse=True)[:top_n]
        top_gamma_put  = sorted(strikes_0dte, key=lambda s: abs(s["put_gex"]), reverse=True)[:top_n]

        max_gamma_strike = max(strikes_0dte, key=lambda s: s["call_gex"] + abs(s["put_gex"]))
        nearest_gamma_strike = max_gamma_strike["strike"]

        structure.update({
            "total_call_oi": total_call_oi,
            "total_put_oi":  total_put_oi,
            "major_call_oi_strikes": [{"strike": s["strike"], "oi": s["call_oi"]} for s in top_call_oi],
            "major_put_oi_strikes":  [{"strike": s["strike"], "oi": s["put_oi"]}  for s in top_put_oi],
            "nearest_structural_oi_strike": nearest_oi_strike,
            "distance_to_oi_node": round(spot - nearest_oi_strike, 2) if spot > 0 else None,
            "major_gamma_call_strikes": [
                {"strike": s["strike"], "gamma_exposure_musd": s["call_gex"]} for s in top_gamma_call
            ],
            "major_gamma_put_strikes": [
                {"strike": s["strike"], "gamma_exposure_musd": s["put_gex"]} for s in top_gamma_put
            ],
            "nearest_gamma_node": nearest_gamma_strike,
            "distance_to_gamma_node": round(spot - nearest_gamma_strike, 2) if spot > 0 else None,
        })

    # ── Concentración por vencimiento (0dte / weekly / monthly, los que haya en cache) ──
    today = datetime.now().date()
    layer_rows = []
    total_oi_all, total_gamma_all = 0.0, 0.0

    for layer_name in ("0dte", "weekly", "monthly"):
        layer_strikes = cache.get(layer_name, {}).get("strikes_data", [])
        exp_date_str  = expirations.get(layer_name)
        if not layer_strikes or not exp_date_str:
            continue
        oi_sum    = sum(s["call_oi"] + s["put_oi"] for s in layer_strikes)
        gamma_sum = sum(abs(s["call_gex"]) + abs(s["put_gex"]) for s in layer_strikes)
        total_oi_all    += oi_sum
        total_gamma_all += gamma_sum
        try:
            days_to_expiry = (datetime.strptime(exp_date_str, "%Y-%m-%d").date() - today).days
        except Exception:
            days_to_expiry = None
        layer_rows.append({
            "layer": layer_name, "expiration": exp_date_str,
            "days_to_expiry": days_to_expiry,
            "total_oi": oi_sum, "total_gamma_exposure_musd": round(gamma_sum, 4),
        })

    for row in layer_rows:
        row["oi_pct_of_total"]    = round(100 * row["total_oi"] / total_oi_all, 2) if total_oi_all else None
        row["gamma_pct_of_total"] = round(100 * row["total_gamma_exposure_musd"] / total_gamma_all, 2) if total_gamma_all else None

    structure["expiry_concentration"] = layer_rows
    return structure

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

# ── VOL ENGINE V1 ────────────────────────────────────────────────────────────
def _value_at_or_before(history, target_ts):
    """Devuelve la última muestra <= target_ts. history: deque[(ts, value)]."""
    for ts, value in reversed(history):
        if ts <= target_ts:
            return value
    return None

def compute_vol_state(now_ts, spot_vix, ratio, vix_history, ratio_history, state_ctx):
    """
    Clasifica el entorno VOL usando desplazamientos absolutos de 5 minutos.
    STABLE: ambos componentes bajo umbral estable.
    UNSTABLE: ambos componentes sobre umbral inestable.
    TRANSITION: resto.
    El cambio confirmado requiere VOL_CONFIRM_SECONDS de persistencia.
    """
    if not (spot_vix and spot_vix > 0 and ratio and ratio > 0):
        return {
            "state": state_ctx.get("confirmed", "BUILDING"), "raw_state": "BUILDING",
            "vix_5m_pct": None, "ratio_5m_pct": None, "ready": False,
            "window_seconds": VOL_WINDOW_SECONDS, "confirm_seconds": VOL_CONFIRM_SECONDS
        }

    vix_history.append((now_ts, float(spot_vix)))
    ratio_history.append((now_ts, float(ratio)))
    cutoff = now_ts - (VOL_WINDOW_SECONDS + 120)
    while vix_history and vix_history[0][0] < cutoff:
        vix_history.popleft()
    while ratio_history and ratio_history[0][0] < cutoff:
        ratio_history.popleft()

    old_vix = _value_at_or_before(vix_history, now_ts - VOL_WINDOW_SECONDS)
    old_ratio = _value_at_or_before(ratio_history, now_ts - VOL_WINDOW_SECONDS)
    if not old_vix or not old_ratio:
        return {
            "state": state_ctx.get("confirmed", "BUILDING"), "raw_state": "BUILDING",
            "vix_5m_pct": None, "ratio_5m_pct": None, "ready": False,
            "window_seconds": VOL_WINDOW_SECONDS, "confirm_seconds": VOL_CONFIRM_SECONDS
        }

    vix_5m = abs((spot_vix / old_vix - 1.0) * 100.0)
    ratio_5m = abs((ratio / old_ratio - 1.0) * 100.0)

    if vix_5m <= VOL_VIX_STABLE_PCT and ratio_5m <= VOL_RATIO_STABLE_PCT:
        raw = "STABLE"
    elif vix_5m >= VOL_VIX_UNSTABLE_PCT and ratio_5m >= VOL_RATIO_UNSTABLE_PCT:
        raw = "UNSTABLE"
    else:
        raw = "TRANSITION"

    confirmed = state_ctx.get("confirmed")
    candidate = state_ctx.get("candidate")
    candidate_since = state_ctx.get("candidate_since")

    if confirmed is None:
        confirmed = raw
        state_ctx.update({"confirmed": raw, "candidate": None, "candidate_since": None})
    elif raw == confirmed:
        state_ctx.update({"candidate": None, "candidate_since": None})
    else:
        if candidate != raw:
            state_ctx.update({"candidate": raw, "candidate_since": now_ts})
        elif candidate_since is not None and now_ts - candidate_since >= VOL_CONFIRM_SECONDS:
            confirmed = raw
            state_ctx.update({"confirmed": raw, "candidate": None, "candidate_since": None})

    return {
        "state": state_ctx.get("confirmed", raw), "raw_state": raw,
        "vix_5m_pct": round(vix_5m, 4), "ratio_5m_pct": round(ratio_5m, 4),
        "ready": True, "window_seconds": VOL_WINDOW_SECONDS,
        "confirm_seconds": VOL_CONFIRM_SECONDS,
        "thresholds": {
            "vix_stable_pct": VOL_VIX_STABLE_PCT, "vix_unstable_pct": VOL_VIX_UNSTABLE_PCT,
            "ratio_stable_pct": VOL_RATIO_STABLE_PCT, "ratio_unstable_pct": VOL_RATIO_UNSTABLE_PCT
        }
    }

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

    # ── Resolver símbolo VIX1D una sola vez (Fase 1A) ─────────────────────────
    vix1d_symbol = resolve_vix1d_symbol(token)
    last_vix1d_retry = time.time()

    cache_vix = {"0dte": {}, "weekly": {}, "monthly": {}}
    last_exp_check = 0.0
    cycle = 0
    consecutive_errors = 0

    # ── GAIA VOL ENGINE V1 — historia intradía en memoria
    vol_vix_history = deque()
    vol_ratio_history = deque()
    vol_state_ctx = {"confirmed": None, "candidate": None, "candidate_since": None}

    # ── Estructura VIX Options (Fase 3) — se refresca cada 3 min, no cada
    # ciclo de 5s (el plan pide "cada 1-5 minutos", la estructura completa
    # se mueve mucho más lento que el precio). Se conserva entre ciclos.
    STRUCTURE_REFRESH_SECONDS = 180
    last_structure_update = 0.0
    last_structure = None

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

            # ── Reintentar resolver VIX1D cada 10 min si sigue sin resolverse
            if vix1d_symbol is None and now - last_vix1d_retry > 600:
                vix1d_symbol = resolve_vix1d_symbol(token)
                last_vix1d_retry = now

            # ── Precios
            quote_vix   = get_quote(VIX_SYMBOL, token)      # last/change/change_pct/bid/ask
            quote_vix1d = (
                get_quote(vix1d_symbol, token) if vix1d_symbol
                else {"last": 0.0, "change": 0.0, "change_pct": 0.0,
                      "bid": None, "ask": None, "timestamp": None}
            )
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

            # ── GAIA VOL ENGINE V1 — estado operativo, no direccional
            vix_data["vol_engine"] = compute_vol_state(
                now, spot_vix, vix_data["vix1d_vix_ratio"],
                vol_vix_history, vol_ratio_history, vol_state_ctx
            )

            # ── Estructura VIX Options (Fase 3) — cada 3 min, no cada ciclo ────
            if spot_vix > 0 and now - last_structure_update > STRUCTURE_REFRESH_SECONDS:
                try:
                    for layer in ("weekly", "monthly"):
                        exp_date = exp_vix.get(layer)
                        if not exp_date:
                            continue
                        raw_layer = read_stream(VIX_SYMBOL, exp_date, spot_vix, token)
                        if raw_layer:
                            layer_strikes, _, _, _ = calculate_gaia(raw_layer, spot_vix)
                            cache_vix[layer] = {"strikes_data": layer_strikes}
                    last_structure = compute_vix_structure(cache_vix, spot_vix, exp_vix)
                    last_structure_update = now
                    log.info(
                        f"Estructura VIX actualizada — MAJOR GAMMA NODE:{last_structure.get('nearest_gamma_node')} "
                        f"NEAREST STRUCTURAL OI:{last_structure.get('nearest_structural_oi_strike')} "
                        f"expiries:{len(last_structure.get('expiry_concentration', []))}"
                    )
                except Exception as e:
                    log.warning(f"Error actualizando estructura VIX: {e}")

            # 'structure' se conserva entre ciclos (no se recalcula cada 5s) —
            # None hasta el primer refresh, luego siempre el último válido.
            vix_data["structure"] = last_structure

            # ── Guardar + Push
            if spot_vix > 0:
                save_vix_json(vix_data)
                push_to_railway({
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "vix":       vix_data,
                    "status":    "live"
                })
                ratio_str = vix_data["vix1d_vix_ratio"] if vix_data["vix1d_vix_ratio"] is not None else "—"
                ve = vix_data.get("vol_engine", {})
                log.info(
                    f"VIX1D: {vix1d_last} (chg {quote_vix1d['change_pct']}%) | 1D/VIX: {ratio_str} | "
                    f"VOL:{ve.get('state','—')} [raw:{ve.get('raw_state','—')} "
                    f"VIX5m:{ve.get('vix_5m_pct')}% Ratio5m:{ve.get('ratio_5m_pct')}%]"
                )
                consecutive_errors = 0
            else:
                log.warning("Sin precio VIX")
                consecutive_errors += 1

            if vix1d_last <= 0 and vix1d_symbol is not None:
                log.warning(f"Sin precio VIX1D pese a símbolo resuelto ('{vix1d_symbol}') — revisar")

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
