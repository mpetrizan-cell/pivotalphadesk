"""
PivotAlphaDesk - GAIA Vol Engine - Data Logger
vol_engine_logger.py

Fase 4 (obligatoria) del Plan de Desarrollo GAIA Vol Engine V1: capturar
cada tick realtime en vol_engine_log.csv, cruzando VIX/VIX1D con SPX/DHP.
NO calcula todavía ningún estado (BUILDING/AMPLIFYING/CONFIRMING/etc.) —
eso es Fase 5, pendiente de validación empírica sobre estos datos primero.

No modifica GAIA SPX ni el dashboard principal. Es un proceso de SOLO
LECTURA aparte: lee los JSON locales que ya escriben ts_gaia_chart.py
(gaia_live.json) y ts_gaia_vix.py (gaia_vix_live.json) en la misma carpeta,
y no toca ninguno de los dos scripts ni sus pipelines.

Correr en la misma carpeta "API Tradestation", en su propia consola, junto
a ts_gaia_chart.py y ts_gaia_vix.py (los tres procesos corren en paralelo,
igual que ya hacés con el resto de los backends).

CAMPOS QUE SE LOGUEAN (ver Fase 4 del plan):
  timestamp, vix, vix1d, vix1d_vix_ratio,
  dvix_1m, dvix_5m, dvix1d_1m, dvix1d_5m,
  spx, dspx_1m, dspx_5m,
  dhp, ddhp_1m, ddhp_5m,
  gex, dex, dealer_regime,
  nearest_oi_node, distance_to_oi_node, nearest_gamma_node,
  distance_to_gamma_node, major_call_oi, major_put_oi,
  nearest_expiry_oi_pct, nearest_expiry_gamma_pct,
  spx_age_sec, vix_age_sec  (diagnóstico, no está en el plan original)

CAMPOS QUE QUEDAN EN BLANCO POR AHORA (a propósito, no son un error):
  - dealer_regime: el label FRAGILE/DEFENSIVE/etc. lo calcula
    gaia_server.py (Pressure Map / Terminal V10) del lado del servidor —
    no vive en gaia_live.json local. No se duplica esa lógica acá para no
    tener dos fuentes de verdad del régimen. Si más adelante hace falta
    en el logger, la forma correcta es que este script pegue una consulta
    de solo lectura a /checklist_data (el mismo endpoint público que ya
    lee Claude), no recalcularlo de cero.
  - nearest_oi_node / distance_to_oi_node / nearest_gamma_node /
    distance_to_gamma_node / major_call_oi / major_put_oi /
    nearest_expiry_oi_pct / nearest_expiry_gamma_pct: son salida de la
    Fase 3 (VIX Options Structure), todavía no construida. Las columnas
    ya están reservadas en el CSV para no tener que migrar el esquema
    después — se completan solas cuando se conecte la Fase 3.

AJUSTES 22-oct-2026:
  a) INSTANCIA ÚNICA: candado en el puerto local 127.0.0.1:47232 (distinto
     del de ts_gaia_vix.py). Si ya hay otra copia, avisa, espera 60s y sale.
  b) ESTADO VOL EN EL CSV: se agregan al final las columnas vol_state,
     vol_raw_state, vol_vix_5m_pct, vol_ratio_5m_pct y session_valid, leídas
     de gaia_vix_live.json (no se recalcula nada). Son la base para cruzar el
     estado VOL con los eventos de trading.
  c) CAMBIO DE ESQUEMA SEGURO: si el CSV existente tiene un encabezado
     distinto, se renombra a vol_engine_log_pre_AAAAMMDD_HHMM.csv y se crea
     uno nuevo (mismo criterio que el fix de schema-drift ya documentado).
  d) SIN PÉRDIDA POR BLOQUEO: si el CSV está bloqueado (abierto en Excel,
     sincronización de OneDrive: "[Errno 13] Permission denied"), las filas
     quedan en memoria y se escriben en el próximo ciclo que se pueda.

GEX y DEX (SPX) SÍ se calculan acá, agregando lo que ya escribe
ts_gaia_chart.py por strike en gaia_live.json:
  - gex = suma de net_gex de los strikes 0DTE (ya viene en millones)
  - dex = suma de (call_delta_oi + put_delta_oi) de los mismos strikes
    (ya etiquetado en ts_gaia_chart.py como "Delta × OI = presión real
    de hedging" — es el proxy de Dealer Exposure más directo disponible
    localmente, sin tocar el pipeline SPX)
"""

import json, os, sys, time, csv, socket, logging, urllib.request, urllib.error
from datetime import datetime, timezone
from collections import deque

# ── CONFIGURACION ─────────────────────────────────────────────────────────────
SPX_JSON_FILE  = "gaia_live.json"       # escrito por ts_gaia_chart.py
VIX_JSON_FILE  = "gaia_vix_live.json"   # escrito por ts_gaia_vix.py
LOG_CSV_FILE   = "vol_engine_log.csv"
LOG_FILE       = "vol_engine_logger.log"

REFRESH_SECONDS   = 5     # mismo ritmo que los dos pipelines que lee
CHECKLIST_URL = "https://app.pivotalphadesk.com/checklist_data"
# Endpoint público, sin auth (confirmado en gaia_server.py). Se consulta cada
# CHECKLIST_REFRESH_SECONDS, no cada ciclo de 5s — regime/permission son
# estados más lentos que el precio, y evita pegarle a Railway sin necesidad.
CHECKLIST_REFRESH_SECONDS = 30
CHECKLIST_TIMEOUT_SECONDS = 8  # corto a propósito — nunca debe frenar el ciclo
SPX_STALE_THRESHOLD_S = 150   # ts_gaia_chart.py: ciclo completo ~45-90s observado
VIX_STALE_THRESHOLD_S = 300   # ts_gaia_vix.py: mismo ciclo base, más el fetch extra
                               # de weekly/monthly cada 3 min (Fase 3 estructura) que
                               # puede alargar puntualmente ese ciclo bastante más.
                               # 300s da margen real sin dejar de detectar una caída
                               # genuina (VIX confirmado escribiendo a las 06:48 PM,
                               # 20-ago, con este mismo umbral en mente).
HISTORY_MINUTES   = 6      # margen sobre 5 min para poder calcular dX_5m
HISTORY_MAXLEN    = int((HISTORY_MINUTES * 60) / REFRESH_SECONDS) + 5

CSV_FIELDS = [
    "timestamp",
    "vix", "vix1d", "vix1d_vix_ratio",
    "dvix_1m", "dvix_5m", "dvix1d_1m", "dvix1d_5m",
    "spx", "dspx_1m", "dspx_5m",
    "dhp", "ddhp_1m", "ddhp_5m",
    "gex", "dex",
    "pm_regime", "pm_permission",       # Pressure Map — via /checklist_data
    "v10_regime", "v10_permission",     # Terminal V10 — via /checklist_data
    "gfs_calls", "gfs_puts", "gfs_net", # pendiente — solo existe en JS del frontend hoy
    "nearest_oi_node", "distance_to_oi_node",
    "nearest_gamma_node", "distance_to_gamma_node",
    "major_call_oi", "major_put_oi",
    "nearest_expiry_oi_pct", "nearest_expiry_gamma_pct",
    # Niveles estructurales SPX (Fase 10, agregado 1-sept a pedido de Nexa) —
    # SÍ disponibles localmente en gaia_live.json -> "levels"
    "spx_call_wall", "spx_put_wall", "spx_gamma_flip",
    "spx_gamma_node", "spx_gravity_pin",
    "dist_call_wall", "dist_put_wall", "dist_gamma_flip",
    "dist_gamma_node", "dist_gravity_pin",
    "spx_age_sec", "vix_age_sec",
    # Estado VOL (22-oct) — leído de gaia_vix_live.json["vix"]["vol_engine"]
    "vol_state", "vol_raw_state", "vol_vix_5m_pct", "vol_ratio_5m_pct", "session_valid",
]

INSTANCE_LOCK_PORT = 47232   # 22-oct: candado de instancia única (≠ ts_gaia_vix.py)
MAX_PENDING_ROWS   = 5000    # 22-oct: tope de filas en memoria si el CSV está bloqueado

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VOLLOG][%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
log = logging.getLogger("GAIA_VOL_LOGGER")

# ── HISTORIA PARA DELTAS (1m / 5m) ─────────────────────────────────────────────
hist_vix   = deque(maxlen=HISTORY_MAXLEN)   # (epoch_seconds, valor)
hist_vix1d = deque(maxlen=HISTORY_MAXLEN)
hist_spx   = deque(maxlen=HISTORY_MAXLEN)
hist_dhp   = deque(maxlen=HISTORY_MAXLEN)


def compute_delta(history, current_value, now, seconds_ago):
    """Busca la muestra más cercana (pero no más nueva) a 'now - seconds_ago'
    y devuelve current_value - esa muestra. None si todavía no hay suficiente
    historia (recién arrancado el logger)."""
    if current_value is None:
        return None
    target = now - seconds_ago
    candidate = None
    for ts, val in history:
        if ts <= target:
            candidate = val
        else:
            break
    if candidate is None:
        return None
    return round(current_value - candidate, 4)


def read_json_safe(path):
    """Lee un JSON local. Devuelve (data, age_seconds) o (None, None) si no
    existe o está corrupto/a medio escribir (el escritor usa tmp+replace
    atómico, pero igual cubrimos el borde de una lectura concurrente)."""
    if not os.path.exists(path):
        return None, None
    try:
        mtime = os.path.getmtime(path)
        age = time.time() - mtime
        with open(path, "r") as f:
            data = json.load(f)
        return data, round(age, 1)
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"Error leyendo {path} (probablemente escribiéndose ahora mismo): {e}")
        return None, None


def extract_spx_fields(spx_data):
    """spot_spx, total_dhp, gex agregado, dex agregado, y niveles estructurales
    (call_wall/put_wall/gamma_flip/gamma_node/gravity_pin) — a partir del JSON
    que ya escribe ts_gaia_chart.py. gex/dex se calculan acá sumando los
    strikes 0DTE; los niveles vienen directo del bloque 'levels' que el script
    ya calcula (confirmado en el código — no se recalcula nada acá)."""
    if not spx_data:
        return None, None, None, None, {}
    spot = spx_data.get("spot_spx")
    dhp  = spx_data.get("total_dhp")
    levels = spx_data.get("levels", {}) or {}

    strikes = (
        spx_data.get("layers", {}).get("0dte", {}).get("strikes")
        or spx_data.get("strikes")
        or []
    )
    gex = None
    dex = None
    if strikes:
        try:
            gex = round(sum(s.get("net_gex", 0.0) for s in strikes), 4)
            dex = round(
                sum(s.get("call_delta_oi", 0.0) + s.get("put_delta_oi", 0.0) for s in strikes),
                4,
            )
        except Exception as e:
            log.warning(f"Error agregando GEX/DEX desde strikes 0DTE: {e}")

    return spot, dhp, gex, dex, levels


def extract_vol_fields(vix_data):
    """Estado VOL tal cual lo publica ts_gaia_vix.py (22-oct). No recalcula nada."""
    if not vix_data:
        return None, None, None, None, None
    ve = (vix_data.get("vix", {}) or {}).get("vol_engine", {}) or {}
    return (ve.get("state"), ve.get("raw_state"), ve.get("vix_5m_pct"),
            ve.get("ratio_5m_pct"), ve.get("session_valid"))


def extract_vix_fields(vix_data):
    """vix, vix1d, ratio — a partir del JSON que ya escribe ts_gaia_vix.py."""
    if not vix_data:
        return None, None, None
    vix_block = vix_data.get("vix", {})
    vix   = vix_block.get("spot")
    vix1d = vix_block.get("vix1d", {}).get("last")
    ratio = vix_block.get("vix1d_vix_ratio")
    return vix, vix1d, ratio


def fetch_checklist_data():
    """Consulta de solo lectura a /checklist_data (Railway, público, sin auth).
    Devuelve (pm_regime, pm_permission, v10_regime, v10_permission) tal cual
    los expone gaia_server.py — NO se recalcula nada acá, solo se consume.
    Cualquier falla de red devuelve (None, None, None, None) sin tirar
    excepción — el logger nunca se frena por esto."""
    try:
        req = urllib.request.Request(CHECKLIST_URL, headers={"User-Agent": "GAIA-VolEngine-Logger"})
        with urllib.request.urlopen(req, timeout=CHECKLIST_TIMEOUT_SECONDS) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        pm  = data.get("pressure_map", {}) or {}
        v10 = data.get("terminal_v10", {}) or {}
        return (
            pm.get("regime"), pm.get("trade_permission"),
            v10.get("regime"), v10.get("trade_permission"),
        )
    except Exception as e:
        log.warning(f"Error consultando {CHECKLIST_URL}: {e}")
        return None, None, None, None


def ensure_csv_header():
    # 22-oct: si el CSV existe con otro encabezado, se archiva y se crea uno nuevo
    if os.path.exists(LOG_CSV_FILE):
        try:
            with open(LOG_CSV_FILE, "r", newline="", encoding="utf-8", errors="replace") as f:
                current = next(csv.reader(f), [])
        except Exception as e:
            log.warning(f"No se pudo leer el encabezado de {LOG_CSV_FILE}: {e}")
            current = CSV_FIELDS
        if current != CSV_FIELDS:
            archived = f"vol_engine_log_pre_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            os.replace(LOG_CSV_FILE, archived)
            log.info(f"Encabezado distinto — {LOG_CSV_FILE} archivado como {archived}.")
    if not os.path.exists(LOG_CSV_FILE):
        with open(LOG_CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
        log.info(f"Creado {LOG_CSV_FILE} con header nuevo.")


_pending_rows = []

def append_row(row: dict):
    """22-oct: si el CSV está bloqueado, guarda la fila en memoria y la escribe
    junto con las siguientes apenas se libere (no se pierden ciclos)."""
    _pending_rows.append(row)
    try:
        with open(LOG_CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerows(_pending_rows)
        if len(_pending_rows) > 1:
            log.info(f"CSV liberado — escritas {len(_pending_rows)} filas pendientes.")
        _pending_rows.clear()
    except PermissionError:
        if len(_pending_rows) > MAX_PENDING_ROWS:
            del _pending_rows[0]
        if len(_pending_rows) in (1, 12, 120) or len(_pending_rows) % 720 == 0:
            log.warning(
                f"{LOG_CSV_FILE} bloqueado (¿abierto en Excel / OneDrive?) — "
                f"{len(_pending_rows)} filas esperando en memoria."
            )


_instance_lock = None

def acquire_instance_lock():
    global _instance_lock
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", INSTANCE_LOCK_PORT))
        s.listen(1)
        _instance_lock = s
        return True
    except OSError:
        s.close()
        return False


def safe_sleep(seconds):
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise
    except Exception:
        pass


def main():
    if not acquire_instance_lock():
        log.warning(
            f"Ya hay otra copia de vol_engine_logger.py corriendo (puerto {INSTANCE_LOCK_PORT} "
            "ocupado). Esta copia no hace nada: espera 60s y sale."
        )
        safe_sleep(60)
        sys.exit(0)

    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA Vol Engine — Data Logger (Fase 4)")
    log.info("  Solo lectura — no modifica GAIA SPX ni el dashboard")
    log.info("=" * 60)

    ensure_csv_header()
    cycle = 0

    # ── Pressure Map / Terminal V10 (via /checklist_data, Fase 10 — 1-sept) ──
    last_checklist_update = 0.0
    pm_regime = pm_permission = v10_regime = v10_permission = None

    while True:
        cycle += 1
        now = time.time()

        try:
            spx_data, spx_age = read_json_safe(SPX_JSON_FILE)
            vix_data, vix_age = read_json_safe(VIX_JSON_FILE)

            if spx_data is None:
                log.warning(f"{SPX_JSON_FILE} no encontrado o ilegible — ¿ts_gaia_chart.py está corriendo?")
            elif spx_age is not None and spx_age > SPX_STALE_THRESHOLD_S:
                log.warning(f"{SPX_JSON_FILE} desactualizado ({spx_age}s) — pipeline SPX puede estar caído")

            if vix_data is None:
                log.warning(f"{VIX_JSON_FILE} no encontrado o ilegible — ¿ts_gaia_vix.py está corriendo?")
            elif vix_age is not None and vix_age > VIX_STALE_THRESHOLD_S:
                log.warning(f"{VIX_JSON_FILE} desactualizado ({vix_age}s) — pipeline VIX puede estar caído")

            spx, dhp, gex, dex, levels = extract_spx_fields(spx_data)
            vix, vix1d, ratio  = extract_vix_fields(vix_data)
            vol_state, vol_raw, vol_v5, vol_r5, vol_session = extract_vol_fields(vix_data)

            # ── Niveles estructurales SPX + distancias (Fase 10, 1-sept) ──────
            spx_call_wall   = levels.get("call_wall")
            spx_put_wall    = levels.get("put_wall")
            spx_gamma_flip  = levels.get("gamma_flip")
            spx_gamma_node  = levels.get("gamma_node")
            spx_gravity_pin = levels.get("gravity_pin")

            def _dist(level):
                if spx is None or level is None:
                    return None
                return round(spx - level, 2)

            dist_call_wall   = _dist(spx_call_wall)
            dist_put_wall    = _dist(spx_put_wall)
            dist_gamma_flip  = _dist(spx_gamma_flip)
            dist_gamma_node  = _dist(spx_gamma_node)
            dist_gravity_pin = _dist(spx_gravity_pin)

            # ── Pressure Map / Terminal V10 — consulta remota cada 30s ─────────
            if now - last_checklist_update > CHECKLIST_REFRESH_SECONDS:
                pm_regime, pm_permission, v10_regime, v10_permission = fetch_checklist_data()
                last_checklist_update = now
                if pm_regime is not None:
                    log.info(
                        f"Checklist actualizado — PM:{pm_regime}/{pm_permission} "
                        f"V10:{v10_regime}/{v10_permission}"
                    )
            # 'pm_regime' etc. se conservan entre ciclos (no se recalculan cada
            # 5s) — quedan en el último valor conocido hasta el próximo refresh
            # o en None si nunca se pudo consultar.

            # ── Deltas contra la historia ya acumulada (antes de agregar el punto actual)
            dvix_1m   = compute_delta(hist_vix,   vix,   now, 60)
            dvix_5m   = compute_delta(hist_vix,   vix,   now, 300)
            dvix1d_1m = compute_delta(hist_vix1d, vix1d, now, 60)
            dvix1d_5m = compute_delta(hist_vix1d, vix1d, now, 300)
            dspx_1m   = compute_delta(hist_spx,   spx,   now, 60)
            dspx_5m   = compute_delta(hist_spx,   spx,   now, 300)
            ddhp_1m   = compute_delta(hist_dhp,   dhp,   now, 60)
            ddhp_5m   = compute_delta(hist_dhp,   dhp,   now, 300)

            # ── Agregar el punto actual a la historia (para deltas futuros)
            if vix   is not None: hist_vix.append((now, vix))
            if vix1d is not None: hist_vix1d.append((now, vix1d))
            if spx   is not None: hist_spx.append((now, spx))
            if dhp   is not None: hist_dhp.append((now, dhp))

            row = {
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "vix": vix, "vix1d": vix1d, "vix1d_vix_ratio": ratio,
                "dvix_1m": dvix_1m, "dvix_5m": dvix_5m,
                "dvix1d_1m": dvix1d_1m, "dvix1d_5m": dvix1d_5m,
                "spx": spx, "dspx_1m": dspx_1m, "dspx_5m": dspx_5m,
                "dhp": dhp, "ddhp_1m": ddhp_1m, "ddhp_5m": ddhp_5m,
                "gex": gex, "dex": dex,
                "pm_regime": pm_regime, "pm_permission": pm_permission,
                "v10_regime": v10_regime, "v10_permission": v10_permission,
                # gfs_*: pendiente a propósito — solo existe como cálculo JS en
                # gaia_flow_v1.html hoy (confirmado 1-sept). No se duplica esa
                # lógica en Python; se decide más adelante si conviene mover el
                # cálculo al servidor como fuente única (frontend + logger).
                "gfs_calls": None, "gfs_puts": None, "gfs_net": None,
                "nearest_oi_node": None, "distance_to_oi_node": None,
                "nearest_gamma_node": None, "distance_to_gamma_node": None,
                "major_call_oi": None, "major_put_oi": None,
                "nearest_expiry_oi_pct": None, "nearest_expiry_gamma_pct": None,
                "spx_call_wall": spx_call_wall, "spx_put_wall": spx_put_wall,
                "spx_gamma_flip": spx_gamma_flip, "spx_gamma_node": spx_gamma_node,
                "spx_gravity_pin": spx_gravity_pin,
                "dist_call_wall": dist_call_wall, "dist_put_wall": dist_put_wall,
                "dist_gamma_flip": dist_gamma_flip, "dist_gamma_node": dist_gamma_node,
                "dist_gravity_pin": dist_gravity_pin,
                "spx_age_sec": spx_age, "vix_age_sec": vix_age,
                "vol_state": vol_state, "vol_raw_state": vol_raw,
                "vol_vix_5m_pct": vol_v5, "vol_ratio_5m_pct": vol_r5,
                "session_valid": vol_session,
            }

            if vix is not None or spx is not None:
                append_row(row)
                log.info(
                    f"Ciclo {cycle} | VIX:{vix} (Δ1m:{dvix_1m}) VIX1D:{vix1d} 1D/VIX:{ratio} | "
                    f"SPX:{spx} (Δ1m:{dspx_1m}) DHP:{dhp} (Δ1m:{ddhp_1m}) GEX:{gex} DEX:{dex} | "
                    f"CW:{spx_call_wall}({dist_call_wall}) PW:{spx_put_wall}({dist_put_wall}) "
                    f"Flip:{spx_gamma_flip}({dist_gamma_flip})"
                )
            else:
                log.warning(f"Ciclo {cycle}: sin datos de VIX ni SPX — no se escribe fila")

        except KeyboardInterrupt:
            log.info("Vol Engine Logger detenido por usuario.")
            break
        except Exception as e:
            log.error(f"Excepción no prevista ciclo {cycle}: {e}")

        safe_sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
