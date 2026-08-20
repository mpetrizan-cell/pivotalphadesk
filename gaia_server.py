"""
PivotAlphaDesk — GAIA DHP Server (Railway Edition)
Flask app que sirve gaia_chart_v3.html, gaia_flow_v1.html y gaia_ndx_chart.html
con autenticación y acceso desde cualquier dispositivo.

RAILWAY: 
  gaia_live.json     se recibe via POST /push     desde ts_gaia_chart.py local
  gaia_ndx_live.json se recibe via POST /push_ndx desde ts_gaia_ndx.py local
"""

from flask import Flask, jsonify, request, send_from_directory, redirect, session, render_template_string
import json, os, time, logging, threading
from functools import wraps
try:
    import requests as _requests
except ImportError:
    _requests = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=BASE_DIR)
app.secret_key = os.environ.get('SECRET_KEY', 'pad_gaia_2026_secret_key_change_in_prod')

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────────────
PUSH_TOKEN   = os.environ.get('PUSH_TOKEN', 'gaia_push_secret_2026')  # set en Railway env vars
ACCESS_CODES = {
    # Códigos generales (se mantienen por compatibilidad — considerar retirar
    # una vez que todos los accesos activos sean individuales)
    'PAD2026PRO': {'type': 'pro',   'days': None, 'name': 'General Pro',   'email': None, 'active': True},
    'PADTRIAL':   {'type': 'trial', 'days': 7,    'name': 'General Trial', 'email': None, 'active': True},

    # ── Códigos individuales ──────────────────────────────────────────────
    # Agregar una línea por persona al aprobar una solicitud del formulario.
    # Para revocar acceso: cambiar 'active' a False (no borrar la línea —
    # así queda el historial de quién tuvo acceso y cuándo se le sacó).
    # Formato del código sugerido: PAD-<APELLIDO o INICIALES>
    #
    # 'PAD-JPEREZ': {'type': 'pro', 'days': None, 'name': 'Juan Pérez', 'email': 'juan@example.com', 'active': True},
}
SESSION_HOURS = 12

# Datos en memoria (Railway no tiene filesystem persistente)
_live_data     = {}
_last_push     = 0
_live_data_ndx = {}
_last_push_ndx = 0
_live_data_etf = {}
_last_push_etf = 0
_live_data_nvda = {}
_last_push_nvda = 0
_live_data_vix = {}
_last_push_vix = 0

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# ── TRADESTATION TOKEN (env vars en Railway) ───────────────────────────────────
TS_CLIENT_ID    = os.environ.get('TS_CLIENT_ID', 'HMVux6j6ncGeYOVFbWVXyB0lSVL4WkWe')
TS_AUTH_URL     = 'https://signin.tradestation.com/oauth/token'
TS_API_URL      = 'https://api.tradestation.com'
_ts_token = {
    'access_token':  os.environ.get('TS_ACCESS_TOKEN', '').strip().replace('\n','').replace(' ',''),
    'refresh_token': os.environ.get('TS_REFRESH_TOKEN', '').strip(),
    'saved_at':      time.time() if os.environ.get('TS_ACCESS_TOKEN') else 0,
    'expires_in':    int(os.environ.get('TS_EXPIRES_IN', '1200')),
}
_ts_lock = threading.Lock()

def _ts_token_valid():
    age = time.time() - _ts_token['saved_at']
    return bool(_ts_token['access_token']) and age < (_ts_token['expires_in'] - 60)

def _ts_refresh():
    global _ts_token
    if not _ts_token['refresh_token']:
        log.warning('TS refresh: no refresh_token available')
        return False
    if not _requests:
        log.warning('TS refresh: requests library not available')
        return False
    try:
        r = _requests.post(TS_AUTH_URL, data={
            'grant_type':    'refresh_token',
            'client_id':     TS_CLIENT_ID,
            'refresh_token': _ts_token['refresh_token'],
        }, timeout=10)
        if r.status_code != 200:
            log.warning(f'TS refresh failed: {r.status_code}')
            return False
        data = r.json()
        with _ts_lock:
            _ts_token['access_token'] = data['access_token']
            if 'refresh_token' in data:
                _ts_token['refresh_token'] = data['refresh_token']
            _ts_token['saved_at']   = time.time()
            _ts_token['expires_in'] = data.get('expires_in', 1200)
        log.info('TS token refreshed OK')
        return True
    except Exception as e:
        log.warning(f'TS refresh exception: {e}')
        return False

def _ts_ensure_token():
    if _ts_token_valid():
        return True
    with _ts_lock:
        if _ts_token_valid():
            return True
        return _ts_refresh()

# Token refresh endpoint — llamado desde ts_gaia_chart.py local para actualizar
@app.route('/push_token', methods=['POST'])
def push_token():
    global _ts_token
    token = request.headers.get('X-Push-Token', '')
    if token != PUSH_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        data = request.get_json(force=True)
        with _ts_lock:
            if 'access_token'  in data: _ts_token['access_token']  = data['access_token']
            if 'refresh_token' in data: _ts_token['refresh_token'] = data['refresh_token']
            if 'saved_at'      in data: _ts_token['saved_at']      = float(data['saved_at'])
            if 'expires_in'    in data: _ts_token['expires_in']    = int(data['expires_in'])
        log.info('TS token updated via /push_token')
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── LOGIN PAGE ─────────────────────────────────────────────────────────────────
LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GAIA DHP | PivotAlphaDesk</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@700;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#070a0e;color:#e8f4f8;font-family:'Space Mono',monospace;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px;}
body::before{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(0,212,255,0.02) 1px,transparent 1px),
  linear-gradient(90deg,rgba(0,212,255,0.02) 1px,transparent 1px);
  background-size:48px 48px;pointer-events:none;}
.card{position:relative;z-index:1;width:100%;max-width:420px;
  background:#0d1319;border:1px solid #1e2d3d;padding:48px 36px;text-align:center;}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:26px;color:#fff;margin-bottom:4px;}
.logo span{color:#00d4ff;}
.sub{font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:#4a6070;margin-bottom:40px;}
.title{font-family:'Syne',sans-serif;font-weight:700;font-size:18px;color:#fff;margin-bottom:8px;}
.desc{font-size:10px;color:#4a6070;letter-spacing:.06em;margin-bottom:28px;line-height:1.6;}
input{width:100%;background:#111820;border:1px solid #1e2d3d;color:#e8f4f8;
  font-family:'Space Mono',monospace;font-size:14px;letter-spacing:.12em;
  padding:14px 16px;text-align:center;text-transform:uppercase;outline:none;
  transition:border-color .2s;margin-bottom:8px;}
input:focus{border-color:#00d4ff;}
.error{font-size:10px;color:#ff4444;letter-spacing:.08em;min-height:20px;margin-bottom:12px;}
.btn{width:100%;background:#00d4ff;color:#070a0e;border:none;padding:14px;
  font-family:'Space Mono',monospace;font-size:11px;letter-spacing:.14em;
  text-transform:uppercase;font-weight:700;cursor:pointer;transition:background .2s;}
.btn:hover{background:#fff;}
.trial{font-size:9px;color:#f0b429;letter-spacing:.08em;margin-top:20px;}
.links{margin-top:24px;display:flex;gap:20px;justify-content:center;}
.links a{font-size:9px;color:#4a6070;letter-spacing:.1em;text-decoration:none;}
.links a:hover{color:#00d4ff;}
</style>
</head>
<body>
<div class="card">
  <div class="logo">Pivot<span>Alpha</span>Desk</div>
  <div class="sub">GAIA Live · Dealer Positioning · 0DTE</div>
  <div class="title">Access GAIA DHP</div>
  <div class="desc">Enter your access code to view<br>real-time dealer hedging pressure.</div>
  <form method="POST" action="/login">
    <input type="text" name="code" placeholder="ACCESS CODE" maxlength="20" autocomplete="off" autofocus />
    <div class="error">{{ error }}</div>
    <button type="submit" class="btn">ACCESS GAIA →</button>
  </form>
  <div class="trial">Free trial available · use code PADTRIAL</div>
  <div class="links">
    <a href="https://pivotalphadesk.com">← pivotalphadesk.com</a>
    <a href="https://pivotalphadesk.com/#pricing">Get Pro Access</a>
  </div>
</div>
</body>
</html>
"""

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GAIA DHP | PivotAlphaDesk</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@700;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{background:#070a0e;color:#e8f4f8;font-family:'Space Mono',monospace;}
.topbar{background:#0d1319;border-bottom:1px solid #1e2d3d;
  padding:10px 24px;display:flex;align-items:center;justify-content:space-between;}
.logo{font-family:'Syne',sans-serif;font-weight:800;font-size:16px;color:#fff;}
.logo span{color:#00d4ff;}
.status{display:flex;align-items:center;gap:8px;font-size:10px;color:#4a6070;letter-spacing:.1em;}
.dot{width:6px;height:6px;border-radius:50%;background:#00c06a;animation:blink 1.5s infinite;}
@keyframes blink{0%,100%{opacity:1;}50%{opacity:.3;}}
.logout{font-size:9px;color:#4a6070;letter-spacing:.1em;text-decoration:none;
  border:1px solid #1e2d3d;padding:5px 12px;}
.logout:hover{color:#ff4444;border-color:#ff4444;}
.tabs{display:flex;background:#0d1319;border-bottom:1px solid #1e2d3d;padding:0 24px;}
.tab{padding:10px 20px;font-size:10px;letter-spacing:.12em;text-transform:uppercase;
  color:#4a6070;cursor:pointer;border-bottom:2px solid transparent;text-decoration:none;}
.tab:hover{color:#00d4ff;}
.tab.active{color:#00d4ff;border-bottom-color:#00d4ff;}
.frame-wrap{width:100%;height:calc(100vh - 82px);}
iframe{width:100%;height:100%;border:none;}
{% if trial_days %}
.trial-bar{background:rgba(240,180,41,0.1);border-bottom:1px solid #f0b429;
  padding:6px 24px;font-size:9px;color:#f0b429;letter-spacing:.1em;text-align:center;}
{% endif %}
</style>
</head>
<body>
<div class="topbar">
  <div class="logo">Pivot<span>Alpha</span>Desk · GAIA Live</div>
  <div class="status"><div class="dot"></div><span>LIVE · {{ spot }}</span></div>
  <a href="/logout" class="logout">LOGOUT</a>
</div>
{% if trial_days %}
<div class="trial-bar">TRIAL ACCESS · {{ trial_days }} days remaining · 
  <a href="https://pivotalphadesk.com/#pricing" style="color:#f0b429;">Upgrade to Pro →</a>
</div>
{% endif %}
<div class="tabs">
  <a href="/ndx" class="tab {% if active == 'ndx' %}active{% endif %}">
    GAIA NDX
  </a>
  <a href="/spy" class="tab {% if active == 'spy' %}active{% endif %}">
    GAIA SPY
  </a>
  <a href="/qqq" class="tab {% if active == 'qqq' %}active{% endif %}">
    GAIA QQQ
  </a>
  <a href="/surface" class="tab {% if active == 'surface' %}active{% endif %}">
    GAIA Surface
  </a>
  <a href="/chart" class="tab {% if active == 'chart' %}active{% endif %}">
    GEX Structure
  </a>
  <a href="/chart4" class="tab {% if active == 'chart4' %}active{% endif %}">
    GEX Structure v4
  </a>
  <a href="/pressure" class="tab {% if active == 'pressure' %}active{% endif %}" style="{% if active == 'pressure' %}background:rgba(204,68,255,0.15);border-color:#cc44ff;color:#cc44ff;{% endif %}">
    Pressure Map
  </a>
  <a href="/flow" class="tab {% if active == 'flow' %}active{% endif %}">
    DHP Flow
  </a>
  <a href="/cvd" class="tab {% if active == 'cvd' %}active{% endif %}">
    CVD
  </a>
  <a href="/alerts" class="tab {% if active == 'alerts' %}active{% endif %}">
    Alerts
  </a>
  <a href="/terminal" class="tab {% if active == 'terminal' %}active{% endif %}" style="{% if active == 'terminal' %}background:rgba(103,232,249,0.12);border-color:#67e8f9;color:#67e8f9;{% endif %}">
    LW Terminal
  </a>
  <a href="/ndx_terminal" class="tab {% if active == 'ndx_terminal' %}active{% endif %}" style="{% if active == 'ndx_terminal' %}background:rgba(103,232,249,0.12);border-color:#67e8f9;color:#67e8f9;{% endif %}">
    NDX Terminal
  </a>
  <a href="/checklist" class="tab {% if active == 'checklist' %}active{% endif %}" style="{% if active == 'checklist' %}background:rgba(232,121,249,0.12);border-color:#e879f9;color:#e879f9;{% endif %}">
    Checklist
  </a>
</div>
<div class="frame-wrap">
  <iframe src="/{{ page }}" id="gaia-frame"></iframe>
</div>
</body>
</html>
"""

# ── AUTH HELPERS ───────────────────────────────────────────────────────────────
def is_authenticated():
    if 'code' not in session: return False
    expiry = session.get('expiry', 0)
    if time.time() > expiry: return False
    code = session.get('code', '').upper()
    cfg = ACCESS_CODES.get(code)
    if not cfg or not cfg.get('active', True): return False
    if cfg['days'] is not None:
        trial_expiry = session.get('trial_expiry', 0)
        if time.time() > trial_expiry: return False
    return True

def get_trial_days():
    code = session.get('code', '').upper()
    if code not in ACCESS_CODES: return None
    if ACCESS_CODES[code]['days'] is None: return None
    trial_expiry = session.get('trial_expiry', 0)
    remaining = (trial_expiry - time.time()) / 86400
    return max(0, int(remaining)) if remaining > 0 else 0

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not is_authenticated():
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

def get_spot():
    try:
        return f"{_live_data.get('spot_es', '——'):.2f}"
    except:
        return str(_live_data.get('spot_es', '——'))

def get_spot_ndx():
    try:
        return f"{_live_data_ndx.get('spot_ndx', '——'):.3f}"
    except:
        return str(_live_data_ndx.get('spot_ndx', '——'))

def get_spot_spy():
    try:
        return f"{_live_data_etf.get('spy', {}).get('spot', '——'):.2f}"
    except:
        return str(_live_data_etf.get('spy', {}).get('spot', '——'))

def get_spot_qqq():
    try:
        return f"{_live_data_etf.get('qqq', {}).get('spot', '——'):.2f}"
    except:
        return str(_live_data_etf.get('qqq', {}).get('spot', '——'))

# ── PUSH ENDPOINT (llamado desde ts_gaia_chart.py local) ──────────────────────
@app.route('/push', methods=['POST'])
def push_data():
    global _live_data, _last_push
    token = request.headers.get('X-Push-Token', '')
    if token != PUSH_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'no data'}), 400
        # Handle token update from ts_gaia_chart.py
        if data.get('_token_update'):
            with _ts_lock:
                if data.get('access_token'):  _ts_token['access_token']  = data['access_token']
                if data.get('refresh_token'): _ts_token['refresh_token'] = data['refresh_token']
                if data.get('saved_at'):      _ts_token['saved_at']      = float(data['saved_at'])
                if data.get('expires_in'):    _ts_token['expires_in']    = int(data['expires_in'])
            log.info('TS token updated via /push')
            return jsonify({'status': 'ok', 'token_updated': True})
        _live_data = data
        _last_push = time.time()
        return jsonify({'status': 'ok', 'timestamp': _last_push})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── PUSH NDX (llamado desde ts_gaia_ndx.py local) ─────────────────────────────
@app.route('/push_ndx', methods=['POST'])
def push_ndx_data():
    global _live_data_ndx, _last_push_ndx
    token = request.headers.get('X-Push-Token', '')
    if token != PUSH_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'no data'}), 400
        _live_data_ndx = data
        _last_push_ndx = time.time()
        return jsonify({'status': 'ok', 'timestamp': _last_push_ndx})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── PUSH ETF (llamado desde ts_gaia_etf.py local) ─────────────────────────────
@app.route('/push_etf', methods=['POST'])
def push_etf_data():
    global _live_data_etf, _last_push_etf
    token = request.headers.get('X-Push-Token', '')
    if token != PUSH_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'no data'}), 400
        _live_data_etf = data
        _last_push_etf = time.time()
        return jsonify({'status': 'ok', 'timestamp': _last_push_etf})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── PUSH VIX (llamado desde ts_gaia_vix.py local) ─────────────────────────────
# Fase 1 GAIA Vol Engine — solo recibe y almacena, no modifica GAIA SPX ni el
# dashboard principal. Mismo patrón que /push_etf.
@app.route('/push_vix', methods=['POST'])
def push_vix_data():
    global _live_data_vix, _last_push_vix
    token = request.headers.get('X-Push-Token', '')
    if token != PUSH_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'no data'}), 400
        _live_data_vix = data
        _last_push_vix = time.time()
        return jsonify({'status': 'ok', 'timestamp': _last_push_vix})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── PUSH NVDA (llamado desde ts_gaia_nvda.py local) ───────────────────────────
@app.route('/push_nvda', methods=['POST'])
def push_nvda_data():
    global _live_data_nvda, _last_push_nvda
    token = request.headers.get('X-Push-Token', '')
    if token != PUSH_TOKEN:
        return jsonify({'error': 'unauthorized'}), 401
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'no data'}), 400
        _live_data_nvda = data
        _last_push_nvda = time.time()
        return jsonify({'status': 'ok', 'timestamp': _last_push_nvda})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/gaia_nvda_live.json')
def gaia_nvda_live():
    if not _live_data_nvda:
        return jsonify({'status': 'waiting', 'symbol': 'NVDA'})
    age = time.time() - _last_push_nvda
    return jsonify({**_live_data_nvda, 'data_age_seconds': round(age, 1)})


@app.route('/')
def index():
    if is_authenticated():
        return redirect('/chart')
    return redirect('/login')

@app.route('/login', methods=['GET','POST'])
def login():
    error = ''
    if request.method == 'POST':
        code = request.form.get('code', '').upper().strip()
        cfg = ACCESS_CODES.get(code)
        if cfg and cfg.get('active', True):
            session['code']   = code
            session['expiry'] = time.time() + SESSION_HOURS * 3600
            if cfg['days'] is not None:
                trial_key = f'trial_start_{code}'
                if trial_key not in session:
                    session[trial_key] = time.time()
                session['trial_expiry'] = session[trial_key] + cfg['days'] * 86400
            log.info(f"Login OK: {code} ({cfg.get('name','—')}) from {request.remote_addr}")
            return redirect('/chart')
        elif cfg and not cfg.get('active', True):
            log.warning(f"Login DENEGADO (código revocado): {code} ({cfg.get('name','—')}) from {request.remote_addr}")
            error = 'Invalid code. Try again.'
        else:
            log.warning(f"Login DENEGADO (código inexistente): {code} from {request.remote_addr}")
            error = 'Invalid code. Try again.'
    return render_template_string(LOGIN_HTML, error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

@app.route('/chart')
@require_auth
def chart():
    return render_template_string(DASHBOARD_HTML,
        active='chart', page='gaia_chart_v3.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/chart4')
@require_auth
def chart4():
    return render_template_string(DASHBOARD_HTML,
        active='chart4', page='gaia_chart_v4.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/pressure')
@require_auth
def pressure():
    return render_template_string(DASHBOARD_HTML,
        active='pressure', page='gaia_pressure_map.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/gaia_pressure_map.html')
@require_auth
def serve_pressure():
    return send_from_directory(BASE_DIR, 'gaia_pressure_map.html')

@app.route('/ndx')
@require_auth
def ndx():
    return render_template_string(DASHBOARD_HTML,
        active='ndx', page='gaia_ndx_chart.html',
        spot=get_spot_ndx(), trial_days=get_trial_days())

@app.route('/etf')
@require_auth
def etf():
    return render_template_string(DASHBOARD_HTML,
        active='etf', page='gaia_etf_chart.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/spy')
@require_auth
def spy():
    return render_template_string(DASHBOARD_HTML,
        active='spy', page='gaia_spy_chart.html',
        spot=get_spot_spy(), trial_days=get_trial_days())

@app.route('/qqq')
@require_auth
def qqq():
    return render_template_string(DASHBOARD_HTML,
        active='qqq', page='gaia_qqq_chart.html',
        spot=get_spot_qqq(), trial_days=get_trial_days())

@app.route('/surface')
@require_auth
def surface():
    return render_template_string(DASHBOARD_HTML,
        active='surface', page='gaia_surface.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/flow')
@require_auth
def flow():
    return render_template_string(DASHBOARD_HTML,
        active='flow', page='gaia_flow_v1.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/cvd')
@require_auth
def cvd():
    return render_template_string(DASHBOARD_HTML,
        active='cvd', page='gaia_cvd_v1.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/alerts')
@require_auth
def alerts():
    return render_template_string(DASHBOARD_HTML,
        active='alerts', page='gaia_alerts_v1.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/gaia_alerts_v1.html')
@require_auth
def serve_alerts():
    return send_from_directory(BASE_DIR, 'gaia_alerts_v1.html')

@app.route('/gaia_ndx_chart.html')
@require_auth
def serve_ndx_chart():
    return send_from_directory(BASE_DIR, 'gaia_ndx_chart.html')

@app.route('/gaia_etf_chart.html')
@require_auth
def serve_etf_chart():
    return send_from_directory(BASE_DIR, 'gaia_etf_chart.html')

@app.route('/gaia_spy_chart.html')
@require_auth
def serve_spy_chart():
    return send_from_directory(BASE_DIR, 'gaia_spy_chart.html')

@app.route('/gaia_qqq_chart.html')
@require_auth
def serve_qqq_chart():
    return send_from_directory(BASE_DIR, 'gaia_qqq_chart.html')

@app.route('/gaia_surface.html')
@require_auth
def serve_surface():
    return send_from_directory(BASE_DIR, 'gaia_surface.html')

@app.route('/gaia_ndx_live.json')
@require_auth
def serve_ndx_json():
    if not _live_data_ndx:
        return jsonify({'error': 'no NDX data yet', 'status': 'waiting'}), 503
    resp = jsonify(_live_data_ndx)
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return resp

@app.route('/gaia_etf_live.json')
@require_auth
def serve_etf_json():
    if not _live_data_etf:
        return jsonify({'error': 'no ETF data yet', 'status': 'waiting'}), 503
    resp = jsonify(_live_data_etf)
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return resp

@app.route('/gaia_vix_live.json')
@require_auth
def serve_vix_json():
    if not _live_data_vix:
        return jsonify({'error': 'no VIX data yet', 'status': 'waiting'}), 503
    resp = jsonify(_live_data_vix)
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return resp

@app.route('/gaia_cvd_v1.html')
@require_auth
def serve_cvd():
    return send_from_directory(BASE_DIR, 'gaia_cvd_v1.html')

@app.route('/gaia_chart_v3.html')
@require_auth
def serve_chart():
    return send_from_directory(BASE_DIR, 'gaia_chart_v3.html')

@app.route('/gaia_chart_v4.html')
@require_auth
def serve_chart_v4():
    return send_from_directory(BASE_DIR, 'gaia_chart_v4.html')

@app.route('/gaia_flow_v1.html')
@require_auth
def serve_flow():
    return send_from_directory(BASE_DIR, 'gaia_flow_v1.html')

@app.route('/gaia_live.json')
@require_auth
def serve_json():
    if not _live_data:
        return jsonify({'error': 'no data yet', 'status': 'waiting'}), 503
    resp = jsonify(_live_data)
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return resp


# ── PRESSURE MAP ENGINE — portado de gaia_pressure_map.html (misma fórmula exacta) ──
def _pm_clamp(v, mn=0.0, mx=1.0):
    return max(mn, min(mx, v))

def _pm_norm_abs(v, mx):
    return _pm_clamp(abs(v) / mx) if mx else 0.0

def _pm_get_conf(raw, strike):
    for c in (raw.get('confluence') or []):
        try:
            if float(c.get('strike', -1e9)) == float(strike):
                return c.get('strength', 1)
        except (TypeError, ValueError):
            continue
    return 1

def _pm_calc_cvx(row, max_g):
    g = abs(row.get('call_gamma') or 0) + abs(row.get('put_gamma') or 0)
    gi = _pm_norm_abs(g, max_g)
    pi = _pm_clamp((row.get('prediction_score') or 0) / 100)
    ivs = _pm_clamp(abs((row.get('put_iv') or 0) - (row.get('call_iv') or 0)) / 5)
    return round(_pm_clamp(gi * .45 + pi * .4 + ivs * .15) * 100)

def _pm_calc_dex(row):
    return (row.get('call_delta_oi') or 0) + (row.get('put_delta_oi') or 0)

def _pm_eq(a, b):
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        return False

def _pm_classify_zone(strike, spot, net_gex, net_dhp, dex, cvx, conf, levels, trig_active):
    near = abs(strike - spot) <= 10
    levels = levels or {}
    is_cw   = _pm_eq(strike, levels.get('call_wall'))
    is_pw   = _pm_eq(strike, levels.get('put_wall'))
    is_node = _pm_eq(strike, levels.get('gamma_node'))
    is_flip = _pm_eq(strike, levels.get('gamma_flip'))
    is_pin  = _pm_eq(strike, levels.get('gravity_pin'))
    if trig_active and net_gex < 0 and near and cvx >= 65: return 'EXPANSION'
    if (is_cw or is_node or is_pin) and net_gex > 0 and conf >= 2 and dex >= 0: return 'ABSORPTION'
    if is_pw and net_gex < 0: return 'FRAGILE'
    if net_gex < 0 and near: return 'FRAGILE'
    if net_gex < 0 and dex < 0 and cvx >= 55: return 'CHASE'
    if (is_flip or net_gex > 0) and cvx >= 50: return 'REACTIVE'
    if net_gex > 0 and abs(net_dhp) < 2: return 'COMPRESSION'
    return 'BALANCED'

def _pm_build_visual(net_gex, net_dhp, cvx, conf, max_g, max_d):
    gi = _pm_norm_abs(net_gex, max_g)
    di = _pm_norm_abs(net_dhp, max_d)
    cfi = _pm_clamp(conf / 7)
    return {
        'glow':  _pm_clamp(.15 + gi * .45 + cfi * .35),
        'thick': _pm_clamp(.2 + cfi * .55 + gi * .25),
        'turb':  _pm_clamp(_pm_clamp(cvx / 100) * .65 + di * .35),
        'fw':    _pm_clamp(.25 + gi * .65 + di * .1),
    }

def _pm_detect_drift(zones, spot):
    ap = sum(z['net_gex'] * (z['visual']['fw'] or 0) for z in zones if z['strike'] > spot)
    bp = sum(z['net_gex'] * (z['visual']['fw'] or 0) for z in zones if z['strike'] < spot)
    net = bp - ap
    if net < -25000: return 'UPWARD_STABILIZATION'
    if net > 25000: return 'DOWNWARD_FRAGILITY'
    return 'NEUTRAL_FLOW'

def _pm_calc_dsi(zones):
    if not zones: return 50
    abs_str = sum(z['visual']['glow'] + z['visual']['thick'] for z in zones if z['state'] in ('ABSORPTION', 'COMPRESSION'))
    frag_str = sum(z['visual']['glow'] + z['visual']['turb'] for z in zones if z['state'] in ('FRAGILE', 'CHASE', 'EXPANSION'))
    avg_turb = sum(z['visual']['turb'] for z in zones) / len(zones)
    frag_pct = (frag_str / ((abs_str + frag_str) or 1)) * 100
    return round(max(0, min(100, 70 + (100 - frag_pct) * .25 - frag_pct * .35 - avg_turb * 25)))

def _pm_calc_vts(zones, spot):
    if not zones: return 50
    frag_z = [z for z in zones if z['state'] in ('FRAGILE', 'CHASE', 'EXPANSION')]
    near_f = sorted(frag_z, key=lambda z: abs(z['dist']))[0] if frag_z else None
    trig_prox = max(0, 100 - abs(near_f['strike'] - spot) * 4) if near_f else 0
    avg_turb = sum(z['visual']['turb'] for z in zones) / len(zones)
    frag_pct = (len(frag_z) / len(zones) * 100)
    return round(max(0, min(100, 20 + frag_pct * .45 + trig_prox * .25 + avg_turb * 35)))

def _pm_calc_mcs(zones, dsi, vts, drift):
    if not zones: return 50
    has_abs = any(z['state'] == 'ABSORPTION' for z in zones)
    has_frag = any(z['state'] == 'FRAGILE' for z in zones)
    balanced = sum(1 for z in zones if z['state'] == 'BALANCED')
    extreme_agree = (dsi >= 75 and vts <= 30) or (dsi <= 35 and vts >= 75)
    mixed = dsi >= 50 and vts >= 50
    contradiction = has_abs and has_frag and abs(dsi - 50) < 15
    clear_drift = drift != 'NEUTRAL_FLOW'
    mcs = 50
    if extreme_agree: mcs += 25
    if has_abs or has_frag: mcs += 10
    if clear_drift: mcs += 10
    if contradiction: mcs -= 20
    if mixed: mcs -= 15
    mcs -= (balanced / len(zones)) * 20
    return round(max(0, min(100, mcs)))

def _pm_trig_status(vts):
    if vts >= 90: return 'EXTREME'
    if vts >= 75: return 'ACTIVE'
    if vts >= 56: return 'ARMING'
    if vts >= 31: return 'WATCH'
    return 'LOW'

def _pm_classify_market(dsi, vts):
    if dsi < 35 and vts >= 75: return 'EXPANSIVE'
    if dsi >= 75 and vts <= 35: return 'STABLE'
    if dsi < 65 or vts >= 50: return 'FRAGILE'
    return 'BALANCED'

def _pm_trade_perm(ms, dsi, vts):
    if dsi < 35 and vts >= 75: return 'DEFENSIVE'
    if ms == 'EXPANSIVE': return 'EXPANSIVE'
    if ms == 'FRAGILE': return 'FRAGILE'
    if ms == 'STABLE': return 'FAVORABLE'
    return 'BALANCED'

# market_state (STABLE/FRAGILE/EXPANSIVE/BALANCED) -> vocabulario de GAIA Execution regime input
_MARKET_TO_REGIME = {'STABLE': 'DAMPENED', 'FRAGILE': 'FRAGILE', 'EXPANSIVE': 'AMPLIFIED', 'BALANCED': 'BALANCED'}
# zone state (7 posibles) -> vocabulario de 3 opciones del pm_state input de Execution
_ZONE_TO_PM = {'ABSORPTION': 'ABSORPTION', 'COMPRESSION': 'COMPRESSION'}  # el resto cae en BALANCED por default

def compute_pressure_schema(raw):
    """Reimplementación exacta de adaptRaw() de gaia_pressure_map.html — misma fórmula, mismo resultado."""
    strikes = raw.get('strikes') or []
    spot = raw.get('spot_spx')
    if not strikes or spot is None:
        return None
    levels = raw.get('levels') or {}
    sorted_strikes = sorted(strikes, key=lambda s: s['strike'])
    PROXIMITY = 10
    spot_idx = min(range(len(sorted_strikes)), key=lambda i: abs(sorted_strikes[i]['strike'] - spot))
    start = max(0, spot_idx - PROXIMITY)
    end = min(len(sorted_strikes) - 1, spot_idx + PROXIMITY)
    filtered = sorted_strikes[start:end + 1]
    max_g = max([abs(s.get('net_gex') or 0) for s in filtered] + [1])
    max_d = max([abs(s.get('net_dhp') or 0) for s in filtered] + [1])
    max_gamma = max([abs(s.get('call_gamma') or 0) + abs(s.get('put_gamma') or 0) for s in filtered] + [1])

    def build_zones(trig_active):
        zones = []
        for row in filtered:
            strike = row['strike']
            conf = _pm_get_conf(raw, strike)
            cvx = _pm_calc_cvx(row, max_gamma)
            dex = _pm_calc_dex(row)
            net_gex = row.get('net_gex') or 0
            net_dhp = row.get('net_dhp') or 0
            state = _pm_classify_zone(strike, spot, net_gex, net_dhp, dex, cvx, conf, levels, trig_active)
            visual = _pm_build_visual(net_gex, net_dhp, cvx, conf, max_g, max_d)
            zones.append({'strike': strike, 'net_gex': net_gex, 'net_dhp': net_dhp,
                          'state': state, 'visual': visual, 'dist': strike - spot})
        return zones

    zones1 = build_zones(False)
    drift = _pm_detect_drift(zones1, spot)
    vts1 = _pm_calc_vts(zones1, spot)
    zones = build_zones(vts1 >= 75)
    dsi = _pm_calc_dsi(zones)
    vts = _pm_calc_vts(zones, spot)
    mcs = _pm_calc_mcs(zones, dsi, vts, drift)
    ms = _pm_classify_market(dsi, vts)
    perm = _pm_trade_perm(ms, dsi, vts)
    trig = _pm_trig_status(vts)
    dom = max(zones, key=lambda z: z['visual']['glow'] + z['visual']['thick']) if zones else None
    frag_c = [z for z in zones if z['state'] in ('FRAGILE', 'CHASE', 'EXPANSION')]
    frag_z = min(frag_c, key=lambda z: abs(z['dist'])) if frag_c else None

    zone_states = {}
    for lvl_key in ('call_wall', 'put_wall', 'gamma_node', 'gamma_flip', 'gravity_pin'):
        target = levels.get(lvl_key)
        if target is not None:
            match = next((z for z in zones if _pm_eq(z['strike'], target)), None)
            zone_states[lvl_key] = match['state'] if match else None

    return {
        'market_state': ms,
        'regime': _MARKET_TO_REGIME.get(ms, 'BALANCED'),
        'trade_permission': perm,
        'dsi': dsi, 'vts': vts, 'mcs': mcs,
        'trigger_status': trig,
        'dominant_node': dom['strike'] if dom else None,
        'fragile_zone': frag_z['strike'] if frag_z else None,
        'pressure_drift': drift,
        'zone_states': zone_states,
        'pressure_state_by_level': {k: _ZONE_TO_PM.get(v, 'BALANCED') for k, v in zone_states.items()},
    }

def compute_terminal_v10_state(raw):
    """Reimplementación exacta de computeState() de gaia_structure_terminal_v10.html.
    Fórmula deliberadamente distinta a compute_pressure_schema() — son dos motores
    reales y separados, no una versión vieja/nueva del mismo cálculo."""
    strikes = raw.get('strikes') or []
    if not strikes:
        return None
    dex = sum((s.get('call_delta_oi') or 0) - (s.get('put_delta_oi') or 0) for s in strikes)
    total_net_dhp = sum(s.get('net_dhp') or 0 for s in strikes)
    avg_net_dhp = total_net_dhp / max(1, len(strikes))
    dhp_pressure = min(60, abs(avg_net_dhp) * 4)
    dhp_momentum = raw.get('dhp_momentum') or 0
    vts = round(32 + dhp_pressure + (18 if dhp_momentum < 0 else 0) + (8 if abs(dex) > 2500 else 0))
    dsi = max(0, min(100, 76 - vts / 3))
    mcs = max(0, min(100, 55 + (8 if dex > 0 else (-8 if dex < 0 else 0))))
    regime = 'FRAGILE' if vts > 65 else ('STABLE' if dsi > 72 else 'BALANCED')
    permission = 'DEFENSIVE' if regime == 'FRAGILE' else ('FAVORABLE' if mcs > 70 else 'WATCH')
    dex_bias = 'BUY HEDGE' if dex > 1500 else ('SELL HEDGE' if dex < -1500 else 'MIXED')
    return {
        'dsi': round(dsi), 'vts': vts, 'mcs': round(mcs),
        'regime': regime, 'trade_permission': permission,
        'dex_bias': dex_bias, 'dex': round(dex, 2),
    }

@app.route('/checklist_data')
def checklist_data():
    """Public endpoint for GAIA Trade Checklist — no auth required."""
    if not _live_data:
        return jsonify({'status': 'waiting', 'error': 'no data yet'}), 503
    d = _live_data
    payload = {
        'timestamp':     d.get('timestamp'),
        'expiration':    d.get('expiration'),
        'status':        d.get('status', 'live'),
        'spot_spx':      d.get('spot_spx'),
        'spot_es':       d.get('spot_es'),
        'basis':         d.get('basis'),
        'total_dhp':     d.get('total_dhp'),
        'dhp_momentum':  d.get('dhp_momentum'),
        'dhp_direction': d.get('dhp_direction'),
        'hiro_call':     d.get('hiro_call'),
        'hiro_put':      d.get('hiro_put'),
        'hiro_total':    d.get('hiro_total'),
        'levels':        d.get('levels', {}),
        'levels_es':     d.get('levels_es', {}),
        'confluence':    d.get('confluence', []),
    }
    # ── Motor 1: Pressure Map (clasificación por zona, ±10 strikes) ──
    try:
        pm = compute_pressure_schema(d)
        if pm:
            payload['pressure_map'] = {
                'market_state':            pm['market_state'],
                'regime':                  pm['regime'],
                'trade_permission':        pm['trade_permission'],
                'dsi': pm['dsi'], 'vts': pm['vts'], 'mcs': pm['mcs'],
                'trigger_status':          pm['trigger_status'],
                'dominant_node':           pm['dominant_node'],
                'fragile_zone':            pm['fragile_zone'],
                'pressure_drift':          pm['pressure_drift'],
                'pressure_state_by_level': pm['pressure_state_by_level'],
            }
    except Exception as e:
        log.warning(f'compute_pressure_schema failed: {e}')

    # ── Motor 2: Terminal V10 (heurístico agregado sobre DEX/DHP total) ──
    try:
        t10 = compute_terminal_v10_state(d)
        if t10:
            payload['terminal_v10'] = t10
    except Exception as e:
        log.warning(f'compute_terminal_v10_state failed: {e}')

    # Nota: pressure_map y terminal_v10 son dos motores reales y distintos —
    # pueden divergir legítimamente. No se combinan en un solo valor "regime"
    # a propósito, para no ocultar la discrepancia cuando exista.
    resp = jsonify(payload)
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/health')
def health():
    age     = time.time() - _last_push     if _last_push     else None
    age_ndx = time.time() - _last_push_ndx if _last_push_ndx else None
    age_etf = time.time() - _last_push_etf if _last_push_etf else None
    age_vix = time.time() - _last_push_vix if _last_push_vix else None
    return jsonify({
        'status':     'ok' if _live_data else 'waiting',
        'spot_es':    _live_data.get('spot_es')          if _live_data     else None,
        'spot_ndx':   _live_data_ndx.get('spot_ndx')     if _live_data_ndx else None,
        'spot_spy':   _live_data_etf.get('spy',{}).get('spot') if _live_data_etf else None,
        'spot_qqq':   _live_data_etf.get('qqq',{}).get('spot') if _live_data_etf else None,
        'spot_vix':   _live_data_vix.get('vix',{}).get('spot')    if _live_data_vix else None,
        'spx_push_seconds_ago': round(age, 1)     if age     else None,
        'ndx_push_seconds_ago': round(age_ndx, 1) if age_ndx else None,
        'etf_push_seconds_ago': round(age_etf, 1) if age_etf else None,
        'vix_push_seconds_ago': round(age_vix, 1) if age_vix else None,
        'dhp_spx':  _live_data.get('total_dhp')               if _live_data     else None,
        'dhp_ndx':  _live_data_ndx.get('total_dhp')           if _live_data_ndx else None,
        'dhp_spy':  _live_data_etf.get('spy',{}).get('total_dhp') if _live_data_etf else None,
        'dhp_qqq':  _live_data_etf.get('qqq',{}).get('total_dhp') if _live_data_etf else None,
        'dhp_vix':  _live_data_vix.get('vix',{}).get('total_dhp')  if _live_data_vix else None,
    })

# ── BARS ENDPOINT ─────────────────────────────────────────────────────────────
TF_CONFIG = {
    'M1':  {'unit': 'Minute', 'interval': 1,  'barsback': 390},
    'M5':  {'unit': 'Minute', 'interval': 5,  'barsback': 100},
    'M15': {'unit': 'Minute', 'interval': 15, 'barsback': 80},
}

@app.route('/bars')
@require_auth
def bars():
    symbol = request.args.get('symbol', 'ESM26').upper()
    tf     = request.args.get('tf', 'M1').upper()
    cfg    = TF_CONFIG.get(tf, TF_CONFIG['M1'])

    if not _ts_ensure_token():
        return jsonify({'error': 'token unavailable', 'status': 'auth_failed'}), 503

    if not _requests:
        return jsonify({'error': 'requests not installed on server'}), 500

    url = (f"{TS_API_URL}/v3/marketdata/barcharts/{symbol}"
           f"?unit={cfg['unit']}&interval={cfg['interval']}&barsback={cfg['barsback']}")
    try:
        r = _requests.get(url, headers={'Authorization': f"Bearer {_ts_token['access_token']}"}, timeout=10)
        if r.status_code == 401:
            # Try refresh once
            if _ts_refresh():
                r = _requests.get(url, headers={'Authorization': f"Bearer {_ts_token['access_token']}"}, timeout=10)
            else:
                return jsonify({'error': 'token expired', 'status': 'auth_failed'}), 401
        if not r.ok:
            return jsonify({'error': f'TS API {r.status_code}', 'status': 'api_error'}), 502
        data = r.json()
        bars_raw = data.get('Bars', data.get('bars', []))
        bars_out = []
        for b in bars_raw:
            try:
                ts_raw = (b.get('TimeStamp') or b.get('timestamp',''))[:19]
                # Parse as UTC then convert to ET (UTC-4 EDT)
                import calendar
                t = time.strptime(ts_raw, '%Y-%m-%dT%H:%M:%S')
                utc_ts = calendar.timegm(t)  # UTC timestamp
                et_ts  = utc_ts - (4 * 3600)  # convert to ET (EDT)
                bars_out.append({
                    'time': et_ts,
                    'open':  float(b.get('Open',  b.get('open',  0))),
                    'high':  float(b.get('High',  b.get('high',  0))),
                    'low':   float(b.get('Low',   b.get('low',   0))),
                    'close': float(b.get('Close', b.get('close', 0))),
                    'volume':int(b.get('TotalVolume', b.get('volume', 0))),
                })
            except Exception:
                continue
        resp = jsonify({'status': 'ok', 'symbol': symbol, 'tf': tf, 'bars': bars_out, 'count': len(bars_out)})
        resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        return resp
    except Exception as e:
        log.warning(f'/bars error: {e}')
        return jsonify({'error': str(e)}), 500

# ── LW TERMINAL NDX ROUTE ─────────────────────────────────────────────────────
@app.route('/ndx_terminal')
@require_auth
def ndx_terminal():
    return render_template_string(DASHBOARD_HTML,
        active='ndx_terminal', page='gaia_ndx_terminal_v10.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/gaia_ndx_terminal_v10.html')
@require_auth
def serve_ndx_terminal():
    return send_from_directory(BASE_DIR, 'gaia_ndx_terminal_v10.html')

# ── CHECKLIST ROUTE ───────────────────────────────────────────────────────────
@app.route('/checklist')
@require_auth
def checklist():
    return render_template_string(DASHBOARD_HTML,
        active='checklist', page='gaia_checklist.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/gaia_checklist.html')
@require_auth
def serve_checklist():
    return send_from_directory(BASE_DIR, 'gaia_checklist.html')

# ── LW TERMINAL ROUTE ─────────────────────────────────────────────────────────
@app.route('/terminal')
@require_auth
def terminal():
    return render_template_string(DASHBOARD_HTML,
        active='terminal', page='gaia_structure_terminal_v10.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/gaia_structure_terminal_v10.html')
@require_auth
def serve_terminal():
    return send_from_directory(BASE_DIR, 'gaia_structure_terminal_v10.html')

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA DHP Server (Railway)")
    log.info("=" * 60)
    app.run(host='0.0.0.0', port=port, debug=False)
