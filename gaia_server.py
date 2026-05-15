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
    'PAD2026PRO': {'type': 'pro',   'days': None},
    'PADTRIAL':   {'type': 'trial', 'days': 7},
}
SESSION_HOURS = 12

# Datos en memoria (Railway no tiene filesystem persistente)
_live_data     = {}
_last_push     = 0
_live_data_ndx = {}
_last_push_ndx = 0
_live_data_etf = {}
_last_push_etf = 0

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# ── TRADESTATION TOKEN (env vars en Railway) ───────────────────────────────────
TS_CLIENT_ID    = os.environ.get('TS_CLIENT_ID', 'HMVux6j6ncGeYOVFbWVXyB0lSVL4WkWe')
TS_AUTH_URL     = 'https://signin.tradestation.com/oauth/token'
TS_API_URL      = 'https://api.tradestation.com'
_ts_token = {
    'access_token':  os.environ.get('TS_ACCESS_TOKEN', ''),
    'refresh_token': os.environ.get('TS_REFRESH_TOKEN', ''),
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
    if code in ACCESS_CODES and ACCESS_CODES[code]['days'] is not None:
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

# ── ROUTES ─────────────────────────────────────────────────────────────────────
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
        if code in ACCESS_CODES:
            session['code']   = code
            session['expiry'] = time.time() + SESSION_HOURS * 3600
            cfg = ACCESS_CODES[code]
            if cfg['days'] is not None:
                trial_key = f'trial_start_{code}'
                if trial_key not in session:
                    session[trial_key] = time.time()
                session['trial_expiry'] = session[trial_key] + cfg['days'] * 86400
            log.info(f"Login: {code} from {request.remote_addr}")
            return redirect('/chart')
        else:
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

@app.route('/health')
def health():
    age     = time.time() - _last_push     if _last_push     else None
    age_ndx = time.time() - _last_push_ndx if _last_push_ndx else None
    age_etf = time.time() - _last_push_etf if _last_push_etf else None
    return jsonify({
        'status':     'ok' if _live_data else 'waiting',
        'spot_es':    _live_data.get('spot_es')          if _live_data     else None,
        'spot_ndx':   _live_data_ndx.get('spot_ndx')     if _live_data_ndx else None,
        'spot_spy':   _live_data_etf.get('spy',{}).get('spot') if _live_data_etf else None,
        'spot_qqq':   _live_data_etf.get('qqq',{}).get('spot') if _live_data_etf else None,
        'spx_push_seconds_ago': round(age, 1)     if age     else None,
        'ndx_push_seconds_ago': round(age_ndx, 1) if age_ndx else None,
        'etf_push_seconds_ago': round(age_etf, 1) if age_etf else None,
        'dhp_spx':  _live_data.get('total_dhp')               if _live_data     else None,
        'dhp_ndx':  _live_data_ndx.get('total_dhp')           if _live_data_ndx else None,
        'dhp_spy':  _live_data_etf.get('spy',{}).get('total_dhp') if _live_data_etf else None,
        'dhp_qqq':  _live_data_etf.get('qqq',{}).get('total_dhp') if _live_data_etf else None,
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

# ── LW TERMINAL ROUTE ─────────────────────────────────────────────────────────
@app.route('/terminal')
@require_auth
def terminal():
    return render_template_string(DASHBOARD_HTML,
        active='terminal', page='gaia_structure_terminal_v9.html',
        spot=get_spot(), trial_days=get_trial_days())

@app.route('/gaia_structure_terminal_v9.html')
@require_auth
def serve_terminal():
    return send_from_directory(BASE_DIR, 'gaia_structure_terminal_v9.html')

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA DHP Server (Railway)")
    log.info("=" * 60)
    app.run(host='0.0.0.0', port=port, debug=False)
