"""
PivotAlphaDesk — GAIA DHP Server (Railway Edition)
Flask app que sirve gaia_chart_v3.html, gaia_flow_v1.html y gaia_ndx_chart.html
con autenticación y acceso desde cualquier dispositivo.

RAILWAY: 
  gaia_live.json     se recibe via POST /push     desde ts_gaia_chart.py local
  gaia_ndx_live.json se recibe via POST /push_ndx desde ts_gaia_ndx.py local
"""

from flask import Flask, jsonify, request, send_from_directory, redirect, session, render_template_string
import json, os, time, logging
from functools import wraps

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

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info("=" * 60)
    log.info("  PivotAlphaDesk — GAIA DHP Server (Railway)")
    log.info("=" * 60)
    app.run(host='0.0.0.0', port=port, debug=False)
