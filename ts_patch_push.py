# ── AGREGAR ESTO A ts_gaia_chart.py ───────────────────────────────────────────
# Al final de la función que escribe gaia_live.json, agrega este bloque:

import requests as _requests

RAILWAY_URL  = 'https://TU-APP.railway.app'   # <-- cambiar por tu URL Railway
PUSH_TOKEN   = 'gaia_push_secret_2026'         # <-- debe coincidir con Railway env var

def push_to_railway(data: dict):
    """Empuja gaia_live.json a Railway vía HTTP POST."""
    try:
        r = _requests.post(
            f'{RAILWAY_URL}/push',
            json=data,
            headers={'X-Push-Token': PUSH_TOKEN},
            timeout=3
        )
        if r.status_code == 200:
            pass  # ok silencioso
        else:
            print(f"[RAILWAY] Push error: {r.status_code}")
    except Exception as e:
        print(f"[RAILWAY] Push failed: {e}")

# En tu loop principal, después de escribir gaia_live.json:
#
#   with open('gaia_live.json', 'w') as f:
#       json.dump(data, f)
#   push_to_railway(data)   # <-- agrega esta línea
#
# ──────────────────────────────────────────────────────────────────────────────
