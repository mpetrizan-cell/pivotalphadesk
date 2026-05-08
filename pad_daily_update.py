#!/usr/bin/env python3
"""
PivotAlphaDesk — Daily Site Update Script
==========================================
Actualiza login.html e index.html con el briefing del día.

USO:
  python pad_daily_update.py

El script te pide los datos del día interactivamente
y modifica los dos archivos automáticamente.
Luego solo haces git add + commit + push.
"""

import re
import os
import sys
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
#  CONFIGURACION — ajusta estos paths si es necesario
# ─────────────────────────────────────────────
LOGIN_FILE = "login.html"
INDEX_FILE = "index.html"
BACKUP     = True   # crea .bak antes de modificar

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    if BACKUP:
        bak = path + ".bak"
        with open(bak, "w", encoding="utf-8") as f:
            f.write(read_file(path))
        print(f"  Backup: {bak}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Updated: {path}")

def fmt_date_label(date_str):
    """2026-05-08 → May 8, 2026"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%b %-d, %Y") if sys.platform != "win32" else dt.strftime("%b %d, %Y").replace(" 0", " ")

def fmt_briefing_filename(date_str):
    """2026-05-08 → briefing_08may2026.html"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return f"briefing_{dt.strftime('%d%b%Y').lower()}.html"

def input_with_default(prompt, default):
    val = input(f"{prompt} [{default}]: ").strip()
    return val if val else default

# ─────────────────────────────────────────────
#  GATHER INPUTS
# ─────────────────────────────────────────────
def gather_inputs():
    print()
    print("=" * 60)
    print("  PivotAlphaDesk — Daily Update")
    print("=" * 60)
    print()

    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # Skip weekends for yesterday
    if today.weekday() == 0:  # Monday
        yesterday = today - timedelta(days=3)

    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    print("── FECHAS ──────────────────────────────────────")
    today_input = input_with_default("Fecha de HOY (YYYY-MM-DD)", today_str)
    yesterday_input = input_with_default("Fecha de AYER (YYYY-MM-DD)", yesterday_str)

    print()
    print("── BRIEFING DEL DÍA ────────────────────────────")
    day_label = fmt_date_label(today_input)
    subtitle = input_with_default(
        f"Subtítulo del briefing (ej: NFP 115K Beat · DAMPENED)",
        "DAMPENED · Pre-Market"
    )
    description = input_with_default(
        "Descripción corta (para la card del index)",
        "Full PAD map ES, NQ, CL, SPX. DAMPENED regime."
    )
    previous_label = input_with_default(
        "Label del briefing anterior (ej: May 7 · ATH · ARM Earnings)",
        fmt_date_label(yesterday_input) + " · Previous"
    )

    print()
    print("── NIVELES DEL DÍA (ticker/marquee) ───────────")
    es_cw   = input_with_default("ES Call Wall", "7,500")
    es_gc2  = input_with_default("ES GC2 / Call Wall 2", "7,400")
    es_flip = input_with_default("ES Gamma Flip", "7,305")
    es_regime = input_with_default("ES Regime", "DAMPENED")

    nq_cw   = input_with_default("NQ Call Wall", "29,000")
    nq_pw   = input_with_default("NQ Put Wall", "28,500")
    nq_node = input_with_default("NQ Gamma Node", "28,700")

    cl_cw   = input_with_default("CL Call Wall", "$95")
    cl_pw   = input_with_default("CL Put Wall", "$90")
    cl_node = input_with_default("CL Gamma Node", "$93")
    cl_note = input_with_default("CL nota (ej: -3.0% NFP)", "DAMPENED")

    spx_flip = input_with_default("SPX Gamma Flip", "7,305")
    spx_node = input_with_default("SPX Node", "7,315")
    spx_cw   = input_with_default("SPX Call Wall", "7,500")
    spx_note = input_with_default("SPX nota (ej: NFP 115K Beat)", "DAMPENED")

    print()
    return {
        "today": today_input,
        "yesterday": yesterday_input,
        "day_label": day_label,
        "yesterday_label": fmt_date_label(yesterday_input),
        "today_file": fmt_briefing_filename(today_input),
        "yesterday_file": fmt_briefing_filename(yesterday_input),
        "subtitle": subtitle,
        "description": description,
        "previous_label": previous_label,
        "es_cw": es_cw, "es_gc2": es_gc2, "es_flip": es_flip, "es_regime": es_regime,
        "nq_cw": nq_cw, "nq_pw": nq_pw, "nq_node": nq_node,
        "cl_cw": cl_cw, "cl_pw": cl_pw, "cl_node": cl_node, "cl_note": cl_note,
        "spx_flip": spx_flip, "spx_node": spx_node, "spx_cw": spx_cw, "spx_note": spx_note,
    }

# ─────────────────────────────────────────────
#  UPDATE LOGIN.HTML
# ─────────────────────────────────────────────
def update_login(d):
    content = read_file(LOGIN_FILE)

    # Build new briefing list items
    new_items = f"""      <a class="access-item" href="{d['today_file']}">
        <div class="access-dot red"></div>
        <span class="access-label">Pre-Open Briefing · {d['day_label']}</span>
        <span class="access-date">LATEST</span>
      </a>
      <a class="access-item" href="{d['yesterday_file']}">
        <div class="access-dot"></div>
        <span class="access-label">Pre-Open Briefing · {d['yesterday_label']}</span>
        <span class="access-date">{d['yesterday_label'].upper()[:6]}</span>
      </a>"""

    # Replace the two briefing access-items
    pattern = r'(<a class="access-item" href="briefing_[^"]+">.*?</a>\s*<a class="access-item" href="briefing_[^"]+">.*?</a>)'
    new_content = re.sub(pattern, new_items, content, flags=re.DOTALL)

    if new_content == content:
        print("  ⚠️  login.html: no match found — check pattern manually")
    else:
        write_file(LOGIN_FILE, new_content)

# ─────────────────────────────────────────────
#  UPDATE INDEX.HTML
# ─────────────────────────────────────────────
def update_index(d):
    content = read_file(INDEX_FILE)

    # 1. Update briefing card badge + title + description
    # Find and replace the PRO badge date
    content = re.sub(
        r'(PRO — MAY \d+[^"]*)',
        f"PRO — {d['day_label'].upper().replace(',', '').split()[0]} {d['day_label'].split()[1].upper()} · LATEST",
        content
    )

    # Replace card title
    content = re.sub(
        r'(<h3[^>]*>)May \d+ ·[^<]*(</h3>)',
        rf'\g<1>{d["day_label"].split(",")[0]} · {d["subtitle"]}\g<2>',
        content
    )

    # Replace card description
    content = re.sub(
        r'(<p style="font-size:13px[^>]*>)[^<]*(</p>)',
        rf'\g<1>{d["description"]}\g<2>',
        content,
        count=1
    )

    # Replace Previous line
    content = re.sub(
        r'(Previous:[^<]*)',
        f'Previous: {d["previous_label"]}',
        content
    )

    # 2. Update ticker — rebuild all ticker items
    ticker_new = f"""    <div class="ticker-item"><span class="sym">ESM26</span> <span class="text3">|</span> <span>Call Wall</span> <span class="neu">{d['es_cw']}</span> <span class="text3">|</span> <span>GC2</span> <span class="neu">{d['es_gc2']}</span> <span class="text3">|</span> <span>Gamma Flip</span> <span class="neu">{d['es_flip']}</span> <span class="text3">|</span> <span>Regime</span> <span class="up">{d['es_regime']}</span></div>
    <div class="ticker-item"><span class="sym">NQM26</span> <span class="text3">|</span> <span>Call Wall</span> <span class="neu">{d['nq_cw']}</span> <span class="text3">|</span> <span>Put Wall</span> <span class="dn">{d['nq_pw']}</span> <span class="text3">|</span> <span>Gamma Node</span> <span class="neu">{d['nq_node']}</span> <span class="text3">|</span> <span>Regime</span> <span class="up">{d['es_regime']}</span></div>
    <div class="ticker-item"><span class="sym">CLM26</span> <span class="text3">|</span> <span>Call Wall</span> <span class="up">{d['cl_cw']}</span> <span class="text3">|</span> <span>Put Wall</span> <span class="dn">{d['cl_pw']}</span> <span class="text3">|</span> <span>Gamma Node</span> <span class="neu">{d['cl_node']}</span> <span class="text3">|</span> <span class="dn">{d['cl_note']}</span></div>
    <div class="ticker-item"><span class="sym">SPX</span> <span class="text3">|</span> <span>Gamma Flip</span> <span class="up">{d['spx_flip']}</span> <span class="text3">|</span> <span>Node</span> <span class="neu">{d['spx_node']}</span> <span class="text3">|</span> <span>Call Wall</span> <span class="neu">{d['spx_cw']}</span> <span class="text3">|</span> <span class="up">{d['spx_note']}</span></div>
    <div class="ticker-item"><span class="sym">GAIA DHP</span> <span class="up">LIVE</span> <span class="text3">|</span> <span>Dealer flow monitoring active</span> <span class="text3">|</span> <span class="up">app.pivotalphadesk.com</span></div>
    <div class="ticker-item"><span class="sym">ESM26</span> <span class="text3">|</span> <span>Call Wall</span> <span class="neu">{d['es_cw']}</span> <span class="text3">|</span> <span>GC2</span> <span class="neu">{d['es_gc2']}</span> <span class="text3">|</span> <span>Gamma Flip</span> <span class="neu">{d['es_flip']}</span> <span class="text3">|</span> <span>Regime</span> <span class="up">{d['es_regime']}</span></div>
    <div class="ticker-item"><span class="sym">NQM26</span> <span class="text3">|</span> <span>Call Wall</span> <span class="neu">{d['nq_cw']}</span> <span class="text3">|</span> <span>Put Wall</span> <span class="dn">{d['nq_pw']}</span> <span class="text3">|</span> <span>Gamma Node</span> <span class="neu">{d['nq_node']}</span> <span class="text3">|</span> <span>Regime</span> <span class="up">{d['es_regime']}</span></div>
    <div class="ticker-item"><span class="sym">CLM26</span> <span class="text3">|</span> <span>Call Wall</span> <span class="up">{d['cl_cw']}</span> <span class="text3">|</span> <span>Put Wall</span> <span class="dn">{d['cl_pw']}</span> <span class="text3">|</span> <span>Gamma Node</span> <span class="neu">{d['cl_node']}</span> <span class="text3">|</span> <span class="dn">{d['cl_note']}</span></div>
    <div class="ticker-item"><span class="sym">SPX</span> <span class="text3">|</span> <span>Gamma Flip</span> <span class="up">{d['spx_flip']}</span> <span class="text3">|</span> <span>Node</span> <span class="neu">{d['spx_node']}</span> <span class="text3">|</span> <span>Call Wall</span> <span class="neu">{d['spx_cw']}</span> <span class="text3">|</span> <span class="up">{d['spx_note']}</span></div>
    <div class="ticker-item"><span class="sym">GAIA DHP</span> <span class="up">LIVE</span> <span class="text3">|</span> <span>Dealer flow monitoring active</span> <span class="text3">|</span> <span class="up">app.pivotalphadesk.com</span></div>"""

    # Replace ticker block
    pattern = r'(<div class="ticker-item">.*?</div>\s*){5,}'
    new_content = re.sub(pattern, ticker_new + "\n", content, flags=re.DOTALL)

    if new_content == content:
        print("  ⚠️  index.html ticker: no match — check pattern manually")
    else:
        write_file(INDEX_FILE, new_content)

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    # Check files exist
    for f in [LOGIN_FILE, INDEX_FILE]:
        if not os.path.exists(f):
            print(f"\n  ERROR: {f} not found in current directory.")
            print(f"  Run this script from your GitHub repo folder.\n")
            sys.exit(1)

    d = gather_inputs()

    print("── ACTUALIZANDO ARCHIVOS ───────────────────────")
    update_login(d)
    update_index(d)

    print()
    print("=" * 60)
    print("  LISTO. Ahora ejecuta:")
    print()
    print(f"  git add login.html index.html")
    print(f"  git commit -m 'Daily update {d['today']} — {d['subtitle']}'")
    print(f"  git push")
    print()
    print("  GitHub Pages se actualiza en ~30 segundos.")
    print("=" * 60)
    print()

if __name__ == "__main__":
    main()
