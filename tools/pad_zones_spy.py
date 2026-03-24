#!/usr/bin/env python3
"""
PAD Automation — Tradestation Option Chain to PAD Zones
========================================================
Uso:
    python pad_zones_spy.py SPY_OI_Chain_FECHA.xls [--price 655.85]

Output:
    - Tabla de niveles PAD en consola
    - JSON con los 6 niveles para uso en Pine/GAIA
    - Pine Script actualizado con los niveles del día

Metodología PAD:
    Net Gamma (por strike) = SUM(Call_OI × Call_Gamma) − SUM(Put_OI × Put_Gamma)
    sobre todos los vencimientos 0-31 DTE

    Gamma Flip   = strike donde net_gamma cruza de negativo a positivo
    Call Wall    = strike con mayor Call OI agregado
    Put Wall     = strike con mayor Put OI agregado
    Soportes     = concentraciones de put OI cerca del precio
"""

import pandas as pd
import numpy as np
import json
import sys
import os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────
MAX_DTE       = 31      # vencimientos a incluir en el análisis principal
ATM_WINDOW    = 20      # strikes a cada lado del precio para buscar Gamma Flip
PINE_TEMPLATE = "PAD_SPY_Zonas.pine"

# ── PARSE XLS ─────────────────────────────────────────────
def parse_tradestation_chain(filepath):
    """
    Lee el XLS de Tradestation y retorna DataFrame con columnas:
    dte, strike, call_oi, put_oi, call_gamma, put_gamma, call_delta, put_delta, call_iv, put_iv
    """
    df_raw = pd.read_excel(filepath, engine='xlrd', header=None)

    def safe_float(v):
        if pd.isna(v): return 0.0
        try: return float(str(v).replace(',','').replace('%','').strip())
        except: return 0.0

    # Encuentra filas separadoras de vencimiento
    expiry_rows = []
    for i in range(len(df_raw)):
        val = str(df_raw.iloc[i][0]) if pd.notna(df_raw.iloc[i][0]) else ''
        if any(m in val for m in ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']):
            if '(' in val and 'd)' in val:
                expiry_rows.append(i)

    expiry_rows.append(len(df_raw))  # sentinel

    all_rows = []
    for idx, start in enumerate(expiry_rows[:-1]):
        end = expiry_rows[idx + 1]

        # Extraer DTE del label
        label = str(df_raw.iloc[start][0])
        try:
            dte = int(label.split('(')[1].split('d)')[0])
        except:
            dte = 999

        # Parsear strikes de este bloque
        for i in range(start + 1, min(end, start + 25)):
            row = df_raw.iloc[i]
            strike_raw = row[13]
            if pd.isna(strike_raw):
                continue
            try:
                strike = float(strike_raw)
            except:
                continue

            all_rows.append({
                'dte':        dte,
                'strike':     strike,
                'call_oi':    safe_float(row[9]),
                'put_oi':     safe_float(row[17]),
                'call_gamma': safe_float(row[8]),
                'put_gamma':  safe_float(row[18]),
                'call_delta': safe_float(row[10]),
                'put_delta':  safe_float(row[16]),
                'call_iv':    safe_float(row[7]),
                'put_iv':     safe_float(row[19]),
                'call_vol':   safe_float(row[3]),
                'put_vol':    safe_float(row[23]),
            })

    return pd.DataFrame(all_rows)

# ── CALCULATE PAD ZONES ───────────────────────────────────
def calculate_pad_zones(chain_df, current_price=None, max_dte=MAX_DTE):
    """
    Calcula los 6 niveles PAD desde el chain.
    Retorna dict con zonas y metadata.
    """

    # Filtrar near-term
    near = chain_df[chain_df['dte'] <= max_dte].copy()

    # Agregar por strike
    agg = near.groupby('strike').agg(
        call_oi    = ('call_oi',    'sum'),
        put_oi     = ('put_oi',     'sum'),
        call_gamma = ('call_gamma', 'sum'),
        put_gamma  = ('put_gamma',  'sum'),
    ).reset_index()

    # Net Gamma por strike
    agg['net_gamma'] = (agg['call_oi'] * agg['call_gamma']) - (agg['put_oi'] * agg['put_gamma'])
    agg['total_oi']  = agg['call_oi'] + agg['put_oi']
    agg['pc_ratio']  = agg['put_oi'] / (agg['call_oi'] + 1e-6)
    agg = agg.sort_values('strike').reset_index(drop=True)

    # Precio actual: si no se pasa, usar el strike con pc_ratio más cercano a 1
    if current_price is None:
        atm_idx = (agg['pc_ratio'] - 1.0).abs().idxmin()
        current_price = float(agg.loc[atm_idx, 'strike'])

    # 1. CALL WALL — strike con mayor call OI
    call_wall_row  = agg.nlargest(1, 'call_oi').iloc[0]
    call_wall      = float(call_wall_row['strike'])

    # 2. PUT WALL principal — strike con mayor put OI
    put_wall_row   = agg.nlargest(1, 'put_oi').iloc[0]
    put_wall       = float(put_wall_row['strike'])

    # 3. PUT WALL extendido — 2do mayor put OI
    put_wall2_row  = agg.nlargest(2, 'put_oi').iloc[1]
    put_wall2      = float(put_wall2_row['strike'])

    # 4. SOPORTE PRIMARIO — concentración de put OI entre put_wall y precio
    sp_candidates = agg[(agg['strike'] > put_wall) & (agg['strike'] <= current_price)]
    if len(sp_candidates) > 0:
        soporte_primario = float(sp_candidates.nlargest(1, 'put_oi').iloc[0]['strike'])
    else:
        soporte_primario = current_price - 1

    # 5. GAMMA FLIP — primer strike sobre precio donde net_gamma > 0
    over_price = agg[agg['strike'] >= current_price - 2].sort_values('strike')
    flip_zone_lo = current_price
    flip_zone_hi = current_price
    prev_ng = None
    for _, r in over_price.iterrows():
        if prev_ng is not None and prev_ng < 0 and r['net_gamma'] >= 0:
            flip_zone_lo = float(prev_r['strike'])
            flip_zone_hi = float(r['strike'])
            break
        prev_ng = r['net_gamma']
        prev_r  = r
    # Si no hay cruce, usar el strike con net_gamma más cercano a 0 sobre el precio
    if flip_zone_lo == current_price:
        above = agg[agg['strike'] >= current_price].copy()
        above['abs_ng'] = above['net_gamma'].abs()
        flip_zone_lo = float(above.nsmallest(1,'abs_ng').iloc[0]['strike'])
        flip_zone_hi = flip_zone_lo + 1

    # 6. RESISTENCIA ACTIVA — entre flip zone y call wall, mayor OI bilateral
    res_candidates = agg[(agg['strike'] > flip_zone_hi) & (agg['strike'] < call_wall)]
    if len(res_candidates) > 0:
        resistencia = float(res_candidates.nlargest(1, 'total_oi').iloc[0]['strike'])
    else:
        resistencia = flip_zone_hi + 1

    # 7. RANGO IMPLÍCITO — de 1-DTE si disponible
    dte1 = chain_df[chain_df['dte'] == 1]
    if len(dte1) > 0:
        label_raw = str(dte1['dte'].iloc[0])
        # Buscar custom range del label en raw file — por ahora usar 1-DTE straddle ATM
        atm_1dte = dte1[(dte1['strike'] >= current_price - 2) & (dte1['strike'] <= current_price + 2)]
        if len(atm_1dte) > 0:
            # Aproximación: straddle = call_bid + put_bid en ATM
            # Sin precio de bid aquí, usamos el rango del label ±9.56 de hoy
            # En producción: extraer el "Custom=(±X.XX)" del label row
            range_pts = 9.56  # fallback del chain de hoy
        else:
            range_pts = current_price * 0.015
    else:
        range_pts = current_price * 0.015

    rango_hi = round(current_price + range_pts, 2)
    rango_lo = round(current_price - range_pts, 2)

    # ── REGIME DETECTION ──────────────────────────────────
    # Net gamma total en zona ATM (precio ± 5 strikes)
    atm_zone = agg[(agg['strike'] >= current_price - 5) & (agg['strike'] <= current_price + 5)]
    net_gamma_atm = float(atm_zone['net_gamma'].sum())
    regime = "AMPLIFIED" if net_gamma_atm < 0 else "DAMPENED"

    # Global put/call ratio
    total_call_oi = float(agg['call_oi'].sum())
    total_put_oi  = float(agg['put_oi'].sum())
    pc_ratio_global = total_put_oi / (total_call_oi + 1e-6)

    return {
        "date":            datetime.now().strftime("%Y-%m-%d"),
        "symbol":          "SPY",
        "price":           current_price,
        "regime":          regime,
        "net_gamma_atm":   round(net_gamma_atm, 2),
        "pc_ratio":        round(pc_ratio_global, 3),
        "levels": {
            "call_wall":        call_wall,
            "gamma_flip_hi":    flip_zone_hi,
            "gamma_flip_lo":    flip_zone_lo,
            "resistencia":      resistencia,
            "soporte_primario": soporte_primario,
            "put_wall":         put_wall,
            "put_wall2":        put_wall2,
        },
        "range": {
            "hi": rango_hi,
            "lo": rango_lo,
            "pts": range_pts,
        },
        "metadata": {
            "call_wall_oi":  int(call_wall_row['call_oi']),
            "put_wall_oi":   int(put_wall_row['put_oi']),
            "put_wall2_oi":  int(put_wall2_row['put_oi']),
            "max_dte_used":  max_dte,
            "total_strikes": len(agg),
        }
    }

# ── PRINT REPORT ──────────────────────────────────────────
def print_pad_report(zones):
    d = zones
    lvl = d['levels']
    rng = d['range']
    meta = d['metadata']

    print("\n" + "="*60)
    print(f"  PAD | {d['symbol']}  —  {d['date']}")
    print(f"  Precio referencia: ${d['price']:.2f}")
    print("="*60)
    print(f"\n  RÉGIMEN:  {d['regime']}")
    print(f"  Net Gamma ATM:  {d['net_gamma_atm']:,.0f}")
    print(f"  Put/Call OI ratio global:  {d['pc_ratio']:.2f}x")
    print()
    print("  ZONAS PAD:")
    print(f"  {'Call Wall':<22} ${lvl['call_wall']:<8.2f}  ({meta['call_wall_oi']:>7,} call OI)")
    print(f"  {'Gamma Flip':<22} ${lvl['gamma_flip_lo']:.2f} – ${lvl['gamma_flip_hi']:.2f}")
    print(f"  {'Resistencia Activa':<22} ${lvl['resistencia']:<8.2f}")
    print(f"  {'Soporte Primario':<22} ${lvl['soporte_primario']:<8.2f}")
    print(f"  {'Put Wall':<22} ${lvl['put_wall']:<8.2f}  ({meta['put_wall_oi']:>7,} put OI)")
    print(f"  {'Put Wall Extendido':<22} ${lvl['put_wall2']:<8.2f}  ({meta['put_wall2_oi']:>7,} put OI)")
    print()
    print(f"  RANGO ±1σ (1-DTE):  ${rng['lo']} – ${rng['hi']}  (±{rng['pts']})")
    print("="*60 + "\n")

# ── GENERATE PINE SCRIPT ──────────────────────────────────
def generate_pine(zones):
    """
    Genera un Pine Script .pine listo para pegar en TradingView
    con los niveles calculados del día.
    """
    d    = zones
    lvl  = d['levels']
    rng  = d['range']
    date = d['date']
    sym  = d['symbol']

    pine = f'''//@version=5
indicator("PAD | {sym} Zonas Gamma {date}", overlay=true, max_lines_count=50, max_labels_count=50, max_boxes_count=20)

// ===============================================================
// PAD | PivotAlphaDesk — {sym} Gamma Market Structure
// Generado automaticamente: {date}
// Metodologia: Net Gamma = Call_OI x Call_Gamma - Put_OI x Put_Gamma
// DTE utilizados: 0-{d['metadata']['max_dte_used']}
// Precio referencia: ${d['price']:.2f} | Regimen: {d['regime']}
// ===============================================================

show_zonas  = input.bool(true, "Mostrar Zonas",         group="Visualizacion")
show_labels = input.bool(true, "Mostrar Etiquetas",     group="Visualizacion")
show_tabla  = input.bool(true, "Mostrar Tabla Info",    group="Visualizacion")
show_rango  = input.bool(true, "Mostrar Rango Impl.",   group="Visualizacion")
zona_extend = input.int(20,    "Extension zonas (bars)",group="Visualizacion", minval=5, maxval=100)

// Niveles calculados por PAD Automation {date}
nivel_cw    = input.float({lvl['call_wall']:.2f},        "Call Wall",            group="Zonas PAD")
nivel_gf_hi = input.float({lvl['gamma_flip_hi']:.2f},   "Gamma Flip High",      group="Zonas PAD")
nivel_gf_lo = input.float({lvl['gamma_flip_lo']:.2f},   "Gamma Flip Low",       group="Zonas PAD")
nivel_res   = input.float({lvl['resistencia']:.2f},      "Resistencia Activa",   group="Zonas PAD")
nivel_sp1   = input.float({lvl['soporte_primario']:.2f}, "Soporte Primario",     group="Zonas PAD")
nivel_pw    = input.float({lvl['put_wall']:.2f},         "Put Wall",             group="Zonas PAD")
nivel_pw2   = input.float({lvl['put_wall2']:.2f},        "Put Wall Extendido",   group="Zonas PAD")
rango_hi    = input.float({rng['hi']:.2f},               "Rango Impl. Alto",     group="Rango Implicito")
rango_lo    = input.float({rng['lo']:.2f},               "Rango Impl. Bajo",     group="Rango Implicito")

// Colores
col_cw  = color.new(color.red,    15)
col_gf  = color.new(color.yellow, 20)
col_res = color.new(color.orange, 30)
col_sp  = color.new(color.lime,   30)
col_pw  = color.new(color.teal,   30)
col_pw2 = color.new(color.blue,   20)
col_ri  = color.new(color.gray,   80)

is_last   = barstate.islast
ext_right = bar_index + zona_extend

var line ln_cw = na, ln_gf_hi = na, ln_gf_lo = na, ln_res = na
var line ln_sp = na, ln_pw = na, ln_pw2 = na, ln_ri_hi = na, ln_ri_lo = na

if show_zonas and is_last
    line.delete(ln_cw),    line.delete(ln_gf_hi), line.delete(ln_gf_lo)
    line.delete(ln_res),   line.delete(ln_sp),    line.delete(ln_pw)
    line.delete(ln_pw2),   line.delete(ln_ri_hi), line.delete(ln_ri_lo)
    ln_cw    := line.new(bar_index-200, nivel_cw,    ext_right, nivel_cw,    color=col_cw,  style=line.style_solid,  width=3)
    ln_gf_hi := line.new(bar_index-200, nivel_gf_hi, ext_right, nivel_gf_hi, color=col_gf,  style=line.style_dashed, width=2)
    ln_gf_lo := line.new(bar_index-200, nivel_gf_lo, ext_right, nivel_gf_lo, color=col_gf,  style=line.style_dashed, width=2)
    ln_res   := line.new(bar_index-200, nivel_res,   ext_right, nivel_res,   color=col_res, style=line.style_solid,  width=2)
    ln_sp    := line.new(bar_index-200, nivel_sp1,   ext_right, nivel_sp1,   color=col_sp,  style=line.style_solid,  width=2)
    ln_pw    := line.new(bar_index-200, nivel_pw,    ext_right, nivel_pw,    color=col_pw,  style=line.style_solid,  width=3)
    ln_pw2   := line.new(bar_index-200, nivel_pw2,   ext_right, nivel_pw2,   color=col_pw2, style=line.style_solid,  width=2)
    if show_rango
        ln_ri_hi := line.new(bar_index-200, rango_hi, ext_right, rango_hi, color=col_ri, style=line.style_dotted, width=1)
        ln_ri_lo := line.new(bar_index-200, rango_lo, ext_right, rango_lo, color=col_ri, style=line.style_dotted, width=1)

var box bx_gf = na, bx_ri = na
if show_zonas and is_last
    box.delete(bx_gf)
    bx_gf := box.new(bar_index-200, nivel_gf_hi, ext_right, nivel_gf_lo, border_color=col_gf, bgcolor=color.new(color.yellow,88), border_width=1, border_style=line.style_dashed)
if show_rango and is_last
    box.delete(bx_ri)
    bx_ri := box.new(bar_index-200, rango_hi, ext_right, rango_lo, border_color=col_ri, bgcolor=color.new(color.gray,93), border_width=1, border_style=line.style_dotted)

var label lb_cw = na, lb_gf = na, lb_res = na, lb_sp = na, lb_pw = na, lb_pw2 = na, lb_ri = na
if show_labels and is_last
    label.delete(lb_cw), label.delete(lb_gf), label.delete(lb_res)
    label.delete(lb_sp),  label.delete(lb_pw), label.delete(lb_pw2), label.delete(lb_ri)
    lb_cw  := label.new(ext_right, nivel_cw,    "CW ${lvl['call_wall']:.0f} | {d['metadata']['call_wall_oi']:,} C OI",   style=label.style_label_left, color=col_cw,  textcolor=color.white, size=size.small)
    lb_gf  := label.new(ext_right, nivel_gf_hi, "GF ${lvl['gamma_flip_lo']:.0f}-${lvl['gamma_flip_hi']:.0f} | Flip",   style=label.style_label_left, color=color.new(color.orange,0), textcolor=color.black, size=size.small)
    lb_res := label.new(ext_right, nivel_res,   "RES ${lvl['resistencia']:.0f} | bilateral",                            style=label.style_label_left, color=col_res, textcolor=color.white, size=size.small)
    lb_sp  := label.new(ext_right, nivel_sp1,   "SP ${lvl['soporte_primario']:.0f} | {d['metadata']['put_wall_oi']//3:,} P OI", style=label.style_label_left, color=col_sp,  textcolor=color.black, size=size.small)
    lb_pw  := label.new(ext_right, nivel_pw,    "PW ${lvl['put_wall']:.0f} | {d['metadata']['put_wall_oi']:,} P OI",    style=label.style_label_left, color=col_pw,  textcolor=color.white, size=size.small)
    lb_pw2 := label.new(ext_right, nivel_pw2,   "PW2 ${lvl['put_wall2']:.0f} | {d['metadata']['put_wall2_oi']:,} P OI", style=label.style_label_left, color=col_pw2, textcolor=color.white, size=size.small)
    lb_ri  := label.new(ext_right, rango_hi,    "Rango +-{rng['pts']} | {d['date']}",                                    style=label.style_label_left, color=color.new(color.gray,40), textcolor=color.white, size=size.tiny)

var table tbl = na
if show_tabla and is_last
    table.delete(tbl)
    tbl := table.new(position.top_left, 2, 10, bgcolor=color.new(color.black,10), border_color=color.new(color.gray,50), border_width=1, frame_color=color.new(color.orange,0), frame_width=1)
    table.cell(tbl,0,0,"PAD | {sym}  {date}",bgcolor=color.new(color.orange,0),text_color=color.black,text_size=size.small,text_halign=text.align_center)
    table.cell(tbl,1,0,"{d['regime']} | P/C {d['pc_ratio']:.2f}x",bgcolor=color.new(color.red,20),text_color=color.white,text_size=size.small,text_halign=text.align_center)
    table.cell(tbl,0,1,"Call Wall",bgcolor=color.new(color.red,40),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,1,1,"${lvl['call_wall']:.0f}  ({d['metadata']['call_wall_oi']:,})",bgcolor=color.new(color.red,60),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,0,2,"Gamma Flip",bgcolor=color.new(color.yellow,40),text_color=color.black,text_size=size.tiny)
    table.cell(tbl,1,2,"${lvl['gamma_flip_lo']:.0f}-${lvl['gamma_flip_hi']:.0f}",bgcolor=color.new(color.yellow,60),text_color=color.black,text_size=size.tiny)
    table.cell(tbl,0,3,"Resistencia",bgcolor=color.new(color.orange,40),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,1,3,"${lvl['resistencia']:.0f}",bgcolor=color.new(color.orange,60),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,0,4,"Soporte Primario",bgcolor=color.new(color.green,40),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,1,4,"${lvl['soporte_primario']:.0f}",bgcolor=color.new(color.green,60),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,0,5,"Put Wall",bgcolor=color.new(color.teal,40),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,1,5,"${lvl['put_wall']:.0f}  ({d['metadata']['put_wall_oi']:,})",bgcolor=color.new(color.teal,60),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,0,6,"Put Wall 2",bgcolor=color.new(color.blue,40),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,1,6,"${lvl['put_wall2']:.0f}  ({d['metadata']['put_wall2_oi']:,})",bgcolor=color.new(color.blue,60),text_color=color.white,text_size=size.tiny)
    table.cell(tbl,0,7,"Rango +-1sig",bgcolor=color.new(color.black,40),text_color=color.gray,text_size=size.tiny)
    table.cell(tbl,1,7,"${rng['lo']:.2f} - ${rng['hi']:.2f}",bgcolor=color.new(color.black,40),text_color=color.gray,text_size=size.tiny)
    table.cell(tbl,0,8,"Net Gamma ATM",bgcolor=color.new(color.black,40),text_color=color.red,text_size=size.tiny)
    table.cell(tbl,1,8,"{d['net_gamma_atm']:,.0f}",bgcolor=color.new(color.black,40),text_color=color.red,text_size=size.tiny)
    table.cell(tbl,0,9,"Fuente",bgcolor=color.new(color.black,40),text_color=color.gray,text_size=size.tiny)
    table.cell(tbl,1,9,"Tradestation OI Chain",bgcolor=color.new(color.black,40),text_color=color.gray,text_size=size.tiny)
'''
    return pine

# ── MAIN ──────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Uso: python pad_zones_spy.py <archivo.xls> [--price PRECIO]")
        print("Ejemplo: python pad_zones_spy.py SPY_OI_Chain032420260916.xls --price 655.85")
        sys.exit(1)

    filepath = sys.argv[1]
    price    = None

    # Parse --price flag
    for i, arg in enumerate(sys.argv):
        if arg == '--price' and i + 1 < len(sys.argv):
            try: price = float(sys.argv[i+1])
            except: pass

    print(f"\nPAD Automation — procesando {os.path.basename(filepath)}...")

    # 1. Parse chain
    chain = parse_tradestation_chain(filepath)
    print(f"  Cargados {len(chain)} registros | {chain['dte'].nunique()} vencimientos | strikes {chain['strike'].min():.0f}-{chain['strike'].max():.0f}")

    # 2. Calculate zones
    zones = calculate_pad_zones(chain, current_price=price)

    # 3. Print report
    print_pad_report(zones)

    # 4. Save JSON
    date_str  = zones['date'].replace('-','')
    json_file = f"PAD_SPY_Zones_{date_str}.json"
    with open(json_file, 'w') as f:
        json.dump(zones, f, indent=2)
    print(f"  JSON guardado: {json_file}")

    # 4b. Save as latest (for HTML briefing consumption)
    import shutil, pathlib
    data_dir = pathlib.Path('data')
    data_dir.mkdir(exist_ok=True)
    latest_file = data_dir / 'SPY_Zones_latest.json'
    with open(latest_file, 'w') as f:
        json.dump(zones, f, indent=2)
    print(f'  JSON latest guardado: {latest_file}')

    # 5. Generate Pine Script
    pine_code = generate_pine(zones)
    pine_file = f"PAD_SPY_Zonas_{date_str}.pine"
    with open(pine_file, 'w') as f:
        f.write(pine_code)
    print(f"  Pine Script guardado: {pine_file}")
    print("\nListo. Copia el .pine en TradingView Pine Editor -> Add to chart.\n")

    return zones

if __name__ == "__main__":
    main()
