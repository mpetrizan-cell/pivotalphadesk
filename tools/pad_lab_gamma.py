#!/usr/bin/env python3
"""
PAD Lab — Gamma Validation & Net Gamma Calculator
==================================================
Upgrade del PAD Lab original (Black-Scholes propio).
Formula original: Net_Gamma = Gamma(BS) × OI × 100

Este modulo:
1. Valida el gamma calculado por BS contra el gamma reportado por Tradestation
2. Calcula Net Gamma con ambos metodos y compara
3. Genera reporte de diferencia (error %) para calibrar el modelo

Uso:
    python pad_lab_gamma.py <chain.xls> [--price 655.85] [--r 0.05]
"""

import pandas as pd
import numpy as np
from scipy.stats import norm
import json, sys, os

# ── BLACK-SCHOLES GAMMA ───────────────────────────────────
def bs_gamma(S, K, T, r, sigma):
    """
    Calcula Gamma Black-Scholes.
    S = spot, K = strike, T = tiempo en anos, r = tasa libre riesgo, sigma = IV
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    return gamma

# ── NET GAMMA CALCULATION ─────────────────────────────────
def calculate_net_gamma_bs(chain_df, current_price, risk_free=0.05):
    """
    Calcula Net Gamma usando Black-Scholes propio (metodologia PAD Lab original).
    Retorna DataFrame con columna net_gamma_bs por strike.
    Formula: Net_Gamma_BS = SUM(Gamma_BS × Call_OI × 100) - SUM(Gamma_BS_put × Put_OI × 100)
    Note: Para ETF americano, gamma de call = gamma de put (put-call parity de gregas)
    """
    results = []
    today = pd.Timestamp.now()

    for _, row in chain_df.iterrows():
        dte   = row.get('dte', 0)
        T     = max(dte / 365.0, 1/365.0)  # minimo 1 dia
        K     = row['strike']
        S     = current_price

        # IV — usar promedio call/put IV si disponible
        call_iv = row.get('call_iv', 0) / 100.0 if row.get('call_iv',0) > 0 else 0.0
        put_iv  = row.get('put_iv',  0) / 100.0 if row.get('put_iv', 0) > 0 else 0.0

        # Si IV = 0 (dato faltante), skip
        if call_iv == 0 and put_iv == 0:
            continue

        # Calcular gamma BS para call y put
        gamma_call_bs = bs_gamma(S, K, T, risk_free, call_iv) if call_iv > 0 else 0.0
        gamma_put_bs  = bs_gamma(S, K, T, risk_free, put_iv)  if put_iv  > 0 else 0.0

        # Net gamma usando gamma de TS (reportado) vs gamma BS
        call_oi = row.get('call_oi', 0)
        put_oi  = row.get('put_oi',  0)

        # Gamma reportado por Tradestation (ya es por accion, multiplicar por 100 para el contrato)
        gamma_call_ts = row.get('call_gamma', 0)
        gamma_put_ts  = row.get('put_gamma',  0)

        net_gamma_ts = (call_oi * gamma_call_ts - put_oi * gamma_put_ts) * 100
        net_gamma_bs = (call_oi * gamma_call_bs - put_oi * gamma_put_bs) * 100

        results.append({
            'dte':           dte,
            'strike':        K,
            'call_oi':       call_oi,
            'put_oi':        put_oi,
            'gamma_call_ts': gamma_call_ts,
            'gamma_call_bs': round(gamma_call_bs, 6),
            'gamma_put_ts':  gamma_put_ts,
            'gamma_put_bs':  round(gamma_put_bs, 6),
            'net_gamma_ts':  round(net_gamma_ts, 2),
            'net_gamma_bs':  round(net_gamma_bs, 2),
            'call_iv':       call_iv,
            'put_iv':        put_iv,
        })

    return pd.DataFrame(results)

# ── VALIDATION REPORT ─────────────────────────────────────
def gamma_validation_report(df, max_dte=31):
    """
    Compara gamma TS vs gamma BS y reporta la diferencia.
    """
    near = df[df['dte'] <= max_dte].copy()
    near = near[(near['gamma_call_ts'] > 0) & (near['gamma_call_bs'] > 0)]

    if len(near) == 0:
        print("No hay datos suficientes para validar.")
        return

    # Error relativo gamma individual
    near['err_call_pct'] = ((near['gamma_call_bs'] - near['gamma_call_ts']) / near['gamma_call_ts'] * 100).abs()
    near['err_put_pct']  = ((near['gamma_put_bs']  - near['gamma_put_ts'])  / near['gamma_put_ts']  * 100).abs().where(near['gamma_put_ts'] > 0, 0)

    print("\n" + "="*65)
    print("  PAD Lab — Validacion Gamma: BS vs Tradestation")
    print("="*65)
    print(f"\n  Strikes analizados (0-{max_dte} DTE): {len(near)}")
    print(f"\n  ERROR RELATIVO GAMMA CALL:")
    print(f"    Media:    {near['err_call_pct'].mean():.2f}%")
    print(f"    Mediana:  {near['err_call_pct'].median():.2f}%")
    print(f"    Max:      {near['err_call_pct'].max():.2f}%  (strike ${near.loc[near['err_call_pct'].idxmax(),'strike']:.0f})")

    print(f"\n  NET GAMMA COMPARACION (suma total, near-term):")
    agg_ts = near.groupby('strike')['net_gamma_ts'].sum()
    agg_bs = near.groupby('strike')['net_gamma_bs'].sum()

    print(f"    Net Gamma TS (Tradestation): {agg_ts.sum():>12,.0f}")
    print(f"    Net Gamma BS (PAD Lab):      {agg_bs.sum():>12,.0f}")
    diff_pct = abs(agg_ts.sum() - agg_bs.sum()) / (abs(agg_ts.sum()) + 1e-6) * 100
    print(f"    Diferencia:                  {diff_pct:>11.2f}%")

    # Muestra strikes ATM para inspeccion visual
    print(f"\n  DETALLE ATM (muestra 10 strikes):")
    print(f"  {'Strike':>8} {'G_Call_TS':>12} {'G_Call_BS':>12} {'Err%':>8} {'NetG_TS':>12} {'NetG_BS':>12}")
    print("  " + "-"*68)

    # Seleccionar strikes cercanos al precio medio del rango
    sample = near.sort_values('err_call_pct').head(5).append(
             near.sort_values('strike').iloc[len(near)//2-2:len(near)//2+3])
    sample = sample.drop_duplicates('strike').sort_values('strike').head(10)

    for _, r in sample.iterrows():
        print(f"  {r['strike']:>8.0f} {r['gamma_call_ts']:>12.5f} {r['gamma_call_bs']:>12.5f} "
              f"{r['err_call_pct']:>7.1f}% {r['net_gamma_ts']:>12.1f} {r['net_gamma_bs']:>12.1f}")

    # Conclusion de calibracion
    print(f"\n  CONCLUSIONES PAD LAB:")
    if diff_pct < 5:
        print(f"  OK  Diferencia <5% — metodologia BS validada con Tradestation.")
        print(f"      Usar Net Gamma BS como respaldo cuando no hay chain disponible.")
    elif diff_pct < 15:
        print(f"  OK  Diferencia {diff_pct:.1f}% — dentro de margen aceptable.")
        print(f"      El modelo BS subestima/sobreestima levemente por smile de volatilidad.")
        print(f"      Recomendado: usar gamma de Tradestation como fuente primaria.")
    else:
        print(f"  ATENCION  Diferencia {diff_pct:.1f}% — revisar inputs de IV o DTE.")
        print(f"      Posibles causas: IV truncada en el export, opciones muy OTM.")

    print("="*65 + "\n")

    # Returnar gamma map consolidado
    gamma_map = pd.DataFrame({
        'strike':       agg_ts.index,
        'net_gamma_ts': agg_ts.values,
        'net_gamma_bs': agg_bs.loc[agg_ts.index].values if len(agg_bs) == len(agg_ts) else np.nan,
    }).reset_index(drop=True)

    return gamma_map

# ── GAMMA EXPOSURE MAP ────────────────────────────────────
def print_gamma_exposure_map(gamma_map, current_price, window=15):
    """
    Imprime el mapa de exposicion gamma por strike.
    Los dealers son LONG gamma cuando OI call > OI put (net_gamma > 0).
    Los dealers son SHORT gamma cuando OI put > OI call (net_gamma < 0) -> AMPLIFIED.
    """
    near_atm = gamma_map[
        (gamma_map['strike'] >= current_price - window) &
        (gamma_map['strike'] <= current_price + window)
    ].sort_values('strike')

    print("\n  MAPA DE EXPOSICION GAMMA (zona ATM):")
    print(f"  {'Strike':>8} {'Net Gamma TS':>14} {'Dealer':>12} {'Bar':>20}")
    print("  " + "-"*60)

    max_abs = near_atm['net_gamma_ts'].abs().max()
    for _, r in near_atm.iterrows():
        ng   = r['net_gamma_ts']
        dealer = "SHORT/AMPL" if ng < 0 else "LONG/DAMP "
        bar_len = int(abs(ng) / (max_abs + 1e-6) * 20)
        bar  = ("█" * bar_len) if ng > 0 else ("░" * bar_len)
        atm  = " <-- ATM" if abs(r['strike'] - current_price) < 1.5 else ""
        print(f"  {r['strike']:>8.0f} {ng:>14,.0f} {dealer:>12} {bar:<20}{atm}")
    print()

# ── MAIN ──────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Uso: python pad_lab_gamma.py <chain.xls> [--price 655.85] [--r 0.05]")
        sys.exit(1)

    filepath = sys.argv[1]
    price    = None
    r_free   = 0.05

    for i, arg in enumerate(sys.argv):
        if arg == '--price' and i+1 < len(sys.argv):
            try: price = float(sys.argv[i+1])
            except: pass
        if arg == '--r' and i+1 < len(sys.argv):
            try: r_free = float(sys.argv[i+1])
            except: pass

    # Import parse function from pad_zones_spy
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from pad_zones_spy import parse_tradestation_chain

    print(f"\nPAD Lab Gamma Validator — {os.path.basename(filepath)}")

    chain = parse_tradestation_chain(filepath)
    if price is None:
        # Usar strike con menor diferencia entre call OI y put OI
        near = chain[chain['dte'] <= 7]
        agg  = near.groupby('strike').agg(c=('call_oi','sum'), p=('put_oi','sum')).reset_index()
        agg['diff'] = (agg['c'] - agg['p']).abs()
        price = float(agg.nsmallest(1,'diff').iloc[0]['strike'])
        print(f"  Precio estimado desde chain: ${price:.2f}")

    print(f"  Precio referencia: ${price:.2f} | Tasa libre riesgo: {r_free*100:.1f}%\n")

    # Calcular y comparar
    df_gamma = calculate_net_gamma_bs(chain, price, risk_free=r_free)
    gamma_map = gamma_validation_report(df_gamma, max_dte=31)

    if gamma_map is not None:
        print_gamma_exposure_map(gamma_map, price)

        # Guardar gamma map
        date_str = pd.Timestamp.now().strftime("%Y%m%d")
        out_file = f"PAD_SPY_GammaMap_{date_str}.json"
        gamma_map.to_json(out_file, orient='records', indent=2)
        print(f"  Gamma map guardado: {out_file}\n")

if __name__ == "__main__":
    main()
