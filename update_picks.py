"""
PAD Pick Performance Tracker
=============================
Genera picks_data.json con el rendimiento de cada pick activo.
Corre automaticamente via GitHub Actions cada dia at 4:30 PM ET.

Uso local:
    pip install yfinance
    python update_picks.py

Output: picks_data.json  (en la misma carpeta, commitear al repo)
"""

import json
import yfinance as yf
from datetime import datetime, date
from zoneinfo import ZoneInfo

# ─── REGISTRO DE PICKS ───────────────────────────────────────────────────────
# Agregar un nuevo pick aqui cuando se publique.
# "ref_price": precio de referencia publicado en el pick (NO el precio de entrada).
# "entry_low" / "entry_high": zona de entrada.
# "benchmark": ETF de referencia para comparar rendimiento.

PICKS = [
    {
        "week":        12,
        "year":        2026,
        "ticker":      "XLE",
        "name":        "Energy Select Sector SPDR",
        "type":        "ETF Sectorial",
        "horizon":     "Posicional 2-4 semanas",
        "pub_date":    "2026-03-14",          # sabado de publicacion
        "ref_price":   57.83,                  # precio referencia del pick
        "entry_low":   54.00,
        "entry_high":  56.00,
        "target1":     63.00,
        "target2":     70.00,
        "stop":        51.00,
        "rr":          "1:2.5",
        "benchmark":   "SPY",
        "gamma_flip":  54.50,
        "put_wall":    55.00,
        "filename":    "pad_pick_semana12_2026.html",
        "status":      "open",                 # "open" | "closed_target1" | "closed_target2" | "closed_stop"
        "closed_date": None,
        "closed_price": None,
    },
    # ── TEMPLATE para nuevo pick ──────────────────────────────────────────
    # {
    #     "week":        13,
    #     "year":        2026,
    #     "ticker":      "TICKER",
    #     "name":        "Nombre del instrumento",
    #     "type":        "Small Cap | ETF Sectorial | ETF Indice | ADR",
    #     "horizon":     "Swing 2-10 dias | Posicional 2-6 semanas",
    #     "pub_date":    "2026-03-21",
    #     "ref_price":   0.00,
    #     "entry_low":   0.00,
    #     "entry_high":  0.00,
    #     "target1":     0.00,
    #     "target2":     0.00,
    #     "stop":        0.00,
    #     "rr":          "1:X",
    #     "benchmark":   "SPY",
    #     "gamma_flip":  0.00,
    #     "put_wall":    0.00,
    #     "filename":    "pad_pick_semanaNN_2026.html",
    #     "status":      "open",
    #     "closed_date": None,
    #     "closed_price": None,
    # },
]

# ─── LOGICA ──────────────────────────────────────────────────────────────────

ET = ZoneInfo("America/New_York")

def get_price_history(ticker: str, start: str) -> list[dict]:
    """Retorna lista de {date, close} desde start hasta hoy."""
    t = yf.Ticker(ticker)
    hist = t.history(start=start, auto_adjust=True)
    result = []
    for idx, row in hist.iterrows():
        result.append({
            "date":  idx.strftime("%Y-%m-%d"),
            "close": round(float(row["Close"]), 2)
        })
    return result


def compute_returns(prices: list[dict], ref_price: float) -> list[dict]:
    """Calcula retorno % acumulado desde ref_price."""
    return [
        {
            "date":   p["date"],
            "close":  p["close"],
            "ret_pct": round((p["close"] - ref_price) / ref_price * 100, 2)
        }
        for p in prices
    ]


def level_status(current: float, pick: dict) -> dict:
    """Determina el estado de cada nivel PAD."""
    def pct_away(target):
        return round((target - current) / current * 100, 2)

    return {
        "entry": {
            "hit": pick["entry_low"] <= current <= pick["entry_high"],
            "above": current > pick["entry_high"],
            "label": f"${pick['entry_low']}–${pick['entry_high']}"
        },
        "target1": {
            "hit": current >= pick["target1"],
            "pct_away": pct_away(pick["target1"]),
            "label": f"${pick['target1']}"
        },
        "target2": {
            "hit": current >= pick["target2"],
            "pct_away": pct_away(pick["target2"]),
            "label": f"${pick['target2']}"
        },
        "stop": {
            "hit": current <= pick["stop"],
            "margin": round(current - pick["stop"], 2),
            "label": f"${pick['stop']}"
        },
        "gamma_flip": {
            "above": current > pick["gamma_flip"],
            "regime": "DAMPENED" if current > pick["gamma_flip"] else "AMPLIFIED",
            "label": f"${pick['gamma_flip']}"
        },
        "put_wall": {
            "above": current > pick["put_wall"],
            "label": f"${pick['put_wall']}"
        }
    }


def process_pick(pick: dict) -> dict:
    """Procesa un pick y retorna su data completa para el JSON."""
    ticker_hist  = get_price_history(pick["ticker"],    pick["pub_date"])
    bench_hist   = get_price_history(pick["benchmark"], pick["pub_date"])

    if not ticker_hist:
        print(f"  WARNING: no hay datos para {pick['ticker']} desde {pick['pub_date']}")
        return None

    # Precio actual (ultimo cierre disponible)
    current_price = ticker_hist[-1]["close"]
    current_date  = ticker_hist[-1]["date"]

    # Retornos acumulados
    ticker_returns = compute_returns(ticker_hist, pick["ref_price"])
    bench_ref      = bench_hist[0]["close"] if bench_hist else 1
    bench_returns  = compute_returns(bench_hist, bench_ref)

    # Retorno actual
    current_ret     = ticker_returns[-1]["ret_pct"] if ticker_returns else 0
    bench_ret       = bench_returns[-1]["ret_pct"]  if bench_returns  else 0
    vs_benchmark    = round(current_ret - bench_ret, 2)

    # Dias activo (dias de mercado desde pub_date)
    days_active = len(ticker_hist)

    # Estado de niveles
    levels = level_status(current_price, pick)

    # Determinar status automaticamente si esta abierto
    status = pick["status"]
    if status == "open":
        if levels["stop"]["hit"]:
            status = "closed_stop"
        elif levels["target2"]["hit"]:
            status = "closed_target2"
        elif levels["target1"]["hit"]:
            status = "closed_target1"

    return {
        "week":           pick["week"],
        "year":           pick["year"],
        "ticker":         pick["ticker"],
        "name":           pick["name"],
        "type":           pick["type"],
        "horizon":        pick["horizon"],
        "pub_date":       pick["pub_date"],
        "ref_price":      pick["ref_price"],
        "entry_zone":     f"${pick['entry_low']}–${pick['entry_high']}",
        "target1":        pick["target1"],
        "target2":        pick["target2"],
        "stop":           pick["stop"],
        "rr":             pick["rr"],
        "benchmark":      pick["benchmark"],
        "filename":       pick["filename"],
        "status":         status,
        "current_price":  current_price,
        "current_date":   current_date,
        "current_ret_pct": current_ret,
        "bench_ret_pct":  bench_ret,
        "vs_benchmark_pct": vs_benchmark,
        "days_active":    days_active,
        "closed_date":    pick.get("closed_date"),
        "closed_price":   pick.get("closed_price"),
        "levels":         levels,
        "chart_ticker":   ticker_returns,
        "chart_benchmark": bench_returns,
    }


def main():
    now_et = datetime.now(ET)
    print(f"PAD Pick Tracker — {now_et.strftime('%Y-%m-%d %H:%M ET')}")
    print(f"Procesando {len(PICKS)} pick(s)...\n")

    output = {
        "updated_at": now_et.isoformat(),
        "updated_at_display": now_et.strftime("%d-%b-%Y %H:%M ET"),
        "picks": []
    }

    for pick in PICKS:
        print(f"  [{pick['week']:02d}] {pick['ticker']} ({pick['pub_date']})...")
        result = process_pick(pick)
        if result:
            output["picks"].append(result)
            print(f"       {result['ticker']}: {result['current_price']} "
                  f"({'+' if result['current_ret_pct'] >= 0 else ''}{result['current_ret_pct']}%) "
                  f"vs {pick['benchmark']}: {'+' if result['vs_benchmark_pct'] >= 0 else ''}{result['vs_benchmark_pct']}% | {result['status']}")

    # Escribir JSON
    out_path = "picks_data.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nGuardado: {out_path}")
    print(f"Total picks procesados: {len(output['picks'])}")


if __name__ == "__main__":
    main()
