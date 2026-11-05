#!/usr/bin/env python3
"""
GAIA EQUITY INTELLIGENCE V1.1 — PHASE 2 (CORRECTED — Round 1)
Structure + Multi-Expiry + Volatility Engine

Input:  Phase 1 normalized JSON (never re-parses the raw CSV, never modifies
        the Phase 1 normalizer).
Output: static analytical core per MASTER SPEC §5.2 (Options Structure),
        §5.3 (Multi-Expiry), §5.4 (Volatility).

Corrections applied in this round (Miguel's review of the first Phase 2 pass):
  1. spot is a MANDATORY EXTERNAL INPUT to this module — never estimated.
     No put-call parity, no delta-crossing approximation is used in any
     calculation. If spot is not supplied by the caller, GEX/levels/distances
     are left unset and reported as blocked in `stop_conditions`. If spot IS
     supplied, this module would compute them using the exact Phase 0 frozen
     formulas — never a re-derivation (not implemented yet, see below).
  2. tenor_bucket (DTE-based) and expiry_calendar_class (calendar-based,
     conservative — Round 2) are two INDEPENDENT taggings, never mixed with
     each other or with DTE. This removes the prior bug where 2026-10-02
     (dte=36, a real Weekly per horizon_label_raw) was mislabeled "OPEX"
     purely because of its DTE, AND removes the Round 1 over-reach where
     DTE>365 was asserted to mean LEAPS and a 3rd-Friday-in-a-quarter-month
     was asserted to mean QUARTERLY — neither is proven by the data we have.
  3. No threshold-based interpretive labels for V1: skew and term structure
     are reported as raw quantitative values only (no PUT SKEW/CALL SKEW/
     FLAT, no CONTANGO/BACKWARDATION with an epsilon tolerance, no "strong
     bias" language).
  4. persistence_top_n is explicitly marked status=EXPERIMENTAL — not a
     validated constant.

Symbol-generic: every function takes the normalized chain (and optionally
spot) as data; nothing here is NVDA-specific.
"""

from __future__ import annotations
import json
import sys
from datetime import datetime, timezone, date
from collections import defaultdict

PERSISTENCE_TOP_N = 5  # EXPERIMENTAL — see Phase 2 report §5, not a validated constant


# ── Tenor bucket — DTE only ─────────────────────────────────────────────────
def tag_tenor_bucket(dte):
    if dte <= 1:
        return "IMMEDIATE"
    if dte <= 7:
        return "NEAR_TERM"
    if dte <= 120:
        return "INTERMEDIATE"
    return "LONGER_DATED"


# ── Expiry type — CORRECTED (Round 2): no longer infers LEAPS/QUARTERLY from
# DTE/calendar position alone — that was an unproven inference dressed as a
# fact. Two separate, conservative fields instead:
#   expiry_type_raw       — only what TradeStation itself provides (e.g. the
#                            "Weekly" label from the export). Never invented.
#   expiry_calendar_class — a narrow, verifiable calendar observation:
#                            STANDARD_MONTHLY (3rd Friday — the one calendar
#                            fact we CAN verify directly), WEEKLY (raw label
#                            says so and it isn't a 3rd Friday), OTHER (a
#                            real, successfully-parsed expiration that fits
#                            neither pattern — e.g. calendar-shifted dates
#                            like 2027-06-17, a Thursday), UNKNOWN (date
#                            itself unparseable — should not occur post
#                            Phase 1 validation, kept as a safety net only).
def tag_expiry_type(expiration_iso, dte, horizon_label_raw):
    try:
        y, m, d = map(int, expiration_iso.split("-"))
        dt = date(y, m, d)
    except (ValueError, AttributeError):
        return {"expiry_type_raw": horizon_label_raw, "expiry_calendar_class": "UNKNOWN"}

    is_third_friday = dt.weekday() == 4 and 15 <= d <= 21

    if is_third_friday:
        cls = "STANDARD_MONTHLY"
    elif horizon_label_raw == "Weekly":
        cls = "WEEKLY"
    else:
        cls = "OTHER"

    return {"expiry_type_raw": horizon_label_raw, "expiry_calendar_class": cls}


def load_normalized(path):
    with open(path) as f:
        return json.load(f)


# ── §1 Options Structure Engine — OI concentration + Gamma-Weighted OI ─────
def build_structure_engine(chain):
    expiries = chain["expiries"]

    chain_call_oi = 0.0
    chain_put_oi = 0.0
    strike_rows = []

    for e in expiries:
        for s in e["strikes"]:
            c_oi = s["call"]["open_interest"] or 0.0
            p_oi = s["put"]["open_interest"] or 0.0
            chain_call_oi += c_oi
            chain_put_oi += p_oi
            strike_rows.append((e["expiration"], s["strike"], c_oi, p_oi))

    chain_total_oi = chain_call_oi + chain_put_oi

    by_expiry = []
    gwoi_by_expiry = []

    for e in expiries:
        exp_call_oi = sum((s["call"]["open_interest"] or 0.0) for s in e["strikes"])
        exp_put_oi = sum((s["put"]["open_interest"] or 0.0) for s in e["strikes"])
        exp_total_oi = exp_call_oi + exp_put_oi

        by_expiry.append({
            "expiration": e["expiration"], "dte": e["dte"],
            "call_oi": exp_call_oi, "put_oi": exp_put_oi, "total_oi": exp_total_oi,
            "call_oi_share_of_chain": round(exp_call_oi / chain_call_oi, 4) if chain_call_oi else None,
            "put_oi_share_of_chain": round(exp_put_oi / chain_put_oi, 4) if chain_put_oi else None,
            "total_oi_share_of_chain": round(exp_total_oi / chain_total_oi, 4) if chain_total_oi else None,
        })

        strikes_out = []
        for s in e["strikes"]:
            c_oi = s["call"]["open_interest"] or 0.0
            p_oi = s["put"]["open_interest"] or 0.0
            c_gamma = s["call"]["gamma"] or 0.0
            p_gamma = s["put"]["gamma"] or 0.0  # already abs()-normalized in Phase 1

            call_gwoi = round(c_gamma * c_oi, 4)
            put_gwoi = round(p_gamma * p_oi, 4)
            total_gwoi = round(call_gwoi + put_gwoi, 4)  # unsigned concentration MASS, not net_gex

            strikes_out.append({
                "strike": s["strike"],
                "call_oi": c_oi, "put_oi": p_oi, "total_oi": c_oi + p_oi,
                "call_oi_share_within_expiry": round(c_oi / exp_call_oi, 4) if exp_call_oi else None,
                "put_oi_share_within_expiry": round(p_oi / exp_put_oi, 4) if exp_put_oi else None,
                "call_gwoi": call_gwoi, "put_gwoi": put_gwoi, "total_gwoi": total_gwoi,
            })

        gwoi_by_expiry.append({
            "expiration": e["expiration"], "dte": e["dte"],
            "strikes": strikes_out,
            "top_oi_strikes": sorted(strikes_out, key=lambda x: -x["total_oi"])[:PERSISTENCE_TOP_N],
            "top_gwoi_strikes": sorted(strikes_out, key=lambda x: -x["total_gwoi"])[:PERSISTENCE_TOP_N],
        })

    return {
        "universe_definition": (
            f"Full symbol chain — all {len(expiries)} captured expiries, "
            f"{len(strike_rows)} strike-rows."
        ),
        "chain_totals": {
            "call_oi_total": chain_call_oi, "put_oi_total": chain_put_oi,
            "total_oi": chain_total_oi,
            "call_put_oi_ratio": round(chain_call_oi / chain_put_oi, 4) if chain_put_oi else None,
        },
        "oi_by_expiry": by_expiry,
        "gamma_weighted_oi_by_expiry": gwoi_by_expiry,
        "gwoi_label_note": (
            "total_gwoi = call_gwoi + put_gwoi es una masa de concentración SIN "
            "signo (Gamma x OI), per MASTER SPEC 5.2. Explícitamente NO es "
            "net_gex y no debe sustituirlo — net_gex requiere spot."
        ),
    }


# ── §2 Multi-Expiry Structure Engine ────────────────────────────────────────
def build_multi_expiry_engine(chain, structure):
    expiries = chain["expiries"]
    tagged = []
    for e in expiries:
        tagged.append({
            "expiration": e["expiration"], "dte": e["dte"],
            "horizon_label_raw": e["horizon_label"],
            "tenor_bucket": tag_tenor_bucket(e["dte"]),
            **tag_expiry_type(e["expiration"], e["dte"], e["horizon_label"]),
        })

    def persistence_for(top_list_key):
        appearances = defaultdict(list)
        for exp_block in structure["gamma_weighted_oi_by_expiry"]:
            for s in exp_block[top_list_key]:
                appearances[s["strike"]].append(exp_block["expiration"])
        n_expiries = len(expiries)
        result = []
        for strike, exp_list in appearances.items():
            result.append({
                "strike": strike,
                "qualifying_expiries": len(exp_list),
                "expiries_analyzed": n_expiries,
                "persistence": round(len(exp_list) / n_expiries, 4),
                "expirations": exp_list,
            })
        return sorted(result, key=lambda x: -x["persistence"])

    oi_persistence = persistence_for("top_oi_strikes")
    gwoi_persistence = persistence_for("top_gwoi_strikes")

    return {
        "tenor_bucket_rule": (
            "DTE-only, independiente de expiry_type: IMMEDIATE dte<=1, "
            "NEAR_TERM 1<dte<=7, INTERMEDIATE 7<dte<=120, LONGER_DATED dte>120. "
            "Convención propuesta, no auditada en Phase 0 — no bloquea el output."
        ),
        "expiry_type_rule": (
            "Conservador (Round 2): expiry_type_raw sólo refleja lo que "
            "TradeStation entrega (ej. 'Weekly'), nunca inferido. "
            "expiry_calendar_class es una observación de calendario acotada: "
            "STANDARD_MONTHLY (3er viernes — el único hecho de calendario "
            "verificable directamente), WEEKLY (label raw='Weekly' y no es "
            "3er viernes), OTHER (expiración real, parseada correctamente, "
            "que no matchea ninguno de los dos patrones — ej. fechas "
            "corridas por feriados), UNKNOWN (fecha no parseable, no debería "
            "ocurrir tras la validación de Phase 1). Ya NO infiere LEAPS ni "
            "QUARTERLY sin metadata que lo confirme."
        ),
        "expiries_tagged": tagged,
        "persistence_top_n": PERSISTENCE_TOP_N,
        "persistence_top_n_status": "EXPERIMENTAL",
        "oi_persistence_top_strikes": oi_persistence[:15],
        "gamma_weighted_oi_persistence_top_strikes": gwoi_persistence[:15],
    }


# ── §3 Volatility Engine — raw values only, no threshold-based labels ──────
def nearest_by_delta(strikes, side, target_delta):
    best, best_diff = None, None
    for s in strikes:
        d = s[side]["delta"]
        if d is None:
            continue
        diff = abs(d - target_delta)
        if best_diff is None or diff < best_diff:
            best, best_diff = s, diff
    return best


def build_volatility_engine(chain):
    expiries = chain["expiries"]
    term_structure = []
    for e in expiries:
        term_structure.append({
            "expiration": e["expiration"], "dte": e["dte"],
            "expiry_iv_summary": e["expiry_iv_summary"],
            "expected_move": e["expected_move"],
        })

    ivs = [t["expiry_iv_summary"] for t in term_structure if t["expiry_iv_summary"] is not None]
    diffs = [round(ivs[i + 1] - ivs[i], 6) for i in range(len(ivs) - 1)]
    if all(d >= 0 for d in diffs):
        monotonicity = "MONOTONIC_NON_DECREASING"
    elif all(d <= 0 for d in diffs):
        monotonicity = "MONOTONIC_NON_INCREASING"
    else:
        monotonicity = "NON_MONOTONIC"

    skew_25d = []
    for e in expiries:
        call25 = nearest_by_delta(e["strikes"], "call", 0.25)
        put25 = nearest_by_delta(e["strikes"], "put", 0.25)
        entry = {"expiration": e["expiration"], "dte": e["dte"]}
        if call25 is None or put25 is None:
            entry.update({"status": "FALTA DATA — no strike with usable delta near 0.25 on one side"})
            skew_25d.append(entry)
            continue
        call_iv = call25["call"]["iv"]
        put_iv = put25["put"]["iv"]
        if call_iv is None or put_iv is None:
            entry.update({
                "status": "FALTA DATA — nearest-25-delta strike has null IV (NE/~0 token)",
                "call_25d_strike": call25["strike"], "put_25d_strike": put25["strike"],
            })
            skew_25d.append(entry)
            continue
        entry.update({
            "status": "OK",
            "call_25d_strike": call25["strike"], "call_25d_delta": call25["call"]["delta"], "call_25d_iv": call_iv,
            "put_25d_strike": put25["strike"], "put_25d_delta": put25["put"]["delta"], "put_25d_iv": put_iv,
            "skew_put_minus_call": round(put_iv - call_iv, 4),
        })
        skew_25d.append(entry)

    return {
        "term_structure": term_structure,
        "term_structure_monotonicity": monotonicity,
        "term_structure_note": (
            "Comparación estrictamente monotónica entre expiraciones consecutivas "
            "de expiry_iv_summary, SIN tolerancia/epsilon. No se etiqueta "
            "CONTANGO/BACKWARDATION — eso implicaría un umbral no auditado."
        ),
        "skew_25delta_by_expiry": skew_25d,
        "skew_method_note": (
            "skew_put_minus_call es un valor crudo (put_25d_iv - call_25d_iv). "
            "Sin clasificación PUT SKEW/CALL SKEW/FLAT — esos requerirían un "
            "umbral no auditado. 25-delta strike seleccionado por |delta-0.25| "
            "mínimo usando el delta ya normalizado de Phase 1 — no requiere spot."
        ),
    }


# ── §4 Structural Candidate Classification — tenor_bucket based, no scores ─
def build_structural_candidates(multi_expiry):
    tenor_by_exp = {t["expiration"]: t["tenor_bucket"] for t in multi_expiry["expiries_tagged"]}

    def buckets_for(strike_records):
        by_strike = defaultdict(set)
        for rec in strike_records:
            for exp in rec["expirations"]:
                by_strike[rec["strike"]].add(tenor_by_exp.get(exp))
        return by_strike

    oi_buckets = buckets_for(multi_expiry["oi_persistence_top_strikes"])
    gwoi_buckets = buckets_for(multi_expiry["gamma_weighted_oi_persistence_top_strikes"])

    all_strikes = set(oi_buckets) | set(gwoi_buckets)
    short_b = {"IMMEDIATE", "NEAR_TERM"}
    candidates = []
    for strike in sorted(all_strikes):
        b_oi = oi_buckets.get(strike, set())
        b_gwoi = gwoi_buckets.get(strike, set())
        b_all = b_oi | b_gwoi

        if len(b_all) >= 2:
            cls = "MULTI-LAYER STRUCTURAL"
        elif b_all == {"LONGER_DATED"}:
            cls = "DISTANT STRUCTURAL"
        elif b_all and b_all.issubset(short_b):
            cls = "NEAR-TERM DOMINANT"
        elif b_all == {"INTERMEDIATE"}:
            cls = "INTERMEDIATE STRUCTURAL"
        elif b_gwoi and not b_oi:
            cls = "GAMMA-SENSITIVE"
        else:
            cls = "UNCLASSIFIED"  # safety net, should not occur given buckets above

        candidates.append({
            "strike": strike, "structural_class": cls,
            "tenor_buckets_oi": sorted(b_oi), "tenor_buckets_gwoi": sorted(b_gwoi),
        })
    return {
        "classification_note": (
            "Clasificación descriptiva basada en tenor_bucket (DTE-only), NO en "
            "expiry_type — evita mezclar tenor con tipo de vencimiento. No es un "
            "score compuesto ni ponderado — cumple la Sección 11 del Master Spec."
        ),
        "candidates": candidates,
    }


# ── STOP CONDITIONS ──────────────────────────────────────────────────────
def build_stop_conditions(spot):
    conditions = []
    if spot is None:
        conditions.append({
            "item": "spot (underlying price)",
            "status": "NEEDS DECISION — spot no fue provisto a este módulo",
            "reason": (
                "spot es un input externo obligatorio de este motor, igual que "
                "lo será ts_gaia_stocks.py en producción. El CSV de opciones no "
                "lo contiene y este módulo NO lo estima (sin put-call parity, "
                "sin aproximación por delta) — decisión explícita de Miguel."
            ),
            "blocks": [
                "audited call_gex / put_gex / net_gex (fórmula raíz Phase 0, requiere spot**2)",
                "Call Wall / Put Wall / Gamma Node / Gravity Pin / Gamma Flip",
                "distance_points, distance_pct, distance_EM",
                "clasificación inside/boundary/outside del Expected Move",
            ],
            "resolution": (
                "Recuperar el Last real de NVDA de TradeStation al momento de "
                "exportación, o repetir un export guardando spot+timestamp+chain "
                "simultáneamente. Pasar como argumento spot= a build_phase2()."
            ),
        })
    return conditions


def build_gex_and_levels(chain, spot):
    """Computes call_gex/put_gex/net_gex and the 5 frozen GAIA levels PER
    EXPIRY, using exactly the Phase 0 frozen root formula (ts_gaia_chart.py
    calculate_gaia / calculate_levels). Deliberately NOT aggregated across
    expiries into one blended map — Phase 0 only audited the formula for a
    single snapshot's strikes; blending net_gex across multiple expiries
    before selecting levels would be a NEW, unaudited aggregation decision.
    That choice is left open (see multi_expiry_gex_aggregation NEEDS
    DECISION note in the output) rather than assumed silently.
    """
    spot2 = spot ** 2
    per_expiry = []
    skipped_empty_expiries = []

    for e in chain["expiries"]:
        # FIX 05-nov-2026: TradeStation puede listar un vencimiento sin strikes
        # todavía (ej. 2026-12-24). Sin strikes no hay niveles que calcular:
        # se omite y se deja registrado, en lugar de caer en max() vacío.
        if not e["strikes"]:
            skipped_empty_expiries.append(e["expiration"])
            continue
        em = e["expected_move"]  # already parsed in Phase 1, per-expiry
        strikes_gex = []
        for s in e["strikes"]:
            c_oi = s["call"]["open_interest"] or 0.0
            p_oi = s["put"]["open_interest"] or 0.0
            c_gamma = s["call"]["gamma"] or 0.0
            p_gamma = s["put"]["gamma"] or 0.0  # already abs()-normalized in Phase 1

            call_gex = round(c_oi * c_gamma * spot2 * 100 / 1e6, 6)
            put_gex = round(p_oi * p_gamma * spot2 * 100 * -1 / 1e6, 6)
            net_gex = round(call_gex + put_gex, 6)

            strikes_gex.append({
                "strike": s["strike"],
                "call_oi": c_oi, "put_oi": p_oi,
                "call_gex": call_gex, "put_gex": put_gex, "net_gex": net_gex,
            })

        strikes_sorted = sorted(strikes_gex, key=lambda x: x["strike"])

        # ── Call Wall ──
        above = [s for s in strikes_sorted if s["strike"] >= spot]
        call_wall = max(above, key=lambda x: x["call_gex"]) if above else \
            max(strikes_sorted, key=lambda x: x["call_gex"])
        call_wall_fallback_used = not bool(above)

        # ── Put Wall ──
        below = [s for s in strikes_sorted if s["strike"] < spot]
        put_wall = min(below, key=lambda x: x["put_gex"]) if below else \
            min(strikes_sorted, key=lambda x: x["put_gex"])
        put_wall_fallback_used = not bool(below)

        # ── Gamma Node — argmax(call_gex + |put_gex|), NOT argmax(net_gex) ──
        gamma_node = max(strikes_sorted, key=lambda x: x["call_gex"] + abs(x["put_gex"]))

        # ── Gravity Pin — argmax(call_oi + put_oi), Max OI, not gamma-weighted ──
        gravity_pin = max(strikes_sorted, key=lambda x: x["call_oi"] + x["put_oi"])

        # ── Gamma Flip — first strike ascending where CUMULATIVE net_gex
        # crosses - to >=0 (industry-standard "zero-gamma level" definition
        # — corregido 14-sep-2026, verificado contra 6 fuentes independientes:
        # FlashAlpha, GEXRadar, VannaCharm, GEXBoard, ZeroGEX, CrossVol.
        # La versión anterior miraba el signo de CADA strike por separado,
        # lo que dejaba que un solo strike sin Open Interest en la cola
        # lejana registrara un "cruce" espurio antes de llegar a la zona
        # relevante cerca del spot — confirmado con datos reales de NVDA
        # 14-sep-2026: daba strike 140 (spot 212.16, 34% de distancia, OI
        # cero de los dos lados) en vez del cruce real en 210 (1% del spot).
        gamma_flip, gamma_flip_fallback_used = None, False
        cumulative = 0.0
        for s in strikes_sorted:
            prev_cumulative = cumulative
            cumulative = round(cumulative + s["net_gex"], 6)
            if prev_cumulative < 0 <= cumulative:
                gamma_flip = s
                break
        if gamma_flip is None:
            gamma_flip = min(strikes_sorted, key=lambda x: abs(x["net_gex"]))
            gamma_flip_fallback_used = True

        def with_distance(level, fallback_used=False):
            if level is None:
                return None
            dist_pts = round(level["strike"] - spot, 4)
            dist_pct = round(dist_pts / spot, 6)
            if em and em > 0:
                dist_em = round(dist_pts / em, 4)
                if abs(dist_pts) < em:
                    em_class = "INSIDE_EM"
                elif abs(dist_pts) > em:
                    em_class = "OUTSIDE_EM"
                else:
                    em_class = "AT_EM_BOUNDARY"
            else:
                dist_em, em_class = None, "FALTA DATA — no expected_move for this expiry"
            return {
                **level,
                "distance_points": dist_pts,
                "distance_pct": dist_pct,
                "distance_EM": dist_em,
                "em_intersection": em_class,
                "fallback_used": fallback_used,
            }

        per_expiry.append({
            "expiration": e["expiration"], "dte": e["dte"],
            "expected_move": em,
            "levels": {
                "call_wall": with_distance(call_wall, call_wall_fallback_used),
                "put_wall": with_distance(put_wall, put_wall_fallback_used),
                "gamma_node": with_distance(gamma_node),
                "gravity_pin": with_distance(gravity_pin),
                "gamma_flip": with_distance(gamma_flip, gamma_flip_fallback_used),
            },
            "strikes_gex": strikes_sorted,
        })

    return {
        "spot": spot,
        "formula_note": (
            "call_gex = call_oi*call_gamma*spot^2*100/1e6 ; "
            "put_gex = put_oi*put_gamma*spot^2*100*-1/1e6 ; net_gex = call_gex+put_gex. "
            "Fórmula raíz congelada en Phase 0 (ts_gaia_chart.py), aplicada sin modificación."
        ),
        "levels_note": (
            "Los 5 niveles y sus fallbacks se calculan POR EXPIRACIÓN, exactamente "
            "como en la fórmula de Phase 0 (que opera sobre un único snapshot de "
            "strikes). gamma_node = argmax(call_gex + |put_gex|), NO argmax(net_gex). "
            "gravity_pin = argmax(call_oi + put_oi), Max OI, no gamma-weighted."
        ),
        "multi_expiry_gex_aggregation": (
            "NEEDS DECISION — no se calculó ningún net_gex 'agregado' que sume o "
            "combine strikes de distintas expiraciones en un solo mapa. Phase 0 "
            "sólo auditó la fórmula para un snapshot único; blend cross-expiry "
            "sería una decisión de agregación nueva, no auditada. Se deja abierta "
            "para Miguel/Nexa, no asumida en silencio."
        ),
        "per_expiry": per_expiry,
        "skipped_empty_expiries": skipped_empty_expiries,
    }


def build_phase2(normalized_path, symbol, spot=None):
    chain = load_normalized(normalized_path)
    if chain["symbol"] != symbol.upper():
        raise ValueError(f"Symbol mismatch: file is {chain['symbol']}, requested {symbol}")

    structure = build_structure_engine(chain)
    multi_expiry = build_multi_expiry_engine(chain, structure)
    volatility = build_volatility_engine(chain)
    structural_candidates = build_structural_candidates(multi_expiry)
    stop_conditions = build_stop_conditions(spot)

    gex_and_levels = None
    if spot is not None:
        gex_and_levels = build_gex_and_levels(chain, spot)

    return {
        "symbol": chain["symbol"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_normalized_file": normalized_path,
        "phase1_source_timestamp": chain["source_timestamp"],
        "phase1_chain_id": chain["chain_id"],
        "spot_input": spot,
        "structure_engine": structure,
        "multi_expiry_engine": multi_expiry,
        "volatility_engine": volatility,
        "structural_candidates": structural_candidates,
        "gex_and_levels": gex_and_levels,
        "stop_conditions": stop_conditions,
        "out_of_scope_confirmation": (
            "No se calculó DSI, VTS, MCS ni ningún score compuesto derivado de "
            "Pressure Map / Terminal V10 en este módulo — confirmado por diseño."
        ),
    }


if __name__ == "__main__":
    normalized_path = sys.argv[1] if len(sys.argv) > 1 else "/home/claude/phase1/NVDA_normalized.json"
    symbol = sys.argv[2] if len(sys.argv) > 2 else "NVDA"
    spot = float(sys.argv[3]) if len(sys.argv) > 3 else None

    result = build_phase2(normalized_path, symbol, spot=spot)
    out_path = f"/home/claude/phase1/{symbol}_phase2.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Symbol: {result['symbol']}")
    print(f"spot_input: {result['spot_input']}")
    print(f"Chain totals: {result['structure_engine']['chain_totals']}")
    print(f"Expiries tagged: {len(result['multi_expiry_engine']['expiries_tagged'])}")
    unknown_types = [t for t in result['multi_expiry_engine']['expiries_tagged'] if t['expiry_calendar_class'] == 'UNKNOWN']
    print(f"expiry_calendar_class UNKNOWN cases: {len(unknown_types)}")
    unclassified = [c for c in result['structural_candidates']['candidates'] if c['structural_class'] == 'UNCLASSIFIED']
    print(f"Structural UNCLASSIFIED: {len(unclassified)}")
    print(f"STOP CONDITIONS: {len(result['stop_conditions'])}")
    print(f"Written: {out_path}")
