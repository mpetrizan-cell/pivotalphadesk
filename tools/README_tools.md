# PAD Tools — Automatizacion SPY

Carpeta de scripts para generar zonas gamma PAD desde el chain de Tradestation.
No afecta GitHub Pages — solo para uso local.

---

## Requisitos

```bash
pip install pandas numpy scipy xlrd openpyxl
```

---

## Workflow diario — SPY

### Paso 1 — Exportar chain desde Tradestation

En OptionStation Pro:
1. Abrir chain de SPY con todos los vencimientos visibles
2. File → Export → Excel (.xls)
3. Guardar como `SPY_OI_Chain_MMDDYYYY.xls`

### Paso 2 — Correr el script

```bash
# Desde la carpeta raiz del repo:
python tools/pad_zones_spy.py SPY_OI_Chain_03242026.xls --price 655.85
```

Output generado automaticamente:
- Reporte de niveles en consola
- `PAD_SPY_Zones_YYYYMMDD.json` — niveles del dia para uso en GAIA/Pine
- `PAD_SPY_Zonas_YYYYMMDD.pine` — Pine Script listo para pegar en TradingView

### Paso 3 — Pegar el Pine en TradingView

1. TradingView → Pine Editor → New
2. Pegar el contenido del .pine generado
3. Add to chart en grafico SPY

---

## Validacion BS (opcional)

Para validar el gamma de Tradestation contra el modelo Black-Scholes propio:

```bash
python tools/pad_lab_gamma.py SPY_OI_Chain_03242026.xls --price 655.85
```

Genera:
- Reporte de error relativo BS vs TS (esperado ~9-12%)
- Mapa de exposicion gamma por strike
- `PAD_SPY_GammaMap_YYYYMMDD.json`

---

## Niveles PAD — SPY (24 Mar 2026 — referencia)

| Nivel         | Strike  | OI Referencia         |
|---------------|---------|----------------------|
| Call Wall     | $660    | 74,308 call OI       |
| Gamma Flip    | $659–661| Net gamma cruza 0    |
| Resistencia   | $658    | bilateral            |
| Soporte P.    | $655    | 30K C / 79K P        |
| Put Wall      | $650    | 158,386 put OI       |
| Put Wall 2    | $645    | 131,661 put OI       |

**Regimen: AMPLIFIED** | Net Gamma ATM: -31,728 | P/C ratio: 3.0x

---

## Proxima fase — API Tradestation

Cuando se active la API key de Tradestation, `pad_zones_spy.py` se actualiza
para descargar el chain automaticamente sin exportar el XLS.
Endpoint: `https://api.tradestation.com/v3/marketdata/options/chain`

---

## Expansion — otros activos via Tradestation

Mismo workflow para cualquier ETF/equity con opciones liquidas:
- QQQ (proxy NQ)
- IWM
- GLD
- XLE
- Acciones individuares del PAD Pick

Cambiar el simbolo en el export de Tradestation y correr el mismo script.

---

*PivotAlphaDesk — Gamma Market Structure Framework*
*pivotalphadesk.com*
