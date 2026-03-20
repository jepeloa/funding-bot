# Ψ-jam MCP Server

**MCP server para el framework Ψ-jam**: datos L1/L2 en tiempo real de Binance Futures + herramientas de análisis basadas en física de Langevin.

Permite que Claude interactúe directamente con datos de mercado reales para validar el modelo Ψ-jam.

## Arquitectura

```
┌──────────────────────────────────────────────────────┐
│                    Claude Desktop                     │
│                                                       │
│  "analizame DOGEUSDT"  →  full_jam_pipeline          │
│  "mostrame el L2 de ETH" → get_orderbook             │
│  "qué dice Kramers-Moyal de BTC 4h?" → analyze_km   │
└──────────────┬───────────────────────────────────────┘
               │ MCP (stdio)
┌──────────────▼───────────────────────────────────────┐
│              Ψ-jam MCP Server                        │
│                                                       │
│  ┌─────────────────┐    ┌──────────────────────────┐ │
│  │  DATA LAYER     │    │  ANALYSIS LAYER          │ │
│  │                 │    │                          │ │
│  │  L1: OHLCV,     │───▶│  Kramers-Moyal (D1,D2)  │ │
│  │      trades,    │    │  Hurst exponent          │ │
│  │      funding,   │    │  Lyapunov exponent       │ │
│  │      OI, LS     │    │  RQA (LAM, DET, ENTR)   │ │
│  │                 │    │  VPIN / Flow Toxicity    │ │
│  │  L2: orderbook, │    │  Kyle's Lambda           │ │
│  │      depth,     │    │  JAM Regime (A/B/N)      │ │
│  │      walls      │    │  Composite Risk Score    │ │
│  └────────┬────────┘    └──────────────────────────┘ │
│           │                                           │
└───────────┼───────────────────────────────────────────┘
            │ HTTPS (público, sin API key)
┌───────────▼───────────────────────────────────────────┐
│              Binance Futures API                       │
│              fapi.binance.com                          │
└───────────────────────────────────────────────────────┘
```

## Instalación

### 1. Requisitos

- Python 3.10+
- pip

### 2. Instalar el paquete

```bash
cd psi-jam-mcp
pip install -e .
```

### 3. Verificar que funciona

```bash
# Debería iniciar sin errores (Ctrl+C para salir)
psi-jam-mcp
```

### 4. Configurar Claude Desktop

Editá el archivo de configuración de Claude Desktop:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
**Linux**: `~/.config/Claude/claude_desktop_config.json`

Agregá la sección del MCP server:

```json
{
  "mcpServers": {
    "psi-jam": {
      "command": "psi-jam-mcp",
      "env": {}
    }
  }
}
```

**Alternativa (ruta explícita a Python)**:

```json
{
  "mcpServers": {
    "psi-jam": {
      "command": "python",
      "args": ["-m", "psi_jam_mcp.server"],
      "cwd": "/ruta/a/psi-jam-mcp",
      "env": {}
    }
  }
}
```

### 5. Reiniciar Claude Desktop

Cerrá y abrí Claude Desktop. Deberías ver las herramientas del MCP disponibles.

---

## Herramientas Disponibles

### 📊 Datos L1

| Tool | Descripción |
|------|-------------|
| `get_klines` | OHLCV candles (cualquier símbolo/timeframe) |
| `get_multi_tf_klines` | Multi-timeframe en una sola llamada |
| `get_recent_trades` | Últimos trades |
| `get_agg_trades` | Trades agregados con rango temporal |
| `get_funding_rate` | Historial de funding rate |
| `get_open_interest` | OI actual o histórico |
| `get_long_short_ratio` | Ratio cuentas long/short |
| `get_taker_volume` | Ratio volumen taker buy/sell |
| `get_ticker` | Stats 24h |
| `list_symbols` | Perpetuos USDT disponibles |

### 📈 Datos L2

| Tool | Descripción |
|------|-------------|
| `get_orderbook` | Order book completo + métricas + detección de walls |
| `get_orderbook_light` | Snapshot rápido: top 5 + métricas clave |

### 🔬 Análisis Ψ-jam

| Tool | Descripción | Output clave |
|------|-------------|--------------|
| `analyze_kramers_moyal` | Extracción coeficientes KM | D1 (drift), D2 (diffusion), potencial efectivo V(x), pozos |
| `analyze_hurst` | Exponente de Hurst (R/S) | H, régimen (persistente/anti-persistente/random walk) |
| `analyze_lyapunov` | Exponente de Lyapunov máximo | λ, régimen (caótico/estable/marginal) |
| `analyze_rqa` | Análisis de Recurrencia | LAM, DET, ENTR, TT |
| `analyze_vpin` | VPIN flow toxicity | VPIN actual, z-score, nivel de toxicidad |
| `analyze_kyles_lambda` | Impacto de precio Kyle | λ, tendencia |
| `analyze_jam_regime` | Clasificación régimen JAM | A/B/Neutral + parámetros Langevin |
| `full_jam_pipeline` | **Pipeline completo Ψ-jam** | Todo lo anterior + risk score compuesto |

---

## Uso con Claude

Una vez configurado, podés pedirle a Claude cosas como:

```
"Analizame DOGEUSDT en 1h"
→ Ejecuta full_jam_pipeline, te da el análisis completo

"Qué dice el order book de ETHUSDT?"
→ Ejecuta get_orderbook, muestra L2 con métricas

"Comparame el Hurst de BTC en 5m vs 4h"
→ Ejecuta analyze_hurst dos veces con distintos timeframes

"Mostrame los coeficientes Kramers-Moyal de SOL en 15m"
→ Extrae D1, D2, potencial efectivo

"Hay señal de jam en alguna altcoin low-cap?"
→ Puede recorrer varios símbolos con analyze_jam_regime

"Qué tan tóxico está el flujo de PEPEUSDT?"
→ Ejecuta analyze_vpin

"Dame el L2 de BTCUSDT y detectame las walls"
→ get_orderbook con wall detection automático
```

---

## Ecuación Maestra del Framework

```
L(V,η)ẍ + γ(V,η)ẋ + κ(x)x = F_ext(t)
```

Donde:
- **L(V,η)**: Masa efectiva (inercia del sistema) — depende de volumen V y liquidez η
- **γ(V,η)**: Coeficiente de amortiguamiento — energía disipada por fricción de mercado
- **κ(x)**: Fuerza restauradora — tendencia a mean-reversion
- **F_ext(t)**: Fuerza externa — shocks de volumen, noticias, liquidaciones

### Mapeo a indicadores del MCP:

| Parámetro físico | Indicador MCP | Tool |
|---|---|---|
| D1 (drift) | Kramers-Moyal D1 | `analyze_kramers_moyal` |
| D2 (difusión/ruido) | Kramers-Moyal D2 | `analyze_kramers_moyal` |
| V(x) (potencial) | Potencial efectivo reconstruido | `analyze_kramers_moyal` |
| γ (damping) | 1/retention | `analyze_jam_regime` |
| κ (restoring) | absorption | `analyze_jam_regime` |
| F_ext (fuerza) | vol_ratio × delta | `analyze_jam_regime` |
| Estabilidad | Lyapunov λ | `analyze_lyapunov` |
| Persistencia | Hurst H | `analyze_hurst` |
| Jamming proximity | Laminarity LAM | `analyze_rqa` |
| Flow toxicity | VPIN z-score | `analyze_vpin` |
| Price impact | Kyle's λ | `analyze_kyles_lambda` |

---

## Desarrollo

```bash
# Instalar en modo desarrollo
pip install -e ".[dev]"

# Correr tests
python -m pytest tests/

# Estructura del proyecto
psi-jam-mcp/
├── pyproject.toml
├── README.md
└── src/
    └── psi_jam_mcp/
        ├── __init__.py
        ├── server.py          # MCP server + tool definitions
        ├── binance_client.py  # Binance Futures API wrapper
        └── analysis.py        # Ψ-jam analysis functions
```

## Notas

- **Sin API key**: Usa solo endpoints públicos de Binance Futures
- **Rate limits**: Binance permite ~1200 requests/min en endpoints públicos
- **Datos**: Futuros USDT-M perpetuos (no spot)
- **Análisis on-the-fly**: Los datos se bajan, analizan y devuelven en cada llamada (no se persisten)
