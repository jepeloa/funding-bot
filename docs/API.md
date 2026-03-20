# Binance Futures Recorder — REST API Documentation

**Versión:** 1.0.0  
**Base URL:** `https://190.31.60.204`  
**Protocolo:** HTTPS (TLS, certificado self-signed — usar `-k` en curl o `verify=False` en Python)  
**Formato de respuesta:** JSON  
**Swagger UI:** `https://190.31.60.204/docs`  
**ReDoc:** `https://190.31.60.204/redoc`

---

## Tabla de Contenidos

- [Autenticación](#autenticación)
- [Parámetros Comunes](#parámetros-comunes)
- [Errores](#errores)
- [Endpoints](#endpoints)
  - [Health Check](#get-health)
  - [Stats](#get-stats)
  - [Symbols](#get-symbols)
  - [DB Size](#get-dbsize)
  - [Storage Monitor](#get-storage)
  - [Trades](#get-tradessymbol)
  - [Depth (Order Book)](#get-depthsymbol)
  - [Book Tickers](#get-tickerssymbol)
  - [Mark Price](#get-markssymbol)
  - [Open Interest](#get-oisymbol)
  - [Funding Rates](#get-fundingsymbol)
  - [OHLCV Candles](#get-ohlcvsymbol)
  - [Liquidations](#get-liquidations)
  - [Long/Short Ratio](#get-lsrsymbol)
  - [Taker Buy/Sell Volume](#get-takersymbol)
  - [Spread Analysis](#get-spreadsymbol)
  - [Strategy Snapshots](#get-snapshotssymbol)
  - [Virtual Trades](#get-vtrades)
  - [PnL Summary](#get-pnl)
- [Ejemplos de Integración](#ejemplos-de-integración)
- [Límites y Consideraciones](#límites-y-consideraciones)

---

## Autenticación

Todos los endpoints (excepto `/health`) requieren un API key enviado en el header `X-API-Key`.

```
X-API-Key: tu_api_key_aqui
```

Si el key es inválido o está ausente, la API devuelve:

```json
{ "detail": "Invalid or missing API key" }
```

**HTTP Status:** `401 Unauthorized`

---

## Parámetros Comunes

La mayoría de los endpoints de datos aceptan estos query parameters:

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `limit` | int | 100 | Número máximo de registros a devolver (1–10,000) |
| `start` | string | — | Tiempo inicial del rango. Acepta ISO-8601 o epoch (seconds/millis) |
| `end` | string | — | Tiempo final del rango. Acepta ISO-8601 o epoch (seconds/millis) |

### Formatos de tiempo aceptados

| Formato | Ejemplo |
|---|---|
| ISO-8601 (UTC) | `2025-01-15T12:00:00Z` |
| ISO-8601 (offset) | `2025-01-15T09:00:00-03:00` |
| Epoch seconds | `1736942400` |
| Epoch milliseconds | `1736942400000` |

---

## Errores

| HTTP Status | Significado |
|---|---|
| `200` | Éxito |
| `400` | Parámetro inválido (ej: formato de tiempo incorrecto, intervalo no soportado) |
| `401` | API key inválido o ausente |
| `404` | Sin datos para el símbolo solicitado |
| `503` | Base de datos no disponible |

Formato de error:

```json
{ "detail": "Descripción del error" }
```

---

## Endpoints

---

### `GET /health`

Estado del servicio y conectividad con la base de datos. **No requiere autenticación.**

#### Response

```json
{ "status": "ok" }
```

Si la DB no responde → `503`.

---

### `GET /stats`

Resumen general del sistema: conteo aproximado de registros por tabla, rango temporal, tamaño de la DB, virtual trades.

#### Response

```json
{
  "depth_updates": {
    "rows_approx": 145000000,
    "from": "2025-03-01T00:00:01+00:00",
    "to": "2025-03-15T23:59:59+00:00"
  },
  "agg_trades": {
    "rows_approx": 89000000,
    "from": "2025-03-01T00:00:00+00:00",
    "to": "2025-03-15T23:59:59+00:00"
  },
  "book_tickers": { "..." : "..." },
  "mark_prices": { "..." : "..." },
  "open_interest": { "..." : "..." },
  "funding_rates": { "..." : "..." },
  "strategy_snapshots": { "..." : "..." },
  "liquidations": { "..." : "..." },
  "long_short_ratio": { "..." : "..." },
  "taker_buy_sell": { "..." : "..." },
  "virtual_trades": { "open": 3, "closed": 12 },
  "db_size_bytes": 92274688000,
  "db_size_gb": 85.93,
  "symbols_count": 554
}
```

---

### `GET /symbols`

Lista de todos los símbolos (pares) que tienen trades grabados.

#### Response

```json
["1000BONKUSDT", "1000FLOKIUSDT", "1000PEPEUSDT", "AAVEUSDT", "...", "BTCUSDT", "ETHUSDT", "..."]
```

---

### `GET /dbsize`

Tamaño detallado por hypertable y total de la base de datos.

#### Response

```json
{
  "tables": [
    { "name": "depth_updates", "size_bytes": 38500000000, "size_pretty": "35 GB" },
    { "name": "agg_trades", "size_bytes": 22000000000, "size_pretty": "20 GB" },
    { "name": "book_tickers", "size_bytes": 18000000000, "size_pretty": "17 GB" }
  ],
  "total_bytes": 92274688000,
  "total_pretty": "85.93 GB"
}
```

---

### `GET /storage`

Monitoreo completo de almacenamiento: tamaño de la DB por tabla, espacio en disco, estadísticas de compresión y tasa de crecimiento con estimación de días restantes.

Ideal para dashboards de monitoreo y alertas.

#### Response

```json
{
  "disk": {
    "path": "/media/mapplics-ia/recorder-data",
    "total_bytes": 3936786309120,
    "total_pretty": "3666.4 GB",
    "used_bytes": 134234181632,
    "used_pretty": "125.0 GB",
    "free_bytes": 3602497826816,
    "free_pretty": "3355.1 GB",
    "used_pct": 3.4
  },
  "database": {
    "total_bytes": 95587455459,
    "total_pretty": "89.02 GB",
    "tables": [
      {
        "name": "depth_updates",
        "size_bytes": 59742928896,
        "size_pretty": "56 GB",
        "rows_approx": 56933550,
        "from": "2026-03-01T11:59:39+00:00",
        "to": "2026-03-01T22:10:59+00:00"
      }
    ]
  },
  "compression": [
    {
      "table": "depth_updates",
      "before_bytes": 320000000000,
      "after_bytes": 19000000000,
      "ratio": 16.8
    }
  ],
  "growth": {
    "recording_days": 15.3,
    "db_growth_gb_per_day": 5.82,
    "est_days_until_full": 576
  }
}
```

| Sección | Campo | Tipo | Descripción |
|---|---|---|---|
| `disk` | `path` | string | Ruta del disco de datos |
| `disk` | `total_bytes` | int | Capacidad total del disco |
| `disk` | `used_bytes` | int | Espacio usado |
| `disk` | `free_bytes` | int | Espacio libre |
| `disk` | `used_pct` | float | Porcentaje de uso del disco |
| `database` | `total_bytes` | int | Tamaño total de la DB |
| `database.tables[]` | `name` | string | Nombre de la hypertable |
| `database.tables[]` | `size_bytes` | int | Tamaño de la tabla |
| `database.tables[]` | `rows_approx` | int | Conteo aproximado de filas |
| `database.tables[]` | `from` / `to` | string | Rango temporal de datos |
| `compression[]` | `ratio` | float | Ratio de compresión (antes/después) |
| `growth` | `recording_days` | float | Días de grabación acumulados |
| `growth` | `db_growth_gb_per_day` | float | Crecimiento promedio diario (GB) |
| `growth` | `est_days_until_full` | float | Días estimados hasta llenar el disco |

**Nota:** `growth` requiere al menos 1 día de datos de grabación para calcularse. `compression` aparece vacío si los chunks aún no fueron comprimidos (TimescaleDB comprime según la política configurada).

El campo `disk.path` puede configurarse con la variable de entorno `DATA_PATH`.

---

### `GET /trades/{symbol}`

Trades agregados para un símbolo específico.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading (ej: `BTCUSDT`) |

#### Query Parameters

`limit`, `start`, `end` — ver [Parámetros Comunes](#parámetros-comunes).

#### Response

```json
[
  {
    "time": "2025-03-15T14:30:01.123000+00:00",
    "id": 4850230481,
    "price": 84250.5,
    "qty": 0.012,
    "is_buyer_maker": false
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `time` | string | Timestamp ISO-8601 del trade |
| `id` | int | ID de trade agregado de Binance |
| `price` | float | Precio de ejecución |
| `qty` | float | Cantidad (en base asset) |
| `is_buyer_maker` | bool | `true` = sell (taker vendió), `false` = buy (taker compró) |

---

### `GET /depth/{symbol}`

Snapshots del order book (20 niveles bid/ask).

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T14:30:00.500000+00:00",
    "update_id": 5823947112,
    "bids": [[84250.1, 1.52], [84250.0, 3.21], "..."],
    "asks": [[84250.2, 0.89], [84250.3, 2.10], "..."]
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `time` | string | Timestamp del snapshot |
| `update_id` | int | ID de actualización de Binance |
| `bids` | array | Array de `[precio, cantidad]` — 20 niveles, mayor a menor |
| `asks` | array | Array de `[precio, cantidad]` — 20 niveles, menor a mayor |

---

### `GET /tickers/{symbol}`

Best bid/ask (book ticker) con spread calculado.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T14:30:00.100000+00:00",
    "bid_price": 84250.1,
    "bid_qty": 1.52,
    "ask_price": 84250.2,
    "ask_qty": 0.89,
    "spread": 0.1
  }
]
```

---

### `GET /marks/{symbol}`

Mark price, index price y funding rate en tiempo real.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T14:30:01+00:00",
    "mark_price": 84252.3,
    "index_price": 84251.8,
    "funding_rate": 0.0001,
    "next_funding": "2025-03-15T16:00:00+00:00"
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `mark_price` | float | Precio mark (usado para liquidaciones) |
| `index_price` | float | Precio índice (promedio de exchanges) |
| `funding_rate` | float | Tasa de funding (positivo = longs pagan a shorts) |
| `next_funding` | string | Próximo funding timestamp |

---

### `GET /oi/{symbol}`

Open Interest (interés abierto) para un símbolo.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T14:30:00+00:00",
    "contracts": 15823.4,
    "value": 1332948700.5
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `contracts` | float | OI en contratos (base asset) |
| `value` | float | OI en USDT |

---

### `GET /funding/{symbol}`

Historial de funding rates.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T16:00:00+00:00",
    "funding_rate": 0.0001
  }
]
```

Notas: Binance paga funding cada 8 horas (00:00, 08:00, 16:00 UTC).

---

### `GET /ohlcv/{symbol}`

Velas OHLCV construidas desde continuous aggregates de TimescaleDB.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `interval` | string | `1m` | Intervalo de la vela: `1m`, `1h`, `1d` |
| `limit` | int | 100 | Máximo registros |
| `start` | string | — | Inicio del rango |
| `end` | string | — | Fin del rango |

#### Response

```json
[
  {
    "time": "2025-03-15T14:30:00+00:00",
    "open": 84200.0,
    "high": 84310.5,
    "low": 84180.2,
    "close": 84250.5,
    "volume": 125.43,
    "volume_usdt": 10567000.0,
    "trades": 4521,
    "taker_buy_vol": 72.1,
    "taker_sell_vol": 53.33
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `time` | string | Timestamp del bucket (inicio de la vela) |
| `open` | float | Precio de apertura |
| `high` | float | Precio máximo |
| `low` | float | Precio mínimo |
| `close` | float | Precio de cierre |
| `volume` | float | Volumen en base asset |
| `volume_usdt` | float | Volumen en USDT |
| `trades` | int | Número de trades en la vela |
| `taker_buy_vol` | float | Volumen comprado por takers |
| `taker_sell_vol` | float | Volumen vendido por takers |

---

### `GET /liquidations`

Liquidaciones forzadas. Puede filtrarse por símbolo o consultar todas.

#### Query Parameters

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `symbol` | string | — | Filtrar por símbolo (opcional) |
| `limit` | int | 100 | Máximo registros |
| `start` | string | — | Inicio del rango |
| `end` | string | — | Fin del rango |

#### Response

```json
[
  {
    "time": "2025-03-15T14:28:30+00:00",
    "symbol": "ETHUSDT",
    "side": "SELL",
    "qty": 12.5,
    "bankruptcy_price": 3180.0,
    "avg_price": 3175.5,
    "filled_qty": 12.5,
    "status": "FILLED"
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `side` | string | `BUY` = liquidación de short, `SELL` = liquidación de long |
| `qty` | float | Cantidad original de la orden de liquidación |
| `bankruptcy_price` | float | Precio de bancarrota |
| `avg_price` | float | Precio promedio de ejecución |
| `filled_qty` | float | Cantidad ejecutada |
| `status` | string | Estado de la orden (`FILLED`, `PARTIALLY_FILLED`, etc.) |

---

### `GET /lsr/{symbol}`

Long/Short Ratio — proporción de posiciones long vs short (top traders y global).

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T14:25:00+00:00",
    "type": "top_account",
    "ratio": 1.85,
    "long_pct": 64.91,
    "short_pct": 35.09
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `type` | string | `top_account`, `top_position`, o `global` |
| `ratio` | float | Ratio long/short (>1 = más longs) |
| `long_pct` | float | Porcentaje de cuentas/posiciones long |
| `short_pct` | float | Porcentaje de cuentas/posiciones short |

---

### `GET /taker/{symbol}`

Volumen de compra/venta de takers y su ratio.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "time": "2025-03-15T14:25:00+00:00",
    "buy_sell_ratio": 1.12,
    "buy_vol": 5840000.0,
    "sell_vol": 5210000.0
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `buy_sell_ratio` | float | Ratio buy/sell (>1 = más compra agresiva) |
| `buy_vol` | float | Volumen de compra agresiva (USDT) |
| `sell_vol` | float | Volumen de venta agresiva (USDT) |

---

### `GET /spread/{symbol}`

Análisis estadístico del spread bid-ask sobre las últimas N muestras.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `limit` | int | 1000 | Número de muestras para el análisis |

**Nota:** Este endpoint no acepta `start`/`end`. Siempre analiza las últimas N muestras.

#### Response

```json
{
  "symbol": "BTCUSDT",
  "samples": 1000,
  "avg_spread": 0.1,
  "min_spread": 0.0,
  "max_spread": 0.3,
  "spread_pct": 0.000119
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `avg_spread` | float | Spread promedio (en precio) |
| `min_spread` | float | Spread mínimo observado |
| `max_spread` | float | Spread máximo observado |
| `spread_pct` | float | Spread promedio como % del bid price |

---

### `GET /snapshots/{symbol}`

Snapshots de la estrategia α_f Bifurcation Short para un símbolo.

#### Path Parameters

| Parámetro | Tipo | Descripción |
|---|---|---|
| `symbol` | string | Par de trading |

#### Query Parameters

`limit`, `start`, `end`

#### Response

```json
[
  {
    "timestamp": "2025-03-15T14:30:00+00:00",
    "symbol": "BTCUSDT",
    "score_total": 3.5,
    "c_fund": 1.0,
    "c_oi": 0.5,
    "c_price": 1.0,
    "c_taker": 0.5,
    "c_vol": 0.5,
    "energy_hours": 12.5,
    "exhaustion": 3,
    "mark_price": 84250.5,
    "funding_rate": 0.0005,
    "oi_value": 1332948700.5,
    "taker_buy_ratio": 54.2,
    "volume_ratio": 2.1,
    "price_change_12h": 3.5,
    "price_change_24h": 5.2,
    "sma_24h": 83100.0
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `score_total` | float | Score Ŝ total (0–5) |
| `c_fund` | float | Componente funding (0, 0.5, 1.0) |
| `c_oi` | float | Componente OI (0, 0.5, 1.0) |
| `c_price` | float | Componente precio (0, 0.5, 1.0) |
| `c_taker` | float | Componente taker (0, 0.5, 1.0) |
| `c_vol` | float | Componente volumen (0, 0.5, 1.0) |
| `energy_hours` | float | Horas acumuladas con score ≥ 3.0 |
| `exhaustion` | int | Conteo de señales de exhaustion |
| `mark_price` | float | Mark price al momento del snapshot |
| `funding_rate` | float | Funding rate vigente |
| `oi_value` | float | Open interest en USDT |
| `taker_buy_ratio` | float | % del volumen que es compra agresiva |
| `volume_ratio` | float | Ratio volumen actual vs promedio |
| `price_change_12h` | float | Cambio de precio en 12h (%) |
| `price_change_24h` | float | Cambio de precio en 24h (%) |
| `sma_24h` | float | SMA de 24 horas |

---

### `GET /vtrades`

Trades virtuales (paper trading) de la estrategia.

#### Query Parameters

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `status` | string | — | Filtrar por estado: `open` o `closed` |
| `symbol` | string | — | Filtrar por símbolo |
| `limit` | int | 100 | Máximo registros |

**Nota:** Este endpoint no acepta `start`/`end`.

#### Response

```json
[
  {
    "id": 15,
    "symbol": "SOLUSDT",
    "variant": "base",
    "status": "closed",
    "entry_time": "2025-03-14T08:15:00+00:00",
    "entry_price": 142.50,
    "exit_time": "2025-03-14T20:30:00+00:00",
    "exit_price": 138.20,
    "exit_reason": "tp",
    "leverage": 5,
    "position_size_usd": 1000,
    "pnl_usd": 150.88,
    "pnl_pct": 15.09,
    "hold_hours": 12.25,
    "mfe_pct": 18.5,
    "mae_pct": -1.2,
    "score_at_entry": 3.5,
    "energy_at_entry": 8.0,
    "exhaustion_at_entry": 3
  }
]
```

| Campo | Tipo | Descripción |
|---|---|---|
| `variant` | string | Variante de la estrategia: `conservative`, `base`, `aggressive`, `high_energy` |
| `status` | string | `open` o `closed` |
| `exit_reason` | string | `tp` (take profit), `sl` (stop loss), `timeout` (hold máximo), `null` si abierto |
| `leverage` | int | Leverage simulado |
| `pnl_usd` | float | Ganancia/pérdida en USD simulada |
| `mfe_pct` | float | Maximum Favorable Excursion (mejor PnL% durante el trade) |
| `mae_pct` | float | Maximum Adverse Excursion (peor PnL% durante el trade) |
| `score_at_entry` | float | Score Ŝ al momento de entrada |
| `energy_at_entry` | float | Horas de energía acumuladas al entrar |

---

### `GET /pnl`

Resumen agregado de performance de la estrategia (solo trades cerrados).

#### Response

```json
{
  "total_trades": 45,
  "winners": 28,
  "losers": 17,
  "win_rate_pct": 62.2,
  "total_pnl_usd": 3250.50,
  "best_trade_usd": 520.30,
  "worst_trade_usd": -280.10,
  "avg_hold_hours": 18.5,
  "open_trades": 3,
  "by_variant": {
    "conservative": { "count": 12, "win_rate_pct": 75.0, "pnl_usd": 1200.00 },
    "base": { "count": 18, "win_rate_pct": 61.1, "pnl_usd": 1400.50 },
    "aggressive": { "count": 10, "win_rate_pct": 50.0, "pnl_usd": 450.00 },
    "high_energy": { "count": 5, "win_rate_pct": 60.0, "pnl_usd": 200.00 }
  }
}
```

---

## Ejemplos de Integración

> **IP del servidor:** `190.31.60.204`  
> **Certificado TLS:** self-signed (usar `-k` en curl, `verify=False` en Python, `ssl=False` en aiohttp)  
> **Reemplazar** `TU_API_KEY` **con tu key real.**

---

### cURL

```bash
# ══════════════════════════════════════════════════════════════
#  Configuración — pegar en tu terminal
# ══════════════════════════════════════════════════════════════
export API="https://190.31.60.204"
export KEY="TU_API_KEY"

# ── Health check (sin auth) ──
curl -sk "$API/health"
# → {"status":"ok"}

# ── Stats generales ──
curl -sk -H "X-API-Key: $KEY" "$API/stats" | python3 -m json.tool

# ── Lista de todos los símbolos grabados ──
curl -sk -H "X-API-Key: $KEY" "$API/symbols" | python3 -m json.tool

# ── Monitoreo de almacenamiento (disco libre, crecimiento, días restantes) ──
curl -sk -H "X-API-Key: $KEY" "$API/storage" | python3 -m json.tool

# ── Últimos 50 trades de BTCUSDT ──
curl -sk -H "X-API-Key: $KEY" "$API/trades/BTCUSDT?limit=50" | python3 -m json.tool

# ── Velas OHLCV de 1 hora, últimas 24 horas ──
curl -sk -H "X-API-Key: $KEY" "$API/ohlcv/BTCUSDT?interval=1h&limit=24" | python3 -m json.tool

# ── Velas diarias de ETH ──
curl -sk -H "X-API-Key: $KEY" "$API/ohlcv/ETHUSDT?interval=1d&limit=30" | python3 -m json.tool

# ── Trades con filtro temporal (rango específico) ──
curl -sk -H "X-API-Key: $KEY" \
  "$API/trades/BTCUSDT?start=2026-03-01T00:00:00Z&end=2026-03-01T12:00:00Z&limit=5000" \
  | python3 -m json.tool

# ── Order book (depth L20) ──
curl -sk -H "X-API-Key: $KEY" "$API/depth/BTCUSDT?limit=5" | python3 -m json.tool

# ── Best bid/ask ──
curl -sk -H "X-API-Key: $KEY" "$API/tickers/BTCUSDT?limit=10" | python3 -m json.tool

# ── Mark price + funding rate ──
curl -sk -H "X-API-Key: $KEY" "$API/marks/BTCUSDT?limit=10" | python3 -m json.tool

# ── Open Interest ──
curl -sk -H "X-API-Key: $KEY" "$API/oi/ETHUSDT?limit=100" | python3 -m json.tool

# ── Historial de funding rates ──
curl -sk -H "X-API-Key: $KEY" "$API/funding/BTCUSDT?limit=50" | python3 -m json.tool

# ── Liquidaciones recientes (todos los pares) ──
curl -sk -H "X-API-Key: $KEY" "$API/liquidations?limit=50" | python3 -m json.tool

# ── Liquidaciones solo de ETHUSDT ──
curl -sk -H "X-API-Key: $KEY" "$API/liquidations?symbol=ETHUSDT&limit=20" | python3 -m json.tool

# ── Long/Short ratio (top accounts, positions, global) ──
curl -sk -H "X-API-Key: $KEY" "$API/lsr/BTCUSDT?limit=20" | python3 -m json.tool

# ── Taker buy/sell volume ──
curl -sk -H "X-API-Key: $KEY" "$API/taker/BTCUSDT?limit=20" | python3 -m json.tool

# ── Spread bid-ask (análisis estadístico) ──
curl -sk -H "X-API-Key: $KEY" "$API/spread/BTCUSDT?limit=1000" | python3 -m json.tool

# ── Strategy snapshots ──
curl -sk -H "X-API-Key: $KEY" "$API/snapshots/SOLUSDT?limit=10" | python3 -m json.tool

# ── Virtual trades (paper trading) ──
curl -sk -H "X-API-Key: $KEY" "$API/vtrades" | python3 -m json.tool
curl -sk -H "X-API-Key: $KEY" "$API/vtrades?status=closed" | python3 -m json.tool

# ── PnL de la estrategia ──
curl -sk -H "X-API-Key: $KEY" "$API/pnl" | python3 -m json.tool

# ── Tamaño de la DB por tabla ──
curl -sk -H "X-API-Key: $KEY" "$API/dbsize" | python3 -m json.tool

# ── Guardar trades a archivo JSON ──
curl -sk -H "X-API-Key: $KEY" \
  "$API/trades/BTCUSDT?start=2026-03-01T00:00:00Z&limit=10000" \
  -o btc_trades.json
```

---

### Python (requests)

```python
import requests

# ── Configuración ──
BASE_URL = "https://190.31.60.204"
API_KEY  = "TU_API_KEY"
HEADERS  = {"X-API-Key": API_KEY}

# Deshabilitar warnings de SSL self-signed
requests.packages.urllib3.disable_warnings()


def api_get(path: str, params: dict = None):
    """Helper para hacer GET a la API."""
    resp = requests.get(f"{BASE_URL}{path}", headers=HEADERS,
                        params=params, verify=False)
    resp.raise_for_status()
    return resp.json()


# ── Stats generales ──
stats = api_get("/stats")
print(f"DB Size: {stats['db_size_gb']} GB")
print(f"Symbols: {stats['symbols_count']}")
for table, info in stats.items():
    if isinstance(info, dict) and "rows_approx" in info:
        print(f"  {table}: ~{info['rows_approx']:,} rows ({info['from'][:10]} → {info['to'][:10]})")

# ── Monitoreo de almacenamiento ──
st = api_get("/storage")
disk = st["disk"]
print(f"\nDisco: {disk['used_pretty']} / {disk['total_pretty']} ({disk['used_pct']}% usado)")
print(f"Libre: {disk['free_pretty']}")
if st["growth"]:
    g = st["growth"]
    print(f"Crecimiento: {g['db_growth_gb_per_day']} GB/día")
    print(f"Días hasta llenar disco: ~{g['est_days_until_full']:.0f}")

# ── Últimos trades de BTCUSDT ──
trades = api_get("/trades/BTCUSDT", {"limit": 20})
for t in trades[:5]:
    side = "SELL" if t["is_buyer_maker"] else "BUY"
    print(f"  {t['time']}  {side}  {t['price']}  qty={t['qty']}")

# ── Velas OHLCV de 1 hora (última semana) ──
candles = api_get("/ohlcv/BTCUSDT", {"interval": "1h", "limit": 168})
for c in candles[:5]:
    print(f"  {c['time']}  O:{c['open']}  H:{c['high']}  L:{c['low']}  C:{c['close']}  V:{c['volume']}")

# ── Velas diarias de ETHUSDT ──
eth_daily = api_get("/ohlcv/ETHUSDT", {"interval": "1d", "limit": 30})
for c in eth_daily[:3]:
    print(f"  {c['time'][:10]}  close={c['close']}  vol_usdt={c['volume_usdt']:.0f}")

# ── Open Interest ──
oi = api_get("/oi/BTCUSDT", {"limit": 10})
for o in oi[:3]:
    print(f"  {o['time']}  contratos={o['contracts']}  valor_usd={o['value']:.0f}")

# ── Funding rates ──
funding = api_get("/funding/BTCUSDT", {"limit": 10})
for f in funding[:3]:
    print(f"  {f['time']}  rate={f['funding_rate']}")

# ── Liquidaciones recientes ──
liqs = api_get("/liquidations", {"limit": 20})
for l in liqs[:5]:
    print(f"  {l['time']}  {l['symbol']}  {l['side']}  qty={l['qty']}  price={l['avg_price']}")

# ── Long/Short ratio ──
lsr = api_get("/lsr/BTCUSDT", {"limit": 10})
for r in lsr[:3]:
    print(f"  {r['time']}  {r['type']}  ratio={r['ratio']}  L:{r['long_pct']}% S:{r['short_pct']}%")

# ── Spread analysis ──
spread = api_get("/spread/BTCUSDT", {"limit": 5000})
print(f"\nSpread BTCUSDT ({spread['samples']} muestras):")
print(f"  avg={spread['avg_spread']}  min={spread['min_spread']}  max={spread['max_spread']}")
print(f"  spread_pct={spread['spread_pct']:.6f}%")

# ── PnL de la estrategia ──
pnl = api_get("/pnl")
print(f"\nPnL: {pnl['total_pnl_usd']} USD | Win rate: {pnl['win_rate_pct']}% | Trades: {pnl['total_trades']}")
if pnl.get("by_variant"):
    for v, d in pnl["by_variant"].items():
        print(f"  {v}: {d['count']} trades, WR={d['win_rate_pct']}%, PnL={d['pnl_usd']} USD")
```

---

### Python (pandas) — análisis de datos

```python
import pandas as pd
import requests

BASE_URL = "https://190.31.60.204"
HEADERS  = {"X-API-Key": "TU_API_KEY"}
requests.packages.urllib3.disable_warnings()


def api_df(path: str, params: dict = None) -> pd.DataFrame:
    """Fetch endpoint y devolver DataFrame con index temporal."""
    resp = requests.get(f"{BASE_URL}{path}", headers=HEADERS,
                        params=params, verify=False)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)
    return df


# ── Trades → VWAP, volumen, distribución ──
df_trades = api_df("/trades/BTCUSDT", {"limit": 10000})
print(df_trades.describe())
print(f"\nTotal volume: {df_trades['qty'].sum():.4f} BTC")
print(f"VWAP: {(df_trades['price'] * df_trades['qty']).sum() / df_trades['qty'].sum():.2f}")
print(f"Buy %: {(~df_trades['is_buyer_maker']).mean()*100:.1f}%")

# ── OHLCV → velas de 1 hora como DataFrame ──
df_1h = api_df("/ohlcv/BTCUSDT", {"interval": "1h", "limit": 168})
print(f"\nOHLCV 1h — últimas {len(df_1h)} velas:")
print(df_1h[["open", "high", "low", "close", "volume"]].head(10))

# ── Exportar a CSV ──
df_1h.to_csv("btcusdt_1h.csv")
print("Guardado en btcusdt_1h.csv")

# ── OI + precio → correlación ──
df_oi = api_df("/oi/BTCUSDT", {"limit": 1000})
df_marks = api_df("/marks/BTCUSDT", {"limit": 1000})
print(f"\nOI últimas {len(df_oi)} muestras — rango: {df_oi['value'].min():.0f} – {df_oi['value'].max():.0f} USDT")

# ── Múltiples símbolos a la vez ──
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
for sym in symbols:
    df = api_df(f"/ohlcv/{sym}", {"interval": "1d", "limit": 7})
    if len(df) > 0:
        last = df.iloc[0]
        print(f"{sym}: close={last['close']}  vol_usdt={last['volume_usdt']:.0f}  trades={last['trades']}")
```

---

### Python (aiohttp — async)

```python
import aiohttp
import asyncio

BASE_URL = "https://190.31.60.204"
API_KEY  = "TU_API_KEY"


async def api_get(session: aiohttp.ClientSession, path: str, params: dict = None):
    async with session.get(
        f"{BASE_URL}{path}",
        headers={"X-API-Key": API_KEY},
        params=params,
        ssl=False,
    ) as resp:
        resp.raise_for_status()
        return await resp.json()


async def main():
    async with aiohttp.ClientSession() as s:
        # Fetch varias cosas en paralelo
        stats, symbols, btc_candles, eth_candles = await asyncio.gather(
            api_get(s, "/stats"),
            api_get(s, "/symbols"),
            api_get(s, "/ohlcv/BTCUSDT", {"interval": "1h", "limit": 24}),
            api_get(s, "/ohlcv/ETHUSDT", {"interval": "1h", "limit": 24}),
        )

        print(f"DB: {stats['db_size_gb']} GB | {stats['symbols_count']} symbols")
        print(f"Símbolos disponibles: {len(symbols)}")
        print(f"\nBTC últimas 3 velas 1h:")
        for c in btc_candles[:3]:
            print(f"  {c['time']}  close={c['close']}  vol={c['volume']}")
        print(f"\nETH últimas 3 velas 1h:")
        for c in eth_candles[:3]:
            print(f"  {c['time']}  close={c['close']}  vol={c['volume']}")

        # Fetch trades de múltiples símbolos en paralelo
        top_symbols = symbols[:10]
        results = await asyncio.gather(
            *[api_get(s, f"/trades/{sym}", {"limit": 1}) for sym in top_symbols]
        )
        for sym, data in zip(top_symbols, results):
            if data:
                print(f"  {sym}: last_price={data[0]['price']}  qty={data[0]['qty']}")


asyncio.run(main())
```

---

### JavaScript / Node.js (fetch)

```javascript
// Node.js 18+ (fetch nativo) o navegador
const BASE_URL = "https://190.31.60.204";
const API_KEY  = "TU_API_KEY";

// En Node.js con certificado self-signed:
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

async function apiGet(path, params = {}) {
  const url = new URL(path, BASE_URL);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const resp = await fetch(url, {
    headers: { "X-API-Key": API_KEY },
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

// ── Uso ──
async function main() {
  // Stats
  const stats = await apiGet("/stats");
  console.log(`DB: ${stats.db_size_gb} GB | Symbols: ${stats.symbols_count}`);

  // Velas OHLCV 1h
  const candles = await apiGet("/ohlcv/BTCUSDT", { interval: "1h", limit: 24 });
  console.log(`\nBTC 1h candles (${candles.length}):`);
  candles.slice(0, 5).forEach(c =>
    console.log(`  ${c.time}  O:${c.open} H:${c.high} L:${c.low} C:${c.close}`)
  );

  // Liquidaciones
  const liqs = await apiGet("/liquidations", { limit: 10 });
  console.log(`\nÚltimas liquidaciones:`);
  liqs.forEach(l =>
    console.log(`  ${l.time} ${l.symbol} ${l.side} qty=${l.qty} price=${l.avg_price}`)
  );

  // Storage / disco
  const storage = await apiGet("/storage");
  console.log(`\nDisco: ${storage.disk.used_pretty} / ${storage.disk.total_pretty} (${storage.disk.used_pct}%)`);
  console.log(`DB: ${storage.database.total_pretty}`);
  if (storage.growth.est_days_until_full) {
    console.log(`Días hasta llenar: ~${storage.growth.est_days_until_full}`);
  }
}

main().catch(console.error);
```

---

## Límites y Consideraciones

### Rate Limits

La API no implementa rate limiting propio. Sin embargo:

- Cada request crea una conexión al pool asyncpg (2–10 conexiones)
- Queries con rangos temporales grandes o `limit=10000` pueden ser lentas
- Se recomienda usar `start`/`end` para acotar rangos en tablas grandes (`depth_updates`, `agg_trades`, `book_tickers`)

### Límite de respuesta

- **Máximo 10,000 registros** por request (`MAX_LIMIT`)
- Default: 100 registros si no se especifica `limit`

### Tablas de alta frecuencia

Las siguientes tablas generan miles de registros por segundo. Para consultas eficientes, siempre especificar `start` y `end`:

| Tabla | Endpoint | Frecuencia aprox |
|---|---|---|
| `depth_updates` | `/depth/{symbol}` | ~10/s por símbolo |
| `book_tickers` | `/tickers/{symbol}` | ~20/s por símbolo |
| `agg_trades` | `/trades/{symbol}` | Variable, picos de 100+/s |

### CORS

La API permite requests desde cualquier origen (`allow_origins=*`), pero solo métodos GET.

### TLS

El servidor usa un certificado self-signed. Los clientes deben:
- Usar `-k` en curl
- Usar `verify=False` en Python requests
- Usar `ssl=False` en aiohttp
- Aceptar el certificado manualmente en navegadores

### Docs Interactivos (Swagger)

FastAPI provee documentación interactiva automáticamente:

- **Swagger UI:** `https://190.31.60.204/docs`
- **ReDoc:** `https://190.31.60.204/redoc`

Para autenticarte en Swagger UI: click en "Authorize" e ingresar tu API key.
