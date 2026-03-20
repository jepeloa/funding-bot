# Binance Futures USDT-M Recorder

Sistema de grabación en tiempo real de **todos** los pares de futuros perpetuos USDT-M de Binance, con motor de estrategia integrado (α_f Bifurcation Short — paper trading).

## Arquitectura General

```
                        Clientes externos (HTTPS)
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│              Caddy 2 (Docker, reverse proxy)                 │
│  • TLS self-signed en :443                                   │
│  • Security headers (HSTS, X-Frame-Options, nosniff)         │
│  • Reverse proxy → FastAPI (host:8008)                       │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│              api.py (FastAPI + uvicorn, systemd)              │
│  • 18 endpoints REST (autenticación API key)                 │
│  • asyncpg pool (2–10 conexiones)                            │
│  • Filtrado por rango temporal, paginación, límites          │
│  • Docs interactivos en /docs (Swagger UI)                   │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌─────────────────────────────────────────────────────────────┐
│                      recorder.py                            │
│                                                             │
│  ┌──────────────────────┐   ┌──────────────────────────┐    │
│  │  12 WebSocket Groups │   │    REST Pollers           │    │
│  │  (47 sym × 4 streams)│   │  • OI        (cada 15s)  │    │
│  │  • @depth@20@100ms   │   │  • Funding   (cada 5min) │    │
│  │  • @aggTrade         │   │  • L/S Ratio (cada 5min) │    │
│  │  • @bookTicker       │   │  • Taker Vol (cada 5min) │    │
│  │  • @markPrice@1s     │   └──────────┬───────────────┘    │
│  └──────────┬───────────┘              │                    │
│  ┌──────────┴───────────┐              │                    │
│  │  1 WS Liquidaciones  │              │                    │
│  │  !forceOrder@arr     │              │                    │
│  └──────────┬───────────┘              │                    │
│             │                          │                    │
│             ▼                          ▼                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              handle_message() → buffers              │   │
│  │              strategy.evaluate()                     │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                   │
│  ┌──────────────────────┴───────────────────────────────┐   │
│  │  Supervisor (monitorea y reinicia TODAS las tareas)  │   │
│  │  Watchdog WS (detecta stall global, fuerza reconex.) │   │
│  │  Watchdog systemd (NOTIFY + WATCHDOG=1 cada 120s)    │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                     db.py (AsyncDBWriter)                     │
│  • 10 buffers en memoria (batch insert)                      │
│  • COPY protocol (bulk insert eficiente)                     │
│  • Flush periódico cada 1s / BATCH_SIZE=1000                 │
│  • Protección OOM: cap 100K registros por buffer             │
│  • Flush aislado por tabla (un fallo no cascadea)            │
│  • Backoff exponencial si la DB no responde                  │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                TimescaleDB (Docker, pg16)                     │
│  • 10 hypertables con compresión columnar automática         │
│  • 3 continuous aggregates (OHLCV 1min, 1h, 1d)             │
│  • Disco dedicado 4TB HDD                                    │
│  • Tuned: shared_buffers=8GB, effective_cache_size=28GB      │
└──────────────────────────────────────────────────────────────┘
```

## Datos Grabados

### WebSocket (tiempo real)

| Tabla | Stream | Frecuencia | Descripción |
|---|---|---|---|
| `depth_updates` | `@depth@20@100ms` | ~10/s por símbolo | Book parcial: 20 niveles bid/ask |
| `agg_trades` | `@aggTrade` | Variable | Trades agregados (precio, qty, side) |
| `book_tickers` | `@bookTicker` | ~20/s por símbolo | Mejor bid/ask en tiempo real |
| `mark_prices` | `@markPrice@1s` | 1/s por símbolo | Mark price, index, funding rate |
| `liquidations` | `!forceOrder@arr` | Variable | Liquidaciones forzadas (todos los símbolos) |

### REST Polling

| Tabla | Endpoint | Intervalo | Descripción |
|---|---|---|---|
| `open_interest` | `/fapi/v1/openInterest` | 15s | OI en contratos y USD (554 pares) |
| `funding_rates` | `/fapi/v1/fundingRate` | 5min | Historial de funding rates |
| `long_short_ratio` | `/futures/data/*` | 5min | Ratio L/S top accounts, positions, global (top 50) |
| `taker_buy_sell` | `/futures/data/takerlongshortRatio` | 5min | Volumen taker compra/venta (top 50) |

### Estrategia

| Tabla | Tipo | Descripción |
|---|---|---|
| `strategy_snapshots` | Hypertable | Score Ŝ, energía E, exhaustion Ê, indicadores por símbolo |
| `virtual_trades` | Regular | Paper trades (entrada, salida, PnL, MFE/MAE) |
| `heartbeat` | Singleton | Crash recovery, contador de mensajes |

### Continuous Aggregates (auto-materializados)

| Vista | Fuente | Datos |
|---|---|---|
| `ohlcv_1m` | `agg_trades` | Velas OHLCV de 1 minuto |
| `ohlcv_1h` | `ohlcv_1m` | Velas OHLCV de 1 hora |
| `ohlcv_1d` | `ohlcv_1h` | Velas OHLCV diarias |

### Compresión

Todas las hypertables tienen compresión columnar nativa activada (ratios típicos 10x–17x):

| Tabla | Intervalo compresión | Ratio típico |
|---|---|---|
| `depth_updates` | 2h | ~17x |
| `agg_trades` | 2h | ~11x |
| `book_tickers` | 2h | ~11x |
| `mark_prices` | 4h | ~16x |
| `open_interest` | 4h | ~8x |
| `funding_rates` | 1d | ~3x |
| `strategy_snapshots` | 4h | ~9x |
| `liquidations` | 2h | variable |
| `long_short_ratio` | 2h | ~4x |
| `taker_buy_sell` | 2h | ~4x |

## Estrategia: α_f Bifurcation Short

La estrategia busca **shortear pumps agotados** en altcoins. Funciona como paper trading (sin capital real).

### Score Ŝ (0–5)

Suma de 5 componentes (cada uno 0, 0.5 o 1.0) que miden si el "spring" del precio está invertido:

| Componente | 0.5 | 1.0 |
|---|---|---|
| `c_fund` — Funding rate | r ≥ 0.01% | r ≥ 0.05% |
| `c_oi` — ΔOI 24h | ΔOI ≥ 2.5% | ΔOI ≥ 5% |
| `c_price` — ΔPrecio 12h | ΔP ≥ 1.5% | ΔP ≥ 3% |
| `c_taker` — Taker buy ratio | η ≥ 52% | η ≥ 55% |
| `c_vol` — Volume spike | V/V̄ ≥ 1.4× | V/V̄ ≥ 2× |

### Energía E

Horas acumuladas con Ŝ ≥ 3.0. Mide cuánto tiempo el spring lleva invertido.

### Exhaustion Ê

Indicadores de reversión (bifurcación). Se requiere Ê ≥ 2 para entrar.

### 4 Variantes Simultáneas

| Variante | Leverage | SL | TP | Hold máx | Condiciones entrada |
|---|---|---|---|---|---|
| **Conservative** | 3× | 3% | 10% | 36h | r≥0.015%, ΔOI≥8%, ΔP≥5%, V≥2× |
| **Base** | 5× | 5% | 15% | 48h | r≥0.01%, ΔOI≥5%, ΔP≥3%, V≥2× |
| **Aggressive** | 7× | 7% | 20% | 72h | r≥0.008%, ΔOI≥3%, ΔP≥2%, V≥2× |
| **High Energy** | 10× | 4% | 25% | 24h | r≥0.01%, ΔOI≥10%, ΔP≥5%, V≥3× |

Todas requieren Ŝ ≥ 2.5, Ê ≥ 2, E ≥ 6h, y precio > SMA_24h.

## Requisitos

- **OS**: Linux (Ubuntu 22.04+)
- **Python**: 3.12+
- **Docker**: con Docker Compose v2
- **Hardware mínimo**: 4 cores, 16GB RAM, disco dedicado para datos
- **Hardware recomendado**: i3-10105 o superior, 32–40GB RAM, HDD/SSD ≥1TB

### Dependencias Python

```
websockets
aiohttp
asyncpg
psycopg2-binary
fastapi
uvicorn[standard]
```

## Instalación

### 1. Clonar y preparar entorno

```bash
git clone <repo-url> ~/websocket_recorder
cd ~/websocket_recorder
python3 -m venv .venv
source .venv/bin/activate
pip install websockets aiohttp asyncpg psycopg2-binary fastapi 'uvicorn[standard]'
```

### 2. Configurar credenciales

```bash
# Crear archivo .env con contraseña de la DB y API key
echo "DB_PASSWORD=$(openssl rand -base64 24)" > .env
echo "API_PORT=8008" >> .env
echo "API_KEYS=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')" >> .env
```

### 3. Levantar TimescaleDB

```bash
docker compose up -d --wait
```

### 4. Aplicar migraciones

```bash
for f in migrations/00*.sql; do
    docker exec -i binance-timescaledb \
        psql -U recorder -d binance_futures < "$f"
done
```

### 5. Instalar servicios systemd

```bash
# Recorder (grabación de datos)
sudo cp systemd/ws-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ws-recorder.service

# REST API
sudo cp systemd/ws-recorder-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ws-recorder-api.service
```

### 6. Configurar HTTPS (Caddy reverse proxy)

```bash
# Generar certificado self-signed (10 años)
mkdir -p caddy/certs
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout caddy/certs/server.key -out caddy/certs/server.crt \
    -days 3650 -subj '/CN=recorder-api' \
    -addext 'subjectAltName=IP:0.0.0.0,DNS:localhost'

# Levantar Caddy (se incluye en docker-compose.yml)
docker compose up -d caddy
```

Acceso: `https://190.31.60.204/health`

## Estructura de Archivos

```
websocket_recorder/
├── recorder.py              # Proceso principal: WS listeners, REST pollers, supervisor
├── db.py                    # AsyncDBWriter: buffers, batch COPY, flush, reconexión
├── strategy.py              # Motor α_f Bifurcation Short (4 variantes)
├── config.py                # Toda la configuración (DB, streams, estrategia)
├── query.py                 # CLI para consultar datos grabados
├── api.py                   # REST API (FastAPI) para acceso remoto
├── check_symbols.py         # Verificación de símbolos activos
├── peek_ws.py               # Utilidad para inspeccionar WS en vivo
├── docker-compose.yml       # TimescaleDB + Caddy reverse proxy (Docker)
├── .env                     # Credenciales y API keys (no versionado)
├── caddy/
│   ├── Caddyfile            # Config Caddy: HTTPS reverse proxy → FastAPI
│   └── certs/               # Certificado TLS self-signed (10 años)
├── migrations/
│   ├── 001_schema.sql       # Tablas, hypertables, índices, compresión
│   ├── 002_continuous_aggregates.sql  # OHLCV 1m/1h/1d
│   └── 003_new_streams.sql  # Liquidaciones, L/S ratio, taker volume
├── systemd/
│   ├── ws-recorder.service  # Unit: recorder con watchdog, auto-restart, OOM protection
│   └── ws-recorder-api.service  # Unit: FastAPI REST API
└── docs/
    └── API.md               # Documentación detallada de la REST API
```

## Uso

### Verificar estado de los servicios

```bash
# Recorder
sudo systemctl status ws-recorder.service
sudo journalctl -u ws-recorder -f          # logs en vivo

# REST API
sudo systemctl status ws-recorder-api.service
sudo journalctl -u ws-recorder-api -f
```

### REST API (acceso remoto)

Todos los datos son accesibles vía HTTPS desde cualquier lugar con autenticación por API key.  
Documentación completa: [`docs/API.md`](docs/API.md)  
Swagger UI interactivo: `https://190.31.60.204/docs`

```bash
# ── Configurar variables ──
export API_URL="https://190.31.60.204"
export API_KEY="TU_API_KEY"

# Health check (sin auth)
curl -sk "$API_URL/health"
# → {"status": "ok"}

# Stats generales (registros, tamaño DB, símbolos)
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/stats" | python3 -m json.tool

# Monitoreo de almacenamiento (disco, crecimiento, días restantes)
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/storage" | python3 -m json.tool

# Lista de símbolos grabados
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/symbols"

# Últimos 50 trades de BTCUSDT
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/trades/BTCUSDT?limit=50"

# Velas OHLCV 1h (últimas 24 horas)
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/ohlcv/BTCUSDT?interval=1h&limit=24"

# Trades con filtro temporal
curl -sk -H "X-API-Key: $API_KEY" \
  "$API_URL/trades/BTCUSDT?start=2026-03-01T00:00:00Z&end=2026-03-01T12:00:00Z&limit=5000"

# Open Interest de ETH
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/oi/ETHUSDT?limit=100"

# Liquidaciones recientes (todos los pares)
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/liquidations?limit=50"

# PnL de la estrategia
curl -sk -H "X-API-Key: $API_KEY" "$API_URL/pnl"
```

#### Ejemplo rápido con Python

```python
import requests
requests.packages.urllib3.disable_warnings()

API = "https://190.31.60.204"
KEY = {"X-API-Key": "TU_API_KEY"}

# Velas de 1 hora de BTC
candles = requests.get(f"{API}/ohlcv/BTCUSDT",
                       headers=KEY, params={"interval": "1h", "limit": 24},
                       verify=False).json()
for c in candles[:3]:
    print(f"{c['time']}  O:{c['open']}  H:{c['high']}  L:{c['low']}  C:{c['close']}")
```

**Endpoints disponibles:**

| Método | Ruta | Auth | Descripción |
|---|---|---|---|
| GET | `/health` | No | Estado y uptime |
| GET | `/stats` | Sí | Conteo de registros por tabla |
| GET | `/symbols` | Sí | Lista de símbolos grabados |
| GET | `/dbsize` | Sí | Tamaño por hypertable |
| GET | `/storage` | Sí | Monitor: disco, DB, compresión, crecimiento, días restantes |
| GET | `/trades/{symbol}` | Sí | Trades agregados |
| GET | `/depth/{symbol}` | Sí | Snapshots order book |
| GET | `/tickers/{symbol}` | Sí | Best bid/ask |
| GET | `/marks/{symbol}` | Sí | Mark price + funding |
| GET | `/oi/{symbol}` | Sí | Open interest |
| GET | `/funding/{symbol}` | Sí | Historial funding rates |
| GET | `/ohlcv/{symbol}` | Sí | Velas OHLCV (1m/1h/1d) |
| GET | `/liquidations` | Sí | Liquidaciones forzadas |
| GET | `/lsr/{symbol}` | Sí | Long/Short ratio |
| GET | `/taker/{symbol}` | Sí | Taker buy/sell volume |
| GET | `/snapshots/{symbol}` | Sí | Strategy snapshots |
| GET | `/vtrades` | Sí | Virtual trades |
| GET | `/pnl` | Sí | PnL por variante |
| GET | `/spread/{symbol}` | Sí | Spread bid-ask |

### Consultar datos (query.py)

```bash
source .venv/bin/activate
set -a && source .env && set +a  # cargar credenciales

# Resumen general
python3 query.py stats

# Últimos trades de BTCUSDT
python3 query.py trades BTCUSDT --limit 20

# Depth (order book)
python3 query.py depth BTCUSDT --limit 10

# Book tickers (best bid/ask)
python3 query.py tickers ETHUSDT --limit 10

# Mark price + funding
python3 query.py marks BTCUSDT --limit 10

# Open interest
python3 query.py oi BTCUSDT --limit 10

# Velas OHLCV
python3 query.py ohlcv BTCUSDT 1h

# Spread bid-ask
python3 query.py spread BTCUSDT

# Liquidaciones
python3 query.py liq --limit 20
python3 query.py liq BTCUSDT --limit 10

# Long/Short ratio
python3 query.py lsr BTCUSDT --limit 10

# Taker buy/sell volume
python3 query.py taker BTCUSDT --limit 10

# Strategy snapshots
python3 query.py snapshots BTCUSDT --limit 10

# Virtual trades y PnL
python3 query.py vtrades
python3 query.py vtrades --status closed
python3 query.py pnl

# Símbolos grabados
python3 query.py symbols

# Tamaño por hypertable
python3 query.py dbsize

# Exportar a CSV
python3 query.py export agg_trades trades.csv
```

## Robustez

El sistema está diseñado para funcionar 24/7 sin intervención humana:

### Nivel Aplicación (recorder.py)

| Mecanismo | Descripción |
|---|---|
| **Per-WS recv timeout** | Si un WS no envía datos en 60s (3600s para liquidaciones), se reconecta |
| **Ping/Pong** | `ping_interval=20s, ping_timeout=10s` — detecta conexiones muertas |
| **Backoff estable** | Solo resetea reintentos si la conexión duró >60s (evita cycling rápido) |
| **Backoff exponencial** | 3s → 6s → 12s → ... → 120s máximo entre reconexiones |
| **WS Watchdog** | Si NINGÚN WS envía datos en 120s, cancela todos y fuerza reconexión |
| **Supervisor genérico** | Monitorea las 24 tareas (WS, pollers, flush, etc.) y reinicia cualquiera que muera |
| **sd_notify** | Envía `READY=1` al arrancar y `WATCHDOG=1` cada 120s a systemd |
| **Exception handler** | Captura excepciones no manejadas en el event loop |

### Nivel DB (db.py)

| Mecanismo | Descripción |
|---|---|
| **Buffer cap** | Si un buffer supera 100K registros, descarta el 25% más antiguo (previene OOM) |
| **Flush aislado** | Cada tabla se flushea independientemente — un fallo no afecta a las demás |
| **Flush backoff** | Si la DB falla, aplica backoff exponencial (10s–60s) antes de reintentar |
| **Connect retry** | 10 intentos con backoff creciente (3s–30s) al conectar |
| **Safe close** | Flush con timeout de 15s + acquire con timeout de 5s — nunca se cuelga |

### Nivel OS (systemd)

| Directiva | Valor | Efecto |
|---|---|---|
| `Type=notify` | — | El servicio solo está "listo" cuando Python confirma `READY=1` |
| `WatchdogSec=300` | 5min | Si no recibe señal de vida, systemd mata y reinicia el proceso |
| `Restart=always` | — | Reinicia automáticamente ante cualquier fallo |
| `RestartSec=5` | 5s | Espera 5s entre reinicios |
| `MemoryMax=8G` | — | Si excede 8GB, systemd lo reinicia limpio (mejor que OOM killer) |
| `MemoryHigh=6G` | — | A partir de 6GB, el kernel throttlea la memoria |
| `OOMScoreAdjust=-500` | — | El kernel prefiere matar otros procesos antes que este |
| `StartLimitBurst=20` | — | Hasta 20 reinicios en 10 minutos antes de dar up |

### Nivel Infraestructura (Docker)

| Configuración | Descripción |
|---|---|
| `restart: unless-stopped` | TimescaleDB se reinicia automáticamente |
| `healthcheck` | `pg_isready` cada 10s, el recorder espera a que pase antes de arrancar |
| `memory: 16G` | Límite de memoria para el contenedor |
| `shm_size: 1g` | Shared memory para PG |

## Configuración

Toda la configuración está en `config.py`. Variables de entorno soportadas:

| Variable | Default | Descripción |
|---|---|---|
| `DB_HOST` | `localhost` | Host de TimescaleDB |
| `DB_PORT` | `5432` | Puerto |
| `DB_NAME` | `binance_futures` | Nombre de la base de datos |
| `DB_USER` | `recorder` | Usuario PostgreSQL |
| `DB_PASSWORD` | `recorder` | Contraseña (usar .env) |
| `API_HOST` | `0.0.0.0` | Dirección de bind de la API |
| `API_PORT` | `8000` | Puerto HTTP de la API |
| `API_KEYS` | *(auto-gen)* | API keys separadas por coma |

### Tunables principales (config.py)

| Parámetro | Valor | Descripción |
|---|---|---|
| `MAX_STREAMS_PER_WS` | 190 | Streams por conexión WS (límite Binance: 200) |
| `DEPTH_LEVELS` | 20 | Niveles de profundidad del order book |
| `DEPTH_SPEED` | 100ms | Frecuencia de updates del order book |
| `BATCH_SIZE` | 1000 | Registros por batch insert |
| `DB_POOL_MIN/MAX` | 5/20 | Pool de conexiones asyncpg |
| `RECONNECT_DELAY_SECS` | 3 | Delay base para reconexión |
| `OI_POLL_INTERVAL_SECS` | 15 | Intervalo de polling de Open Interest |
| `SENTIMENT_TOP_N` | 50 | Top N símbolos para L/S ratio y taker volume |

## Mantenimiento

### Borrar datos y reiniciar limpio

```bash
# Parar recorder
sudo systemctl stop ws-recorder.service

# Borrar y recrear DB
docker exec binance-timescaledb psql -U recorder -d postgres \
    -c "DROP DATABASE IF EXISTS binance_futures;"
docker exec binance-timescaledb psql -U recorder -d postgres \
    -c "CREATE DATABASE binance_futures OWNER recorder;"
docker exec binance-timescaledb psql -U recorder -d binance_futures \
    -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# Reaplicar migraciones
for f in migrations/00*.sql; do
    docker exec -i binance-timescaledb \
        psql -U recorder -d binance_futures < "$f"
done

# Arrancar
sudo systemctl start ws-recorder.service
```

### Backup de datos

```bash
# Dump completo
docker exec binance-timescaledb pg_dump -U recorder binance_futures \
    | gzip > backup_$(date +%Y%m%d).sql.gz

# Solo schema
docker exec binance-timescaledb pg_dump -U recorder --schema-only binance_futures \
    > schema_backup.sql
```

### Logs

```bash
# Últimos logs del recorder
sudo journalctl -u ws-recorder -n 100

# Logs del recorder en vivo
sudo journalctl -u ws-recorder -f

# Logs de la API en vivo
sudo journalctl -u ws-recorder-api -f

# Solo errores (recorder)
sudo journalctl -u ws-recorder -p err

# Solo errores (API)
sudo journalctl -u ws-recorder-api -p err

# Logs de hoy
sudo journalctl -u ws-recorder --since today
sudo journalctl -u ws-recorder-api --since today
```

## Throughput Típico

Con 554 pares USDT-M (marzo 2026):

| Métrica | Valor |
|---|---|
| Streams totales | 2,216 market + 1 liquidaciones |
| Conexiones WS | 12 market + 1 liquidaciones |
| Mensajes/segundo | ~5,000–8,000 |
| Tareas activas | 24 |
| Crecimiento DB | ~6 GB/día (comprimido) |
| RAM proceso recorder | ~50–200 MB |
| RAM proceso API | ~30–80 MB |

## Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| `ws-recorder` (systemd) | — | Grabación de datos (WS + REST pollers) |
| `ws-recorder-api` (systemd) | 8008 | REST API (FastAPI + uvicorn) |
| `binance-timescaledb` (Docker) | 5432 | TimescaleDB pg16 |
| `recorder-caddy` (Docker) | 443 | HTTPS reverse proxy → API |
