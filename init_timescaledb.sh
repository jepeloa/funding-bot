#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  Inicialización de TimescaleDB para ws-recorder
#  Ejecutar con: bash init_timescaledb.sh
# ═══════════════════════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
ENV_FILE="$SCRIPT_DIR/.env"

# Cargar variables de entorno si existe .env
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-binance_futures}"
DB_USER="${DB_USER:-recorder}"
DB_PASSWORD="${DB_PASSWORD:-recorder}"

echo "═══════════════════════════════════════════════════"
echo "  TimescaleDB — Inicialización"
echo "═══════════════════════════════════════════════════"

# ── Crear directorio de datos si no existe ──
DATA_DIR="/media/mapplics-ia/recorder-data/timescaledb"
if [ ! -d "$DATA_DIR" ]; then
    echo "📁 Creando directorio de datos: $DATA_DIR"
    sudo mkdir -p "$DATA_DIR"
    sudo chown 999:999 "$DATA_DIR"  # UID de postgres en el container
fi

# ── Levantar container ──
echo "🐳 Levantando TimescaleDB..."
cd "$SCRIPT_DIR"
docker compose up -d

# ── Esperar a que esté healthy ──
echo "⏳ Esperando a que TimescaleDB esté listo..."
RETRIES=30
for i in $(seq 1 $RETRIES); do
    if docker compose exec -T timescaledb pg_isready -U "$DB_USER" -d "$DB_NAME" > /dev/null 2>&1; then
        echo "✅ TimescaleDB listo (intento $i)"
        break
    fi
    if [ "$i" -eq "$RETRIES" ]; then
        echo "❌ TimescaleDB no respondió después de $RETRIES intentos"
        echo "   Revisar logs: docker compose logs timescaledb"
        exit 1
    fi
    sleep 2
done

# ── Helper: ejecutar SQL via docker exec ──
run_psql() {
    docker exec -i binance-timescaledb psql -U "$DB_USER" -d "$DB_NAME" "$@"
}

# ── Ejecutar migraciones ──
echo ""
echo "📋 Ejecutando schema (001_schema.sql)..."
run_psql < "$SCRIPT_DIR/migrations/001_schema.sql"

echo ""
echo "📋 Ejecutando continuous aggregates (002_continuous_aggregates.sql)..."
run_psql < "$SCRIPT_DIR/migrations/002_continuous_aggregates.sql"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✓ TimescaleDB inicializado correctamente"
echo "═══════════════════════════════════════════════════"
echo ""
echo "  Host:     $DB_HOST:$DB_PORT"
echo "  Database: $DB_NAME"
echo "  User:     $DB_USER"
echo ""
echo "  Conectar: psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
echo ""
echo "  Verificar hypertables:"
echo "    SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;"
echo ""
echo "  Verificar compresión:"
echo "    SELECT * FROM timescaledb_information.compression_settings;"
echo ""
