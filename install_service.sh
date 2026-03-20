#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  Instalador del servicio systemd para ws-recorder
#  Ejecutar con: sudo bash install_service.sh
# ═══════════════════════════════════════════════════════════════

set -e

SERVICE_NAME="ws-recorder"
SERVICE_FILE="$(dirname "$0")/systemd/ws-recorder.service"
SYSTEMD_DIR="/etc/systemd/system"

echo "═══════════════════════════════════════════════════"
echo "  Instalando servicio: $SERVICE_NAME"
echo "═══════════════════════════════════════════════════"

# Verificar que se ejecuta como root
if [ "$EUID" -ne 0 ]; then
    echo "❌ Ejecutar como root: sudo bash $0"
    exit 1
fi

# Verificar que existe el archivo de servicio
if [ ! -f "$SERVICE_FILE" ]; then
    echo "❌ No se encuentra $SERVICE_FILE"
    exit 1
fi

# Parar servicio si ya existe
if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
    echo "⏹  Parando servicio existente..."
    systemctl stop "$SERVICE_NAME"
fi

# Copiar archivo de servicio
echo "📋 Copiando $SERVICE_FILE → $SYSTEMD_DIR/"
cp "$SERVICE_FILE" "$SYSTEMD_DIR/"

# Recargar systemd
echo "🔄 Recargando systemd..."
systemctl daemon-reload

# Habilitar (arrancar en boot)
echo "✅ Habilitando servicio (auto-start en boot)..."
systemctl enable "$SERVICE_NAME"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✓ Servicio instalado correctamente"
echo "═══════════════════════════════════════════════════"
echo ""
echo "Comandos útiles:"
echo "  sudo systemctl start $SERVICE_NAME     # Arrancar"
echo "  sudo systemctl stop $SERVICE_NAME      # Parar"
echo "  sudo systemctl restart $SERVICE_NAME   # Reiniciar"
echo "  sudo systemctl status $SERVICE_NAME    # Ver estado"
echo "  journalctl -u $SERVICE_NAME -f         # Ver logs en vivo"
echo "  journalctl -u $SERVICE_NAME --since '1 hour ago'  # Logs última hora"
echo ""
echo "Para arrancarlo ahora:"
echo "  sudo systemctl start $SERVICE_NAME"
