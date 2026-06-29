#!/bin/bash
# ============================================================================
# Deploy Simulador iSeries al EC2 - TFW Bancolombia
# ============================================================================
# Uso:
#   chmod +x deploy.sh
#   ./deploy.sh
#
# Requisitos:
#   - SSH key configurada para el EC2
#   - EC2 instance corriendo (leo-alarcon-tfw)
# ============================================================================

EC2_HOST="44.201.147.154"
EC2_USER="ubuntu"
SSH_KEY="~/.ssh/id_rsa"  # Ajusta si tu key tiene otro nombre
REMOTE_DIR="/home/ubuntu/iseries-simulator"

echo "════════════════════════════════════════════════════════════"
echo "  🚀 DEPLOY SIMULADOR iSeries → EC2 (leo-alarcon-tfw)"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  EC2: ${EC2_HOST}"
echo "  Dir: ${REMOTE_DIR}"
echo ""

# 1. Crear directorio remoto
echo "📁 Creando estructura de directorios..."
ssh -i ${SSH_KEY} ${EC2_USER}@${EC2_HOST} "mkdir -p ${REMOTE_DIR}/{config,generator/models,setup}"

# 2. Subir archivos
echo "📤 Subiendo archivos..."

# Config
scp -i ${SSH_KEY} config/__init__.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/config/
scp -i ${SSH_KEY} config/settings.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/config/

# Generator
scp -i ${SSH_KEY} generator/__init__.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/generator/
scp -i ${SSH_KEY} generator/account_pool.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/generator/
scp -i ${SSH_KEY} generator/fragmenter.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/generator/
scp -i ${SSH_KEY} generator/kafka_producer.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/generator/

# Models
scp -i ${SSH_KEY} generator/models/__init__.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/generator/models/
scp -i ${SSH_KEY} generator/models/sciffmrcmv.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/generator/models/

# Setup
scp -i ${SSH_KEY} setup/setup_maestro_cuentas.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/setup/

# Root files
scp -i ${SSH_KEY} main.py ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/
scp -i ${SSH_KEY} requirements.txt ${EC2_USER}@${EC2_HOST}:${REMOTE_DIR}/

# 3. Instalar dependencias en el EC2
echo ""
echo "📦 Instalando dependencias..."
ssh -i ${SSH_KEY} ${EC2_USER}@${EC2_HOST} << 'REMOTE_SCRIPT'
cd /home/ubuntu/iseries-simulator

# Instalar Python 3.11 si no existe
if ! command -v python3.11 &> /dev/null; then
    echo "⚙️  Instalando Python 3.11..."
    sudo add-apt-repository -y ppa:deadsnakes/ppa
    sudo apt-get update
    sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
fi

# Instalar pip para 3.11
if ! python3.11 -m pip --version &> /dev/null; then
    echo "⚙️  Instalando pip..."
    sudo apt-get install -y python3-pip
fi

# Instalar dependencias
echo "📦 Instalando paquetes Python..."
python3.11 -m pip install --user -r requirements.txt

# Verificar configuración
echo ""
echo "🔍 Verificando configuración..."
cd /home/ubuntu/iseries-simulator
python3.11 -c "from config.settings import print_config_status; print_config_status()"

echo ""
echo "✅ Deploy completado!"
echo ""
echo "Comandos disponibles:"
echo "  cd ${REMOTE_DIR}"
echo ""
echo "  # Verificar config"
echo "  python3.11 -c 'from config.settings import print_config_status; print_config_status()'"
echo ""
echo "  # Setup cuentas (si no existe)"
echo "  cd setup && python3.11 setup_maestro_cuentas.py --cuentas 1000000 && cd .."
echo ""
echo "  # Demo rápido"
echo "  python3.11 main.py --tps 500 --duration 60"
echo ""
echo "  # Demo TFW"
echo "  python3.11 main.py --scenario demo-quick"
echo ""
echo "  # Burst test completo"
echo "  python3.11 main.py --scenario burst-test"
REMOTE_SCRIPT

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  ✅ DEPLOY COMPLETADO"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  SSH: ssh -i ${SSH_KEY} ${EC2_USER}@${EC2_HOST}"
echo "  Dir: cd ${REMOTE_DIR}"
echo ""
