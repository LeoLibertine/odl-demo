#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Crear conexión Kafka en Atlas Stream Processing
# ═══════════════════════════════════════════════════════════════
#
# Credenciales por variables de entorno (NADA hardcodeado):
#   ATLAS_PUBLIC_KEY   — API key pública de Atlas
#   ATLAS_PRIVATE_KEY  — API key privada de Atlas
#   ATLAS_PROJECT_ID   — Group/Project ID de Atlas
#   SP_INSTANCE_NAME   — Nombre del SP instance (ej: "spinstance")
#   KAFKA_BOOTSTRAP    — bootstrap servers de tu Kafka/MSK
#   KAFKA_USER         — usuario SASL
#   KAFKA_PASSWORD     — password SASL

set -euo pipefail

: "${ATLAS_PUBLIC_KEY:?Set ATLAS_PUBLIC_KEY}"
: "${ATLAS_PRIVATE_KEY:?Set ATLAS_PRIVATE_KEY}"
: "${ATLAS_PROJECT_ID:?Set ATLAS_PROJECT_ID}"
: "${SP_INSTANCE_NAME:?Set SP_INSTANCE_NAME}"
: "${KAFKA_BOOTSTRAP:?Set KAFKA_BOOTSTRAP}"
: "${KAFKA_USER:?Set KAFKA_USER}"
: "${KAFKA_PASSWORD:?Set KAFKA_PASSWORD}"

API_BASE="https://cloud.mongodb.com/api/atlas/v2"

echo "=== Creando conexión Kafka 'msk-bancolombia' ==="

curl -s -w "\nHTTP %{http_code}\n" \
  --user "${ATLAS_PUBLIC_KEY}:${ATLAS_PRIVATE_KEY}" \
  --digest \
  -X POST "${API_BASE}/groups/${ATLAS_PROJECT_ID}/streams/${SP_INSTANCE_NAME}/connections" \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.atlas.2023-02-01+json" \
  -d "{
    \"name\": \"msk-bancolombia\",
    \"type\": \"Kafka\",
    \"bootstrapServers\": \"${KAFKA_BOOTSTRAP}\",
    \"security\": { \"protocol\": \"SASL_SSL\" },
    \"authentication\": {
      \"mechanism\": \"SCRAM-SHA-512\",
      \"username\": \"${KAFKA_USER}\",
      \"password\": \"${KAFKA_PASSWORD}\"
    }
  }"

echo ""
echo "=== Verificando conexiones existentes ==="

curl -s \
  --user "${ATLAS_PUBLIC_KEY}:${ATLAS_PRIVATE_KEY}" \
  --digest \
  -X GET "${API_BASE}/groups/${ATLAS_PROJECT_ID}/streams/${SP_INSTANCE_NAME}/connections" \
  -H "Accept: application/vnd.atlas.2023-02-01+json" | python3 -m json.tool

echo ""
echo "=== Done ==="
