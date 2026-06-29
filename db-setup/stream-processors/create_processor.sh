#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Crear Stream Processor via Atlas Admin API
# ═══════════════════════════════════════════════════════════════
#
# Requiere las mismas vars que create_connection.sh

set -euo pipefail

: "${ATLAS_PUBLIC_KEY:?Set ATLAS_PUBLIC_KEY}"
: "${ATLAS_PRIVATE_KEY:?Set ATLAS_PRIVATE_KEY}"
: "${ATLAS_PROJECT_ID:?Set ATLAS_PROJECT_ID}"
: "${SP_INSTANCE_NAME:?Set SP_INSTANCE_NAME}"

API_BASE="https://cloud.mongodb.com/api/atlas/v2"

echo "=== Creando Stream Processor 'bancolombia-assembler' ==="

curl -s -w "\nHTTP %{http_code}\n" \
  --user "${ATLAS_PUBLIC_KEY}:${ATLAS_PRIVATE_KEY}" \
  --digest \
  -X POST "${API_BASE}/groups/${ATLAS_PROJECT_ID}/streams/${SP_INSTANCE_NAME}/processor" \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.atlas.2024-05-30+json" \
  -d '{
    "name": "bancolombia-assembler",
    "pipeline": [
      {
        "$source": {
          "connectionName": "msk-bancolombia",
          "topic": "bancolombia.depositos.cdc.raw",
          "tsFieldName": "_produced_at"
        }
      },
      {
        "$match": {
          "correlation.key": { "$exists": true },
          "correlation.fragment_type": { "$exists": true },
          "after": { "$exists": true }
        }
      },
      {
        "$tumblingWindow": {
          "interval": { "size": 1, "unit": "second" },
          "allowedLateness": { "size": 1, "unit": "second" },
          "idleTimeout": { "size": 1, "unit": "second" },
          "pipeline": [
            {
              "$group": {
                "_id": "$correlation.key",
                "fragments": { "$push": "$$ROOT" },
                "fragment_count": { "$sum": 1 },
                "first_ts": { "$min": "$_produced_at" }
              }
            }
          ]
        }
      },
      {
        "$addFields": {
          "_h": {
            "$arrayElemAt": [
              { "$filter": { "input": "$fragments", "cond": { "$eq": ["$$this.correlation.fragment_type", "HEADER"] } } },
              0
            ]
          },
          "_m": {
            "$arrayElemAt": [
              { "$filter": { "input": "$fragments", "cond": { "$eq": ["$$this.correlation.fragment_type", "MONETARY"] } } },
              0
            ]
          },
          "_d": {
            "$arrayElemAt": [
              { "$filter": { "input": "$fragments", "cond": { "$eq": ["$$this.correlation.fragment_type", "METADATA"] } } },
              0
            ]
          }
        }
      },
      {
        "$match": {
          "fragment_count": 3,
          "_h": { "$ne": null },
          "_m": { "$ne": null },
          "_d": { "$ne": null }
        }
      },
      {
        "$project": {
          "_id": 0,
          "cuenta": {
            "numero": "$_h.after.NUMCTA",
            "tipo": "$_h.after.TIPCTA",
            "sucursal": "$_h.after.CODSUC"
          },
          "movimiento": {
            "tipo": "$_h.after.TIPMOV",
            "fecha_iso": "$$NOW",
            "hora": { "$concat": [
              { "$substrBytes": ["$_h.after.HORMOV", 0, 2] }, ":",
              { "$substrBytes": ["$_h.after.HORMOV", 2, 2] }, ":",
              { "$substrBytes": ["$_h.after.HORMOV", 4, 2] }
            ]},
            "canal": {
              "codigo": "$_h.after.CODCAN",
              "nombre": "$_h.after.CODCAN"
            },
            "estado": { "$switch": {
              "branches": [
                { "case": { "$eq": ["$_h.after.ESTTRN", "00"] }, "then": "APPROVED" },
                { "case": { "$eq": ["$_h.after.ESTTRN", "01"] }, "then": "PENDING" }
              ],
              "default": "REJECTED"
            }},
            "codtrn": "$_h.after.CODTRN",
            "secuencia": "$_h.after.SECMOV"
          },
          "monetario": {
            "valor": { "$toDouble": "$_m.after.VALTRA" },
            "moneda": "$_m.after.CODMON",
            "saldo_anterior": { "$toDouble": "$_m.after.SLDANT" },
            "saldo_nuevo": { "$toDouble": "$_m.after.SLDNUE" },
            "signo": "$_m.after.SIGNO",
            "gmf": { "$toDouble": { "$ifNull": ["$_m.after.VALGMF", 0] } }
          },
          "metadata": {
            "referencia": "$_d.after.NUMREF",
            "descripcion": "$_d.after.DESCRP",
            "cuenta_destino": "$_d.after.CTADES",
            "banco_destino": "$_d.after.BANDES",
            "nombre_destino": "$_d.after.NOMDES",
            "identificacion": "$_d.after.NUMIDE",
            "tipo_id": "$_d.after.TIPIDE",
            "ip_origen": "$_d.after.IPORIG",
            "user_agent": "$_d.after.USERAG",
            "dispositivo": "$_d.after.DISPOS",
            "geo": {
              "lat": { "$toDouble": { "$ifNull": ["$_d.after.LATGEO", 0] } },
              "lon": { "$toDouble": { "$ifNull": ["$_d.after.LONGGEO", 0] } }
            }
          },
          "_asp_metadata": {
            "correlation_key": "$_id",
            "assembly_timestamp": "$$NOW",
            "is_complete": true,
            "fragment_count": "$fragment_count",
            "source": "ASP_MSK",
            "first_fragment_at": "$first_ts"
          }
        }
      },
      {
        "$merge": {
          "into": {
            "connectionName": "StreamsAtlasConnection",
            "db": "bancolombia_odl",
            "coll": "movimientos"
          }
        }
      }
    ]
  }'

echo ""
echo "=== Iniciando processor ==="

curl -s -w "\nHTTP %{http_code}\n" \
  --user "${ATLAS_PUBLIC_KEY}:${ATLAS_PRIVATE_KEY}" \
  --digest \
  -X POST "${API_BASE}/groups/${ATLAS_PROJECT_ID}/streams/${SP_INSTANCE_NAME}/processor/bancolombia-assembler:start" \
  -H "Accept: application/vnd.atlas.2024-05-30+json"

echo ""
echo "=== Done ==="
