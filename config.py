"""
══════════════════════════════════════════════════════════════════════════════════
  ODL DEMO — Configuración Centralizada
  Technical Feasibility Workshop (TFW)
══════════════════════════════════════════════════════════════════════════════════

  Archivo único de configuración para todos los servicios.
  Todos los módulos (api_canales, api_simulate, api_chat) importan de aquí.

  ⚠️  TODAS las credenciales se leen EXCLUSIVAMENTE de variables de entorno.
      No hay valores hardcodeados. Copia .env.example a .env, completa tus
      llaves, y arranca con ./start.sh (que exporta el .env automáticamente).

      Mínimo indispensable para levantar app + dashboard:
        MONGODB_ODL_URI

      Opcionales (la demo degrada con gracia si faltan):
        VOYAGE_API_KEY     → búsqueda vectorial / embeddings en el chat
        ANTHROPIC_API_KEY  → chat con IA
        OPENAI_API_KEY     → text-to-speech (voz)
══════════════════════════════════════════════════════════════════════════════════
"""

import os

# ═══════════════════════════════════════════════════════════════════════════════
#  MONGODB ATLAS
# ═══════════════════════════════════════════════════════════════════════════════

# Cluster ODL — donde viven los movimientos ensamblados (datos ya poblados)
MONGODB_ODL_URI = os.environ.get("MONGODB_ODL_URI", "")

# Cluster Origen — pool de cuentas para el simulador (opcional)
MONGODB_ORIGEN_URI = os.environ.get("MONGODB_URI_ORIGEN", "")

# Bases de datos
DB_NAME_ODL = "bancolombia_odl"
DB_NAME_ORIGEN = "iseries_sim"

# Colecciones principales
COLLECTION_MOVIMIENTOS = "movimientos"
COLLECTION_DLQ = "movimientos_dlq"
COLLECTION_METRICAS = "metricas_asp"
COLLECTION_CUENTAS = "cuentas"

# ═══════════════════════════════════════════════════════════════════════════════
#  KAFKA (OPCIONAL — solo para el simulador en vivo)
#  La app y el dashboard NO necesitan Kafka: leen los datos ya poblados en Mongo.
#  Si quieres correr el simulador generando carga en tiempo real, provee tu propia
#  infraestructura Kafka/MSK + un consumidor que escriba a Mongo. Déjalo vacío para
#  deshabilitarlo.
# ═══════════════════════════════════════════════════════════════════════════════

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY", "")
KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "bancolombia.depositos.cdc.raw")

# ═══════════════════════════════════════════════════════════════════════════════
#  VOYAGE AI (Embeddings vía MongoDB Atlas proxy)
# ═══════════════════════════════════════════════════════════════════════════════

VOYAGE_API_URL = "https://ai.mongodb.com/v1/embeddings"
VOYAGE_API_KEY = os.environ.get("VOYAGE_API_KEY", "")
VOYAGE_MODEL = "voyage-4-large"
VOYAGE_DIMENSIONS = 1024

# ═══════════════════════════════════════════════════════════════════════════════
#  ANTHROPIC (Claude AI)
# ═══════════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")

# ═══════════════════════════════════════════════════════════════════════════════
#  OPENAI TTS (Text-to-Speech) — opcional
# ═══════════════════════════════════════════════════════════════════════════════

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_TTS_URL = "https://api.openai.com/v1/audio/speech"
OPENAI_TTS_MODEL = "tts-1"        # tts-1 (rápido) o tts-1-hd (mejor calidad)
OPENAI_TTS_VOICE = "nova"          # nova/alloy/echo/fable/onyx/shimmer

# ═══════════════════════════════════════════════════════════════════════════════
#  API SERVER
# ═══════════════════════════════════════════════════════════════════════════════

API_PORT = int(os.environ.get("API_PORT", "8788"))
API_HOST = "0.0.0.0"

# ═══════════════════════════════════════════════════════════════════════════════
#  VECTOR SEARCH / ATLAS SEARCH
# ═══════════════════════════════════════════════════════════════════════════════

VECTOR_INDEX_NAME = "movimientos_vector"
VECTOR_FIELD_PATH = "comercio.embedding"
SEARCH_INDEX_NAME = "movimientos_search"

# ═══════════════════════════════════════════════════════════════════════════════
#  UTILIDADES
# ═══════════════════════════════════════════════════════════════════════════════

def print_config_status():
    """Imprime el estado de la configuración al arrancar."""
    print()
    print("═" * 70)
    print("  CONFIGURACIÓN — ODL Demo (TFW)")
    print("═" * 70)
    print()
    print(f"  MongoDB ODL:    {'✅' if MONGODB_ODL_URI else '⚠️  No configurada (requerida)'}")
    print(f"  MongoDB Origen: {'✅' if MONGODB_ORIGEN_URI else 'ℹ️  No configurada (opcional)'}")
    print(f"  Kafka:          {'✅' if KAFKA_BOOTSTRAP_SERVERS else 'ℹ️  Deshabilitado (opcional)'}")
    print(f"  Voyage AI:      {'✅' if VOYAGE_API_KEY else 'ℹ️  No configurada (opcional)'}")
    print(f"  Claude AI:      {'✅' if ANTHROPIC_API_KEY else 'ℹ️  No configurada (opcional)'}")
    print(f"  OpenAI TTS:     {'✅' if OPENAI_API_KEY else 'ℹ️  No configurada (opcional)'}")
    print(f"  Puerto API:     {API_PORT}")
    print()

    if not MONGODB_ODL_URI:
        print("  ⚠️  Falta MONGODB_ODL_URI. Copia .env.example a .env y complétala.")
        print()

    print("═" * 70)
    print()
