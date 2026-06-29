"""
Configuración del Simulador iSeries - TFW
=========================================
Toda la configuración se lee EXCLUSIVAMENTE de variables de entorno.
No hay credenciales hardcodeadas. Define las variables en tu .env
(ver .env.example en la raíz del repo).
"""

import os
from dataclasses import dataclass


# =============================================================================
# DATACLASSES DE CONFIGURACIÓN
# =============================================================================

@dataclass
class MongoDBOrigenConfig:
    """Configuración del cluster ORIGEN (pool de cuentas - simulación iSeries)."""
    uri: str
    database: str
    collection_cuentas: str


@dataclass
class MongoDBDestinoConfig:
    """Configuración del cluster DESTINO (ODL - donde ASP escribe)."""
    uri: str
    database: str
    collection_movimientos: str
    collection_dlq: str


@dataclass
class KafkaConfig:
    """Configuración de Confluent Cloud Kafka."""
    bootstrap_servers: str
    api_key: str
    api_secret: str
    topic: str


@dataclass
class GeneratorConfig:
    """Configuración del generador de transacciones."""
    tps: int
    duration_seconds: int
    accounts_sample_size: int
    pct_incomplete: float
    pct_duplicate: float
    pct_out_of_order: float
    delay_monetary_min: int = 50
    delay_monetary_max: int = 300
    delay_metadata_min: int = 100
    delay_metadata_max: int = 500


# =============================================================================
# CREDENCIALES HARDCODEADAS - TFW BANCOLOMBIA
# =============================================================================

# --- MongoDB Atlas - Cluster ORIGEN (Pool de cuentas) ---
_MONGODB_ORIGEN_URI = os.environ.get("MONGODB_URI_ORIGEN", "")
_MONGODB_ORIGEN_DATABASE = os.environ.get(
    "MONGODB_DATABASE_ORIGEN",
    "iseries_sim"
)
_MONGODB_ORIGEN_COLLECTION_CUENTAS = os.environ.get(
    "MONGODB_COLLECTION_CUENTAS",
    "cuentas"
)

# --- MongoDB Atlas - Cluster DESTINO (ODL) ---
_MONGODB_DESTINO_URI = os.environ.get(
    "MONGODB_URI_DESTINO",
    os.environ.get("MONGODB_ODL_URI", "")
)
_MONGODB_DESTINO_DATABASE = os.environ.get(
    "MONGODB_DATABASE_DESTINO",
    "bancolombia_odl"
)
_MONGODB_DESTINO_COLLECTION_MOVIMIENTOS = os.environ.get(
    "MONGODB_COLLECTION_MOVIMIENTOS",
    "movimientos"
)
_MONGODB_DESTINO_COLLECTION_DLQ = os.environ.get(
    "MONGODB_COLLECTION_DLQ",
    "dlq_fragmentos_incompletos"
)

# --- Kafka (opcional — provee tu propia infraestructura) ---
_KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
_KAFKA_API_KEY = os.environ.get("KAFKA_API_KEY", "")
_KAFKA_API_SECRET = os.environ.get("KAFKA_API_SECRET", "")
_KAFKA_TOPIC = os.environ.get(
    "KAFKA_TOPIC",
    "bancolombia.depositos.cdc.raw"
)

# --- Generador (defaults) ---
_GENERATOR_TPS = int(os.environ.get("GENERATOR_TPS", "500"))
_GENERATOR_DURATION = int(os.environ.get("GENERATOR_DURATION_SECONDS", "300"))
_GENERATOR_ACCOUNTS = int(os.environ.get("GENERATOR_ACCOUNTS_SAMPLE", "100000"))
_GENERATOR_PCT_INCOMPLETE = float(os.environ.get("GENERATOR_PCT_INCOMPLETE", "0.03"))
_GENERATOR_PCT_DUPLICATE = float(os.environ.get("GENERATOR_PCT_DUPLICATE", "0.02"))
_GENERATOR_PCT_OUT_OF_ORDER = float(os.environ.get("GENERATOR_PCT_OUT_OF_ORDER", "0.10"))


# =============================================================================
# FUNCIONES DE ACCESO (interfaz pública)
# =============================================================================

def get_mongodb_origen_config() -> MongoDBOrigenConfig:
    """Retorna configuración del cluster ORIGEN."""
    return MongoDBOrigenConfig(
        uri=_MONGODB_ORIGEN_URI,
        database=_MONGODB_ORIGEN_DATABASE,
        collection_cuentas=_MONGODB_ORIGEN_COLLECTION_CUENTAS,
    )


def get_mongodb_destino_config() -> MongoDBDestinoConfig:
    """Retorna configuración del cluster DESTINO (ODL)."""
    return MongoDBDestinoConfig(
        uri=_MONGODB_DESTINO_URI,
        database=_MONGODB_DESTINO_DATABASE,
        collection_movimientos=_MONGODB_DESTINO_COLLECTION_MOVIMIENTOS,
        collection_dlq=_MONGODB_DESTINO_COLLECTION_DLQ,
    )


def get_kafka_config() -> KafkaConfig:
    """Retorna configuración de Kafka/Confluent Cloud."""
    return KafkaConfig(
        bootstrap_servers=_KAFKA_BOOTSTRAP_SERVERS,
        api_key=_KAFKA_API_KEY,
        api_secret=_KAFKA_API_SECRET,
        topic=_KAFKA_TOPIC,
    )


def get_generator_config() -> GeneratorConfig:
    """Retorna configuración del generador de transacciones."""
    return GeneratorConfig(
        tps=_GENERATOR_TPS,
        duration_seconds=_GENERATOR_DURATION,
        accounts_sample_size=_GENERATOR_ACCOUNTS,
        pct_incomplete=_GENERATOR_PCT_INCOMPLETE,
        pct_duplicate=_GENERATOR_PCT_DUPLICATE,
        pct_out_of_order=_GENERATOR_PCT_OUT_OF_ORDER,
    )


# =============================================================================
# VERIFICACIÓN
# =============================================================================

def print_config_status():
    """Imprime el estado de la configuración al arrancar."""
    print()
    print("═" * 60)
    print("  CONFIGURACIÓN DEL SIMULADOR iSeries - TFW Bancolombia")
    print("═" * 60)
    
    # MongoDB Origen
    origen = get_mongodb_origen_config()
    uri_masked = origen.uri[:30] + "..." if len(origen.uri) > 30 else origen.uri
    print(f"\n📦 CLUSTER ORIGEN (Cuentas):")
    print(f"   URI: {uri_masked}")
    print(f"   DB:  {origen.database}")
    print(f"   Col: {origen.collection_cuentas}")
    
    # MongoDB Destino
    destino = get_mongodb_destino_config()
    uri_masked = destino.uri[:30] + "..." if len(destino.uri) > 30 else destino.uri
    print(f"\n🎯 CLUSTER DESTINO (ODL):")
    print(f"   URI: {uri_masked}")
    print(f"   DB:  {destino.database}")
    print(f"   Mov: {destino.collection_movimientos}")
    print(f"   DLQ: {destino.collection_dlq}")
    
    # Kafka
    kafka = get_kafka_config()
    print(f"\n📡 CONFLUENT CLOUD (Kafka):")
    print(f"   Bootstrap: {kafka.bootstrap_servers}")
    print(f"   API Key:   {kafka.api_key[:8]}...")
    print(f"   Topic:     {kafka.topic}")
    
    # Generador
    gen = get_generator_config()
    print(f"\n⚙️  GENERADOR:")
    print(f"   TPS Default:  {gen.tps:,}")
    print(f"   Duración:     {gen.duration_seconds}s")
    print(f"   Cuentas:      {gen.accounts_sample_size:,}")
    print(f"   Anomalías:    {gen.pct_incomplete*100:.0f}% incompletos, "
          f"{gen.pct_duplicate*100:.0f}% duplicados, "
          f"{gen.pct_out_of_order*100:.0f}% desordenados")
    
    print()
    print("═" * 60)


if __name__ == "__main__":
    print_config_status()
