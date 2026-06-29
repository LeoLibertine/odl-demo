#!/usr/bin/env python3
"""
Setup Maestro de Cuentas - TFW Bancolombia
==========================================
Precarga 1 millón de cuentas en MongoDB Atlas (CLUSTER ORIGEN)
para simular el Maestro de Cuentas del AS/400.

Uso:
    python setup_maestro_cuentas.py --cuentas 1000000
    python setup_maestro_cuentas.py --cuentas 100000  # Para pruebas rápidas
"""

import argparse
import random
import sys
import time
from datetime import datetime, timedelta
from typing import Generator, Dict, Any, List

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

sys.path.insert(0, '..')
from config.settings import get_mongodb_origen_config


# =============================================================================
# CONSTANTES - Distribuciones realistas para Bancolombia
# =============================================================================

TIPOS_CUENTA = {
    "AHO": 0.70,    # 70% Cuentas de ahorro
    "COR": 0.30,    # 30% Cuentas corrientes
}

ESTADOS_CUENTA = {
    "01": ("ACTIVA", 0.85),
    "02": ("INACTIVA", 0.08),
    "03": ("BLOQUEADA", 0.04),
    "04": ("EMBARGADA", 0.02),
    "05": ("CANCELADA", 0.01),
}

PLANES = {
    "AHO": ["001", "002", "003", "010", "015", "020"],
    "COR": ["050", "051", "052", "060"],
}

SUCURSALES = [f"{i:04d}" for i in range(1, 501)]

SALDOS = {
    "AHO": {"min": 0, "max": 500_000_000},
    "COR": {"min": 0, "max": 2_000_000_000},
}


def seleccionar_por_distribucion(distribucion: dict) -> str:
    r = random.random()
    acumulado = 0.0
    for valor, prob in distribucion.items():
        if isinstance(prob, tuple):
            prob = prob[1]
        acumulado += prob
        if r <= acumulado:
            return valor
    return list(distribucion.keys())[-1]


def generar_saldo(tipo_cuenta: str) -> float:
    config = SALDOS[tipo_cuenta]
    saldo = random.lognormvariate(mu=14, sigma=1.5)
    if tipo_cuenta == "COR":
        saldo *= 5
    saldo = max(config["min"], min(saldo, config["max"]))
    return round(saldo, 2)


def generar_fecha_apertura() -> datetime:
    dias_atras = random.randint(1, 365 * 30)  # Hasta 30 años atrás
    return datetime.now() - timedelta(days=dias_atras)


def generar_cuenta(indice: int) -> Dict[str, Any]:
    """Genera una cuenta bancaria realista."""
    tipo = seleccionar_por_distribucion(TIPOS_CUENTA)
    estado = seleccionar_por_distribucion(ESTADOS_CUENTA)
    prefijo = "4000" if tipo == "AHO" else "4100"
    numero_cuenta = f"{prefijo}{indice:010d}"
    
    fecha_apertura = generar_fecha_apertura()
    saldo = generar_saldo(tipo)
    
    # Si cuenta está cancelada, saldo = 0
    if estado == "05":
        saldo = 0
    
    return {
        "_id": numero_cuenta,
        "numero_cuenta": numero_cuenta,
        "tipo_cuenta": tipo,
        "estado_codigo": estado,
        "estado_nombre": ESTADOS_CUENTA[estado][0],
        "plan": random.choice(PLANES[tipo]),
        "sucursal_apertura": random.choice(SUCURSALES),
        "fecha_apertura": fecha_apertura,
        "saldo_disponible": saldo,
        "saldo_total": saldo + random.uniform(0, saldo * 0.05),
        "saldo_canje": random.uniform(0, 500000) if random.random() < 0.1 else 0,
        "identificacion_cliente": f"{random.randint(10000000, 99999999)}",
        "tipo_identificacion": random.choice(["CC", "CC", "CC", "CE", "NIT", "PAS"]),
        "nombre_cliente": f"CLIENTE {indice}",
        "exento_gmf": random.random() < 0.15,
        "moneda": "COP",
        "tasa_interes": round(random.uniform(0.5, 8.0), 2) if tipo == "AHO" else 0,
        "sobregiro_aprobado": random.choice([0, 500000, 1000000, 2000000, 5000000]) if tipo == "COR" else 0,
        "created_at": fecha_apertura,
        "updated_at": datetime.now(),
    }


def generar_cuentas_batch(start: int, batch_size: int) -> List[Dict[str, Any]]:
    return [generar_cuenta(i) for i in range(start, start + batch_size)]


def main():
    parser = argparse.ArgumentParser(description="Setup Maestro de Cuentas")
    parser.add_argument("--cuentas", "-c", type=int, default=1_000_000,
                       help="Número de cuentas a generar (default: 1,000,000)")
    parser.add_argument("--batch", "-b", type=int, default=10_000,
                       help="Tamaño del batch de inserción (default: 10,000)")
    parser.add_argument("--drop", action="store_true",
                       help="Eliminar colección existente antes de insertar")
    args = parser.parse_args()

    print("=" * 60)
    print("🏦 SETUP MAESTRO DE CUENTAS - TFW BANCOLOMBIA")
    print("   📦 Cluster ORIGEN (Simulación iSeries)")
    print("=" * 60)

    config = get_mongodb_origen_config()
    print(f"\n🔌 Conectando a MongoDB Atlas (ORIGEN)...")
    print(f"   Database: {config.database}")
    print(f"   Collection: {config.collection_cuentas}")

    client = MongoClient(config.uri)
    db = client[config.database]
    collection = db[config.collection_cuentas]

    # Ping
    client.admin.command('ping')
    print("✅ Conexión exitosa")

    # Drop si se solicita
    if args.drop:
        print(f"\n⚠️  Eliminando colección existente...")
        collection.drop()
        print("   ✅ Colección eliminada")

    # Verificar si ya hay datos
    existing = collection.count_documents({})
    if existing > 0:
        print(f"\n⚠️  Ya existen {existing:,} cuentas en la colección.")
        resp = input("   ¿Deseas continuar agregando más? (s/N): ")
        if resp.lower() != 's':
            print("   Operación cancelada.")
            return

    # Generar e insertar
    total = args.cuentas
    batch_size = args.batch
    print(f"\n📊 Generando {total:,} cuentas en batches de {batch_size:,}...")

    start_time = time.time()
    inserted = 0

    for start in range(0, total, batch_size):
        current_batch = min(batch_size, total - start)
        cuentas = generar_cuentas_batch(start + existing, current_batch)

        try:
            result = collection.insert_many(cuentas, ordered=False)
            inserted += len(result.inserted_ids)
        except BulkWriteError as e:
            inserted += e.details.get('nInserted', 0)

        elapsed = time.time() - start_time
        rate = inserted / elapsed if elapsed > 0 else 0
        pct = inserted / total * 100
        print(f"   [{pct:5.1f}%] {inserted:>10,} / {total:,} cuentas "
              f"({rate:,.0f} docs/s) | {elapsed:.1f}s", end="\r")

    elapsed = time.time() - start_time
    print()

    # Crear índices
    print(f"\n📑 Creando índices...")
    collection.create_index([("tipo_cuenta", ASCENDING)])
    collection.create_index([("estado_codigo", ASCENDING)])
    collection.create_index([("sucursal_apertura", ASCENDING)])
    collection.create_index([("identificacion_cliente", ASCENDING)])
    print("   ✅ Índices creados")

    # Resumen
    total_final = collection.count_documents({})
    print(f"\n{'═' * 60}")
    print(f"✅ SETUP COMPLETADO")
    print(f"   Cuentas insertadas: {inserted:,}")
    print(f"   Total en colección: {total_final:,}")
    print(f"   Tiempo: {elapsed:.1f}s ({inserted/elapsed:,.0f} docs/s)")
    print(f"{'═' * 60}")

    client.close()


if __name__ == "__main__":
    main()
