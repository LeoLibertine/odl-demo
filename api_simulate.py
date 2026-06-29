"""
══════════════════════════════════════════════════════════════════════════
  BANCOLOMBIA ODL — Simulación Read-Your-Writes (v2 — Kafka-compatible)
══════════════════════════════════════════════════════════════════════════

  Genera fragmentos CDC IDÉNTICOS al generador original (sciffmrcmv.py)
  para que ASP los procese correctamente.

  Formato de cada fragmento (CDCFragment.to_kafka_message):
  {
    "header": {
      "operation": "INSERT",
      "timestamp": "...",
      "source": {"system":"AS400","library":"SCILIBRAMD","table":"SCIFFMRCMV","commit_lsn":"..."},
      "transaction_id": "..."
    },
    "correlation": {
      "key": "NUMCTA-FECMOV-HORMOV-CODCAN",
      "fragment_type": "HEADER|MONETARY|METADATA",
      "fragment_sequence": 1|2|3,
      "total_fragments": 3
    },
    "before": null,
    "after": { ...campos SCIFFMRCMV con ~70 campos legacy... }
  }
══════════════════════════════════════════════════════════════════════════
"""

import os
import json
import uuid
import time
import random
import string
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ═══ CONFIGURACIÓN — importada desde config.py ═══
from config import (
    KAFKA_BOOTSTRAP_SERVERS as KAFKA_BOOTSTRAP,
    KAFKA_API_KEY, KAFKA_API_SECRET, KAFKA_TOPIC,
    MONGODB_ODL_URI,
)
MONGODB_URI = MONGODB_ODL_URI


def safe_number(val, default=0):
    """Convierte cualquier tipo MongoDB (Decimal128, etc.) a float."""
    if val is None:
        return default
    try:
        return float(str(val)) if not isinstance(val, (int, float)) else float(val)
    except (ValueError, TypeError):
        return default


# ═══ KAFKA PRODUCER ═══
_producer = None

def get_producer():
    global _producer
    if _producer is not None:
        return _producer
    if not KAFKA_BOOTSTRAP:
        return None
    try:
        from confluent_kafka import Producer
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-512",
            "sasl.username": KAFKA_API_KEY,
            "sasl.password": KAFKA_API_SECRET,
            "linger.ms": 50,
            "batch.num.messages": 100,
            "compression.type": "lz4",
            "acks": "all",
            "client.id": "tfw-demo-ryw",
        }
        _producer = Producer(conf)
        print(f"✅ Kafka Producer conectado a {KAFKA_BOOTSTRAP}")
        return _producer
    except ImportError:
        print("⚠️  confluent_kafka no instalado — modo fallback")
        return None
    except Exception as e:
        print(f"⚠️  Error conectando Kafka: {e} — modo fallback")
        return None


# ═══ MONGODB ═══
_db = None

def get_db():
    global _db
    if _db is not None:
        return _db
    if not MONGODB_URI:
        return None
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGODB_URI)
        _db = client["bancolombia_odl"]
        return _db
    except Exception as e:
        print(f"⚠️  Error conectando MongoDB: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# GENERADOR DE FRAGMENTOS CDC — Formato idéntico al generador original
# ═══════════════════════════════════════════════════════════════════════

_lsn_counter = 0

def _gen_lsn() -> str:
    global _lsn_counter
    _lsn_counter += 1
    return f"{_lsn_counter:08X}:{random.randint(0, 0xFFFF):04X}:0001"

def _gen_txn_id() -> str:
    return f"TXN-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"

def _rand_str(n: int) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def _gen_legacy_code(prefix: str, length: int = 6) -> str:
    pad = length - len(prefix)
    return f"{prefix}{random.randint(0, 10**pad - 1):0{pad}d}"


# ── Tipos de transacción ──
TIPOS_TX = {
    "NOMINA":                {"cod": "PAY", "codtrn": "NM0001", "signo": "C", "desc": "Pago Nómina",             "rango": (1_500_000, 12_000_000), "canal": "APP"},
    "TRANSFERENCIA_RECIBIDA":{"cod": "TRF", "codtrn": "TR0001", "signo": "C", "desc": "Transferencia Entrante",  "rango": (50_000, 5_000_000),     "canal": "APP"},
    "RETIRO_ATM":            {"cod": "WDR", "codtrn": "RT0001", "signo": "D", "desc": "Retiro",                  "rango": (50_000, 2_000_000),     "canal": "ATM"},
    "COMPRA_WEB":            {"cod": "PSE", "codtrn": "PS0001", "signo": "D", "desc": "Pago PSE",                "rango": (10_000, 3_000_000),     "canal": "WEB"},
    "PAGO_SERVICIO":         {"cod": "PSE", "codtrn": "PS0002", "signo": "D", "desc": "Pago servicios públicos", "rango": (30_000, 500_000),       "canal": "APP"},
    "CONSIGNACION":          {"cod": "DEP", "codtrn": "DP0001", "signo": "C", "desc": "Consignación",            "rango": (50_000, 10_000_000),    "canal": "SUC"},
}

BANCOS = [
    ("007", "BANCOLOMBIA"), ("001", "BANCO DE BOGOTA"), ("051", "DAVIVIENDA"),
    ("013", "BBVA COLOMBIA"), ("002", "BANCO POPULAR"), ("023", "BANCO DE OCCIDENTE"),
]

CANAL_NOMBRES = {
    "APP": "App Móvil", "WEB": "Banca Web", "ATM": "Cajero ATM",
    "SUC": "Sucursal", "PRM": "Sistema",
}


def _gen_header_legacy() -> Dict[str, Any]:
    """~60 campos legacy del HEADER — idéntico a LegacyFieldsGenerator."""
    now = datetime.now()
    return {
        "CODENT": _gen_legacy_code("E", 4), "CODPAI": "CO",
        "CODREG": f"{random.randint(1,32):02d}", "CODCIU": f"{random.randint(1,999):03d}",
        "CODZON": f"{random.randint(1,99):02d}", "CODSEC": _gen_legacy_code("S", 5),
        "CODARE": _gen_legacy_code("A", 4), "CODUNI": _gen_legacy_code("U", 6),
        "CODCEN": _gen_legacy_code("C", 5), "CODPRO": _gen_legacy_code("P", 4),
        "CODLIN": _gen_legacy_code("L", 3), "CODSUB": _gen_legacy_code("SB", 4),
        "CODMOD": _gen_legacy_code("M", 3), "CODPLA": _gen_legacy_code("PL", 4),
        "CODTAR": _gen_legacy_code("T", 5), "CODCONV": _gen_legacy_code("CV", 6),
        "CODCAM": _gen_legacy_code("CM", 4), "CODSEG": _gen_legacy_code("SG", 3),
        "CODNIC": _gen_legacy_code("N", 4), "CODCLI": _gen_legacy_code("CLI", 8),
        "RUTEFEC": now.strftime("%Y%m%d"), "RUTEHOR": now.strftime("%H%M%S"),
        "RUTEORI": _gen_legacy_code("RO", 6), "RUTEDES": _gen_legacy_code("RD", 6),
        "RUTEINT": _gen_legacy_code("RI", 6), "RUTECOD": _gen_legacy_code("RC", 8),
        "RUTESEQ": random.randint(1, 999999), "RUTEPRI": random.randint(1, 9),
        "RUTEFLG": random.choice(["Y", "N"]), "RUTEEST": f"{random.randint(0,99):02d}",
        "RUTERET": random.randint(0, 5), "RUTETIM": random.randint(100, 9999),
        "RUTEQUE": _gen_legacy_code("Q", 4), "RUTEPOL": _gen_legacy_code("POL", 5),
        "RUTEBAL": _gen_legacy_code("BAL", 4),
        "SISTCOD": _gen_legacy_code("SYS", 5),
        "SISTVER": f"{random.randint(1,9)}.{random.randint(0,99)}",
        "SISTMOD": _gen_legacy_code("MOD", 4),
        "SISTENV": random.choice(["PRD", "QAS", "DEV"]),
        "SISTNOD": f"NODE{random.randint(1,99):02d}",
        "SISTCLU": f"CLU{random.randint(1,9)}",
        "SISTPAR": f"PAR{random.randint(1,99):02d}",
        "SISTJOB": _gen_legacy_code("JOB", 8), "SISTTSK": _gen_legacy_code("TSK", 6),
        "SISTPGM": _gen_legacy_code("PGM", 10), "SISTLIB": _gen_legacy_code("LIB", 10),
        "SISTFIL": _gen_legacy_code("FIL", 10), "SISTMBR": _gen_legacy_code("MBR", 10),
        "SISTUSR": _gen_legacy_code("USR", 10), "SISTPRF": _gen_legacy_code("PRF", 10),
        "FILLER_HDR_01": " " * 10, "FILLER_HDR_02": " " * 20,
        "FILLER_HDR_03": " " * 15, "FILLER_HDR_04": " " * 8,
        "FILLER_HDR_05": " " * 12, "RESERV_HDR_01": "0" * 10,
        "RESERV_HDR_02": "0" * 8, "RESERV_HDR_03": "0" * 6,
        "RESERV_HDR_04": "0" * 4, "RESERV_HDR_05": "0" * 12,
    }


def _gen_monetary_legacy() -> Dict[str, Any]:
    """~70 campos legacy del MONETARY."""
    base = random.uniform(1000, 10000000)
    return {
        "MTOBRUT": base, "MTONETO": base * 0.96, "MTOBASE": base * 0.95,
        "MTOAJUS": base * 0.02, "MTODESC": base * 0.01, "MTORECA": base * 0.005,
        "MTOBONI": 0, "MTORETE": base * 0.004, "MTOIVA1": base * 0.19,
        "MTOIVA2": 0, "MTOIVA3": 0, "MTOEXEN": 0, "MTOGRAV": base,
        "MTONOAP": 0, "MTOCOMI": base * 0.003, "MTOSEGU": base * 0.001,
        "MTOINTE": base * 0.005, "MTOMORA": 0, "MTOMULT": 0, "MTOINCA": 0,
        "MTOVACA": 0, "MTOPRES": 0, "MTOAPEN": 0, "MTOASAL": 0, "MTOARSG": 0,
        "TASNOM": round(random.uniform(0.01, 0.25), 6),
        "TASEFE": round(random.uniform(0.01, 0.30), 6),
        "TASMOR": round(random.uniform(0.01, 0.05), 6),
        "TASPEN": round(random.uniform(0.001, 0.02), 6),
        "TASBON": 0, "TASDSC": round(random.uniform(0.001, 0.05), 6),
        "TASIVA": 0.19, "TASRET": 0.04, "TASRFT": 0.004, "TASGMF": 0.004,
        "TASCRE": round(random.uniform(0.01, 0.20), 6),
        "TASDEB": round(random.uniform(0.001, 0.01), 6),
        "TASAHF": round(random.uniform(0.01, 0.08), 6),
        "TASCDT": round(random.uniform(0.03, 0.12), 6),
        "TASDTF": round(random.uniform(0.05, 0.15), 6),
        "SLDINI": random.uniform(0, 100000000), "SLDFIN": random.uniform(0, 100000000),
        "SLDMED": random.uniform(0, 50000000), "SLDMIN": random.uniform(0, 10000000),
        "SLDMAX": random.uniform(10000000, 200000000),
        "SLDPRO": random.uniform(0, 50000000), "SLDDIS": random.uniform(0, 100000000),
        "SLDRES": random.uniform(0, 10000000), "SLDPIG": random.uniform(0, 5000000),
        "SLDEMB": 0, "SLDCAJ": random.uniform(0, 1000000),
        "SLDREM": random.uniform(0, 500000), "SLDPDT": 0, "SLDCOB": 0, "SLDPAG": 0,
        "INDLIQ": random.choice(["A", "B", "C"]),
        "INDRIE": random.choice(["1", "2", "3", "4", "5"]),
        "INDREN": random.choice(["A", "B", "C", "D"]),
        "INDSOL": random.choice(["S", "N"]),
        "INDCAR": random.choice(["A", "B", "C", "D", "E"]),
        "INDMOR": random.choice(["0", "1", "2", "3"]),
        "INDCAS": random.choice(["N", "C", "P"]),
        "INDPRO": random.choice(["A", "B", "C"]),
        "INDCAL": random.choice(["A", "B", "C", "D", "E"]),
        "INDVIG": random.choice(["V", "S", "C"]),
        "FILLER_MON_01": " " * 15, "FILLER_MON_02": " " * 10,
        "FILLER_MON_03": "0" * 18, "FILLER_MON_04": "0" * 12,
        "FILLER_MON_05": " " * 20,
    }


def _gen_metadata_legacy() -> Dict[str, Any]:
    """~70 campos legacy del METADATA."""
    now = datetime.now()
    return {
        "AUDFCRE": now.strftime("%Y%m%d"), "AUDHCRE": now.strftime("%H%M%S"),
        "AUDUSRC": _gen_legacy_code("USR", 10), "AUDPGMC": _gen_legacy_code("PGM", 10),
        "AUDFMOD": now.strftime("%Y%m%d"), "AUDHMOD": now.strftime("%H%M%S"),
        "AUDUSRM": _gen_legacy_code("USR", 10), "AUDPGMM": _gen_legacy_code("PGM", 10),
        "AUDFELI": "00000000", "AUDHELI": "000000", "AUDUSRE": "", "AUDPGME": "",
        "AUDSECC": random.randint(1, 999999), "AUDSECM": random.randint(1, 999999),
        "AUDVERS": random.randint(1, 99),
        "AUDESTA": random.choice(["A", "I", "P", "E"]),
        "AUDTIPO": random.choice(["N", "M", "E", "C"]),
        "AUDORIG": _gen_legacy_code("ORI", 6), "AUDDEST": _gen_legacy_code("DES", 6),
        "AUDOBS": _rand_str(50),
        "TRKUUID": _rand_str(32), "TRKCORR": _rand_str(24),
        "TRKSPAN": _rand_str(16), "TRKPARENT": _rand_str(16),
        "TRKROOT": _rand_str(16), "TRKSVC": _gen_legacy_code("SVC", 10),
        "TRKOPER": _gen_legacy_code("OPR", 8),
        "TRKVER": f"v{random.randint(1,5)}.{random.randint(0,9)}",
        "TRKENV": random.choice(["prd", "stg", "dev"]),
        "TRKHOST": f"host{random.randint(1,99):02d}",
        "TRKPOD": f"pod-{_rand_str(8).lower()}", "TRKNS": random.choice(["default", "banking", "core"]),
        "TRKLAT": random.randint(1, 5000), "TRKSTA": random.choice(["OK", "ERR", "WARN"]),
        "TRKCOD": f"{random.randint(200, 599)}",
        "CLINOMB": _rand_str(30), "CLIAPE1": _rand_str(20), "CLIAPE2": _rand_str(20),
        "CLIDIR1": _rand_str(40), "CLIDIR2": _rand_str(40),
        "CLIBARR": _rand_str(20), "CLICIUD": _rand_str(20), "CLIDEPA": _rand_str(20),
        "CLIPAIS": "COLOMBIA",
        "CLITEL1": f"3{random.randint(100000000, 999999999)}",
        "CLITEL2": f"6{random.randint(10000000, 99999999)}",
        "CLIEMAI": f"cliente{random.randint(1000,9999)}@email.com",
        "CLIPROF": _gen_legacy_code("PRO", 4), "CLIOCUP": _gen_legacy_code("OCU", 4),
        "CLIACTV": _gen_legacy_code("ACT", 4),
        "MIGFEC1": "19950615", "MIGFEC2": "20010301", "MIGFEC3": "20080915",
        "MIGFEC4": "20151120", "MIGFEC5": "20200601",
        "MIGCOD1": _gen_legacy_code("M95", 8), "MIGCOD2": _gen_legacy_code("M01", 8),
        "MIGCOD3": _gen_legacy_code("M08", 8), "MIGCOD4": _gen_legacy_code("M15", 8),
        "MIGCOD5": _gen_legacy_code("M20", 8),
        "LEGSIS1": _gen_legacy_code("SIS1", 10), "LEGSIS2": _gen_legacy_code("SIS2", 10),
        "LEGSIS3": _gen_legacy_code("SIS3", 10),
        "LEGCOD1": _gen_legacy_code("LC1", 15), "LEGCOD2": _gen_legacy_code("LC2", 15),
        "LEGCOD3": _gen_legacy_code("LC3", 15),
        "LEGREF1": _gen_legacy_code("LR1", 20), "LEGREF2": _gen_legacy_code("LR2", 20),
        "LEGFLG1": random.choice(["Y", "N", "P", "X"]),
        "LEGFLG2": random.choice(["Y", "N", "P", "X"]),
        "FILLER_MET_01": " " * 30, "FILLER_MET_02": " " * 50,
        "FILLER_MET_03": " " * 20, "FILLER_MET_04": "0" * 25,
        "FILLER_MET_05": " " * 40, "FILLER_MET_06": " " * 15,
        "FILLER_MET_07": "0" * 10, "FILLER_MET_08": " " * 25,
        "FILLER_MET_09": " " * 35, "FILLER_MET_10": "0" * 20,
    }


def build_cdc_fragment(
    fragment_type: str,
    fragment_sequence: int,
    correlation_key: str,
    transaction_id: str,
    after_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Construye un fragmento CDC con el formato EXACTO de CDCFragment.to_kafka_message()
    + el campo _produced_at que main.py agrega en línea 152.
    Este es el formato que ASP espera.
    """
    # ── CRITICAL: Usar UTC real para que event time ≈ system time ──
    # datetime.now() en Colombia = UTC-5 → watermark queda 5h atrás de system time
    # → la ventana NUNCA cierra porque el watermark no avanza lo suficiente
    # datetime.utcnow() = UTC real → watermark ≈ system time → ventanas cierran OK
    ts = datetime.utcnow().isoformat() + "Z"

    return {
        "header": {
            "operation": "INSERT",
            "timestamp": ts,
            "source": {
                "system": "AS400",
                "library": "SCILIBRAMD",
                "table": "SCIFFMRCMV",
                "commit_lsn": _gen_lsn(),
            },
            "transaction_id": transaction_id,
        },
        "correlation": {
            "key": correlation_key,
            "fragment_type": fragment_type,
            "fragment_sequence": fragment_sequence,
            "total_fragments": 3,
        },
        "before": None,
        "after": after_data,
        "_produced_at": datetime.utcnow().isoformat() + "Z",
    }


def generate_3_fragments(
    cuenta: str,
    tipo_cuenta: str,
    sucursal: str,
    tipo_tx: dict,
    monto: float,
    saldo_anterior: float,
) -> tuple:
    """
    Genera los 3 fragmentos CDC idénticos al generador original.
    Retorna (frag_header, frag_monetary, frag_metadata, correlation_key, saldo_nuevo).
    """
    now = datetime.now()
    fecha = now.strftime("%Y%m%d")
    hora = now.strftime("%H%M%S")
    canal = tipo_tx["canal"]
    correlation_key = f"{cuenta}-{fecha}-{hora}-{canal}"
    transaction_id = _gen_txn_id()

    saldo_nuevo = saldo_anterior + monto if tipo_tx["signo"] == "C" else saldo_anterior - monto
    gmf = monto * 0.004 if tipo_tx["signo"] == "D" else 0
    es_trf = tipo_tx["cod"] == "TRF"
    banco_dest = random.choice(BANCOS) if es_trf else ("", "")

    # ── HEADER after (~70 campos) ──
    header_core = {
        "NUMCTA": cuenta,
        "TIPCTA": tipo_cuenta,
        "FECMOV": fecha,
        "HORMOV": hora,
        "CODCAN": canal,
        "CODSUC": sucursal,
        "TIPMOV": tipo_tx["cod"],
        "CODTRN": tipo_tx["codtrn"],
        "SECMOV": random.randint(1, 999999),
        "ESTTRN": "00",
    }
    header_after = {**header_core, **_gen_header_legacy()}

    # ── MONETARY after (~80 campos) ──
    monetary_core = {
        "VALTRA": monto,
        "CODMON": "COP",
        "SLDANT": saldo_anterior,
        "SLDNUE": saldo_nuevo,
        "SIGNO": tipo_tx["signo"],
        "TASCAM": 1.0,
        "VALORI": monto,
        "MONORI": "COP",
        "VALIVA": 0,
        "VALGMF": gmf,
    }
    monetary_after = {**monetary_core, **_gen_monetary_legacy()}

    # ── METADATA after (~83 campos) ──
    ref_ts = now.strftime("%Y%m%d%H%M%S")
    metadata_core = {
        "NUMREF": f"REF{ref_ts}{random.randint(100000, 999999)}",
        "DESCRP": tipo_tx["desc"][:80],
        "CTADES": f"4000{random.randint(100000, 999999):06d}" if es_trf else "",
        "BANDES": banco_dest[0],
        "NOMDES": f"DESTINATARIO {random.randint(1, 9999)}" if es_trf else "",
        "NUMIDE": f"{random.randint(10000000, 99999999)}",
        "TIPIDE": "CC",
        "IPORIG": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "USERAG": f"BancolombiaApp/{random.randint(5,9)}.{random.randint(0,9)}.{random.randint(0,99)}",
        "LATGEO": round(random.uniform(4.5, 11.0), 6),
        "LONGGEO": round(random.uniform(-77.0, -72.0), 6),
        "DISPOS": random.choice(["iPhone14", "iPhone15", "SamsungS23", "SamsungA54", "XiaomiNote12", "Web"]),
        "SESION": f"SES{ref_ts}{random.randint(1000, 9999)}",
    }
    metadata_after = {**metadata_core, **_gen_metadata_legacy()}

    # ── Build CDCFragment.to_kafka_message() format ──
    frag_header = build_cdc_fragment("HEADER", 1, correlation_key, transaction_id, header_after)
    frag_monetary = build_cdc_fragment("MONETARY", 2, correlation_key, transaction_id, monetary_after)
    frag_metadata = build_cdc_fragment("METADATA", 3, correlation_key, transaction_id, metadata_after)

    return frag_header, frag_monetary, frag_metadata, correlation_key, saldo_nuevo


# ═══ MODELO DE REQUEST ═══
class SimulacionRequest(BaseModel):
    tipo: Optional[str] = None
    monto: Optional[int] = None


# ═══ ROUTER ═══
router = APIRouter(prefix="/api/v1", tags=["Simulación Read-Your-Writes"])


@router.post("/cuentas/{numero}/simular-movimiento")
async def simular_movimiento(numero: str, req: SimulacionRequest = SimulacionRequest()):
    """
    🚀 Simula una transacción completa para una cuenta específica.

    Produce 3 fragmentos CDC al Kafka con delays realistas entre ellos,
    exactamente como lo haría el iSeries real. ASP los procesa, ensambla
    y escribe en el ODL. La app detecta el cambio por polling.

    Flujo completo:
    POST → Kafka (3 fragmentos) → ASP assembly → MongoDB ODL → API GET

    Parámetros:
    - numero: Número de cuenta
    - tipo: Tipo de transacción (NOMINA, TRANSFERENCIA_RECIBIDA, RETIRO_ATM, COMPRA_WEB, PAGO_SERVICIO, CONSIGNACION)
    - monto: Monto específico (opcional, se genera aleatorio si no se proporciona)
    """
    t0 = time.time()

    # ── Validar cuenta y obtener saldo actual ──
    db = get_db()
    if db is None:
        raise HTTPException(503, "MongoDB ODL no disponible")

    ultimo = db.movimientos.find_one(
        {"cuenta.numero": numero},
        sort=[("movimiento.fecha_iso", -1)],
        projection={"monetario.saldo_nuevo": 1, "cuenta.tipo": 1, "cuenta.sucursal": 1}
    )

    if ultimo:
        saldo_actual = safe_number(ultimo.get("monetario", {}).get("saldo_nuevo"), 5_000_000)
        tipo_cuenta = ultimo.get("cuenta", {}).get("tipo") or "AHO"
        tipo_cuenta_code = {"SAVINGS": "AHO", "CHECKING": "COR", "NOMINA": "NOM"}.get(tipo_cuenta, tipo_cuenta)
        sucursal = ultimo.get("cuenta", {}).get("sucursal") or "0001"
    else:
        saldo_actual = 15_000_000.0
        tipo_cuenta = "AHO"
        tipo_cuenta_code = "AHO"
        sucursal = "0001"

    # ── Seleccionar tipo de transacción ──
    tipo_key = req.tipo or random.choice(list(TIPOS_TX.keys()))
    if tipo_key not in TIPOS_TX:
        raise HTTPException(400, f"Tipo '{tipo_key}' no válido. Opciones: {list(TIPOS_TX.keys())}")
    tipo_tx = TIPOS_TX[tipo_key]

    # ── Generar monto ──
    min_m, max_m = tipo_tx["rango"]
    monto = req.monto or random.randint(min_m, max_m)
    monto = round(monto / 1000) * 1000

    if tipo_tx["signo"] == "D" and monto > saldo_actual:
        monto = min(monto, int(saldo_actual * 0.8))
        if monto < 10_000:
            monto = 50_000

    # ── Generar los 3 fragmentos (formato idéntico al generador) ──
    demo_id = str(uuid.uuid4())
    frag_h, frag_m, frag_d, correlation_key, saldo_nuevo = generate_3_fragments(
        cuenta=numero,
        tipo_cuenta=tipo_cuenta_code,
        sucursal=sucursal,
        tipo_tx=tipo_tx,
        monto=float(monto),
        saldo_anterior=float(saldo_actual),
    )

    # ── Enviar ──
    producer = get_producer()
    modo = "kafka"
    fragmentos_enviados = 0
    delays = []
    now = datetime.now(timezone.utc)
    canal_nombre = CANAL_NOMBRES.get(tipo_tx["canal"], tipo_tx["canal"])

    def _build_direct_doc():
        return {
            "cuenta": {"numero": numero, "tipo": tipo_cuenta, "sucursal": sucursal},
            "movimiento": {
                "tipo": tipo_tx["cod"],
                "fecha_iso": datetime.now(timezone.utc),
                "hora": datetime.now().strftime("%H:%M:%S"),
                "canal": {"codigo": tipo_tx["canal"], "nombre": canal_nombre},
                "estado": "APPROVED",
            },
            "monetario": {
                "valor": float(monto),
                "moneda": "COP",
                "saldo_anterior": float(saldo_actual),
                "saldo_nuevo": float(saldo_nuevo),
                "signo": tipo_tx["signo"],
            },
            "_asp_metadata": {
                "correlation_key": correlation_key,
                "assembly_timestamp": datetime.now(timezone.utc),
                "is_complete": True,
                "source": "TFW_DEMO_DIRECT",
                "demo_id": demo_id,
            },
        }

    if producer:
        try:
            key_bytes = correlation_key.encode("utf-8")

            for frag_name, frag_data in [("HEADER", frag_h), ("MONETARY", frag_m), ("METADATA", frag_d)]:
                producer.produce(
                    KAFKA_TOPIC, key=key_bytes,
                    value=json.dumps(frag_data).encode("utf-8"),
                )
                producer.poll(0)
            fragmentos_enviados = 3
            delays = [{"fragment": "ALL", "delay_ms": 0}]

            def _flush_kafka():
                try:
                    flush_ts = (datetime.utcnow() + timedelta(seconds=20)).isoformat() + "Z"
                    FLUSH_ACCOUNTS = [
                        "99900000000001", "99900000000002", "99900000000003",
                        "99900000000004", "99900000000005", "99900000000006",
                    ]
                    for pi, fc in enumerate(FLUSH_ACCOUNTS):
                        flush_frag = {
                            "header": {
                                "operation": "INSERT", "timestamp": flush_ts,
                                "source": {"system": "FLUSH", "library": "FLUSH", "table": "FLUSH", "commit_lsn": "0"},
                                "transaction_id": f"TXN-FLUSH-{pi}-{uuid.uuid4().hex[:8]}",
                            },
                            "correlation": {
                                "key": f"{fc}-FLUSH-{uuid.uuid4().hex[:6]}",
                                "fragment_type": "HEADER", "fragment_sequence": 1, "total_fragments": 3,
                            },
                            "before": None,
                            "after": {"NUMCTA": fc, "TIPCTA": "AHO", "FECMOV": "20260101", "HORMOV": "000000",
                                      "CODCAN": "FLU", "CODSUC": "0000", "TIPMOV": "FLU", "CODTRN": "FL0000",
                                      "SECMOV": 0, "ESTTRN": "99"},
                            "_produced_at": flush_ts,
                        }
                        producer.produce(
                            KAFKA_TOPIC, key=fc.encode("utf-8"),
                            value=json.dumps(flush_frag).encode("utf-8"), partition=pi,
                        )
                    producer.flush(timeout=5)
                except Exception:
                    pass
            threading.Thread(target=_flush_kafka, daemon=True).start()

            direct_doc = _build_direct_doc()
            direct_doc["correlation_key"] = correlation_key
            db.movimientos.insert_one(direct_doc)

        except Exception as e:
            raise HTTPException(500, f"Error produciendo a Kafka: {str(e)}")

    else:
        modo = "direct_write"
        try:
            db.movimientos.insert_one(_build_direct_doc())
            fragmentos_enviados = 3
            delays = [{"fragment": "DIRECT", "delay_ms": 0}]
        except Exception as e:
            raise HTTPException(500, f"Error escribiendo a MongoDB: {str(e)}")

    elapsed_ms = round((time.time() - t0) * 1000, 1)

    return JSONResponse(content={
        "status": "ok",
        "demo_id": demo_id,
        "cuenta": numero,
        "transaccion": {
            "tipo": tipo_tx["cod"],
            "descripcion": tipo_tx["desc"],
            "monto": int(monto),
            "signo": tipo_tx["signo"],
            "canal": tipo_tx["canal"],
            "monto_formateado": f"${int(monto):,}".replace(",", "."),
        },
        "saldo_anterior": float(saldo_actual),
        "saldo_esperado": float(saldo_nuevo),
        "pipeline": {
            "modo": modo,
            "fragmentos_enviados": fragmentos_enviados,
            "delays": delays,
            "correlation_key": correlation_key,
            "topic": KAFKA_TOPIC if modo == "kafka" else "N/A",
        },
        "timestamp_envio": now.isoformat(),
        "api_time_ms": elapsed_ms,
        "_meta": {
            "instrucciones": (
                "La app debe hacer polling a GET /api/v1/cuentas/{cuenta}/saldo cada 300ms. "
                "Cuando el saldo cambie respecto a saldo_anterior, la transacción llegó al ODL."
            ),
            "tiempo_estimado_pipeline": "2-15s (Kafka → ASP tumblingWindow 10s → MongoDB write)" if modo == "kafka"
                else "<200ms (direct write)",
        },
    })


@router.get("/cuentas/{numero}/verificar-cambio")
async def verificar_cambio(
    numero: str,
    saldo_anterior: float = Query(..., description="Saldo antes de la simulación"),
    demo_id: Optional[str] = Query(None, description="ID de demo para verificación exacta"),
):
    """
    ⚡ Verifica si una transacción simulada ya llegó al ODL.

    Diseñado para polling eficiente desde la app:
    - Compara saldo actual vs saldo_anterior
    - Si cambió, devuelve el nuevo saldo y la latencia
    """
    t0 = time.time()
    db = get_db()
    if db is None:
        raise HTTPException(503, "MongoDB no disponible")

    ultimo = db.movimientos.find_one(
        {"cuenta.numero": numero},
        sort=[("movimiento.fecha_iso", -1)],
        projection={
            "monetario.saldo_nuevo": 1,
            "monetario.valor": 1,
            "monetario.signo": 1,
            "movimiento.tipo": 1,
            "movimiento.canal": 1,
            "movimiento.fecha_iso": 1,
            "_asp_metadata.e2e_latency_ms": 1,
            "_asp_metadata.assembly_latency_ms": 1,
            "_asp_metadata.demo_id": 1,
        },
    )

    saldo_actual = safe_number(ultimo.get("monetario", {}).get("saldo_nuevo")) if ultimo else 0
    cambio = abs(saldo_actual - saldo_anterior) > 0.01
    elapsed = round((time.time() - t0) * 1000, 1)

    result = {
        "cambio_detectado": cambio,
        "saldo_actual": float(saldo_actual),
        "saldo_anterior": float(saldo_anterior),
        "query_time_ms": elapsed,
    }

    if cambio and ultimo:
        asp_meta = ultimo.get("_asp_metadata", {})
        result["transaccion"] = {
            "tipo": ultimo.get("movimiento", {}).get("tipo"),
            "canal": ultimo.get("movimiento", {}).get("canal"),
            "monto": safe_number(ultimo.get("monetario", {}).get("valor")),
            "signo": ultimo.get("monetario", {}).get("signo"),
        }
        result["latencias"] = {
            "assembly_ms": safe_number(asp_meta.get("assembly_latency_ms")),
            "e2e_ms": safe_number(asp_meta.get("e2e_latency_ms")),
        }
        fecha = ultimo.get("movimiento", {}).get("fecha_iso")
        if fecha and hasattr(fecha, "isoformat"):
            result["transaccion"]["fecha"] = fecha.isoformat()
        if demo_id and asp_meta.get("demo_id") == demo_id:
            result["demo_match"] = True

    return JSONResponse(content=result)


# ═══ PARTITION PRIMER — Avanzar watermarks en TODAS las particiones ═══

# Cuentas dummy para priming — 6 cuentas diferentes para 6 particiones
# Estas cuentas NO existen en el ODL, así que los docs ensamblados no afectan
PRIMER_ACCOUNTS = [
    "99000000000001", "99000000000002", "99000000000003",
    "99000000000004", "99000000000005", "99000000000006",
]


@router.post("/odl/prime-pipeline")
async def prime_pipeline():
    """
    🔧 Envía fragmentos dummy a TODAS las particiones de Kafka.

    Resuelve el problema de watermark: cuando solo 1 partición recibe datos,
    ASP no puede avanzar el watermark global (min across partitions).
    Este endpoint "despierta" las 6 particiones para que el tumblingWindow
    pueda cerrar ventanas y emitir documentos.

    Ejecutar UNA VEZ antes de la primera simulación demo.
    """
    producer = get_producer()
    if not producer:
        raise HTTPException(503, "Kafka producer no disponible")

    sent = 0
    errors = []

    for i, cuenta in enumerate(PRIMER_ACCOUNTS):
        target_partition = i  # 0,1,2,3,4,5 — una cuenta por partición
        try:
            now_utc = datetime.utcnow()
            ts = now_utc.isoformat() + "Z"
            ck = f"{cuenta}-{now_utc.strftime('%Y%m%d')}-{now_utc.strftime('%H%M%S')}-PRM"
            txn_id = f"TXN-PRIME-{i:03d}-{uuid.uuid4().hex[:8].upper()}"

            for ftype, fseq in [("HEADER", 1), ("MONETARY", 2), ("METADATA", 3)]:
                frag = {
                    "header": {
                        "operation": "INSERT",
                        "timestamp": ts,
                        "source": {
                            "system": "AS400",
                            "library": "SCILIBRAMD",
                            "table": "SCIFFMRCMV",
                            "commit_lsn": _gen_lsn(),
                        },
                        "transaction_id": txn_id,
                    },
                    "correlation": {
                        "key": ck,
                        "fragment_type": ftype,
                        "fragment_sequence": fseq,
                        "total_fragments": 3,
                    },
                    "before": None,
                    "after": {
                        "NUMCTA": cuenta,
                        "TIPCTA": "AHO",
                        "FECMOV": now_utc.strftime("%Y%m%d"),
                        "HORMOV": now_utc.strftime("%H%M%S"),
                        "CODCAN": "PRM",
                        "CODSUC": "0001",
                        "TIPMOV": "PAY",
                        "CODTRN": "NM0001",
                        "SECMOV": fseq,
                        "ESTTRN": "00",
                    } if ftype == "HEADER" else {
                        "VALTRA": 1000, "CODMON": "COP",
                        "SLDANT": 1000000, "SLDNUE": 1001000,
                        "SIGNO": "C", "TASCAM": 1.0,
                        "VALORI": 1000, "MONORI": "COP",
                        "VALIVA": 0, "VALGMF": 0,
                    } if ftype == "MONETARY" else {
                        "NUMREF": f"REFPRIME{i}", "DESCRP": "Primer",
                        "CTADES": "", "BANDES": "", "NOMDES": "",
                        "NUMIDE": "00000000", "TIPIDE": "CC",
                        "IPORIG": "10.0.0.1", "USERAG": "Primer/1.0",
                        "LATGEO": 6.25, "LONGGEO": -75.56,
                        "DISPOS": "Primer", "SESION": "SESPRIMER",
                    },
                    "_produced_at": now_utc.isoformat() + "Z",
                }

                # Enviar con partition explícita para garantizar distribución
                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=json.dumps(frag).encode("utf-8"),
                    key=ck.encode("utf-8"),
                    partition=target_partition,
                )
                sent += 1
                producer.poll(0)

        except Exception as e:
            errors.append(f"Partition {target_partition}: {str(e)}")

    producer.flush(timeout=10)

    return JSONResponse(content={
        "status": "ok",
        "fragments_sent": sent,
        "partitions_primed": len(PRIMER_ACCOUNTS),
        "errors": errors if errors else None,
        "instrucciones": (
            "Espera ~15 segundos para que ASP procese los primers y "
            "avance el watermark. Luego ejecuta la simulación normalmente."
        ),
    })


# ═══ STANDALONE MODE ═══
if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(
        title="Bancolombia ODL — Simulación Read-Your-Writes",
        description="Endpoint para simular transacciones en vivo durante el TFW",
        version="2.0.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)

    @app.get("/")
    def root():
        return {
            "service": "Bancolombia ODL — Read-Your-Writes v2",
            "kafka_configured": bool(KAFKA_BOOTSTRAP),
            "kafka_topic": KAFKA_TOPIC,
            "mongodb_configured": bool(MONGODB_URI),
            "endpoints": [
                "POST /api/v1/odl/prime-pipeline  ← EJECUTAR PRIMERO",
                "POST /api/v1/cuentas/{numero}/simular-movimiento",
                "GET  /api/v1/cuentas/{numero}/verificar-cambio?saldo_anterior=X",
            ],
        }

    uvicorn.run(app, host="0.0.0.0", port=8789)
