"""
Modelo SCIFFMRCMV - Tabla de Movimientos del AS/400
====================================================
Simula la estructura de la tabla de movimientos de depósitos
de Bancolombia con ~200 campos (realista al iSeries).

Esta tabla tiene la característica de que los datos llegan
FRAGMENTADOS temporalmente desde el CDC del AS/400.

Los ~200 campos representan 30 años de deuda técnica acumulada
que será transformada a ~30 campos limpios ISO en el ODL.
"""

import random
import string
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from enum import Enum


# =============================================================================
# ENUMS Y CONSTANTES
# =============================================================================

class TipoMovimiento(Enum):
    """Tipos de movimiento bancario."""
    TRF_OUT = ("TRF", "D", "Transferencia Saliente", 0.25)
    TRF_IN = ("TRF", "C", "Transferencia Entrante", 0.25)
    DEP = ("DEP", "C", "Consignación", 0.15)
    WDR = ("WDR", "D", "Retiro", 0.15)
    PSE = ("PSE", "D", "Pago PSE", 0.10)
    PAY = ("PAY", "C", "Pago Nómina", 0.05)
    INT = ("INT", "C", "Abono Intereses", 0.03)
    FEE = ("FEE", "D", "Cobro Comisión", 0.02)
    
    @property
    def codigo(self) -> str:
        return self.value[0]
    
    @property
    def signo(self) -> str:
        return self.value[1]
    
    @property
    def descripcion(self) -> str:
        return self.value[2]
    
    @property
    def probabilidad(self) -> float:
        return self.value[3]


class Canal(Enum):
    """Canales de origen de la transacción."""
    APP = ("APP", "Aplicación Móvil", 0.45)
    WEB = ("WEB", "Banca Web", 0.25)
    ATM = ("ATM", "Cajero Automático", 0.15)
    SUC = ("SUC", "Sucursal", 0.10)
    API = ("API", "API Terceros", 0.05)
    
    @property
    def codigo(self) -> str:
        return self.value[0]
    
    @property
    def descripcion(self) -> str:
        return self.value[1]
    
    @property
    def probabilidad(self) -> float:
        return self.value[2]


class FragmentType(Enum):
    """Tipos de fragmento para el CDC."""
    HEADER = "HEADER"
    MONETARY = "MONETARY"
    METADATA = "METADATA"


# Códigos de transacción por tipo (simulando códigos legacy del AS/400)
CODIGOS_TRANSACCION = {
    "TRF": ["TR0001", "TR0002", "TR0003", "TR0010", "TR0015"],
    "DEP": ["DP0001", "DP0002", "DP0005"],
    "WDR": ["RT0001", "RT0002", "RT0003"],
    "PSE": ["PS0001", "PS0002"],
    "PAY": ["NM0001", "NM0002"],
    "INT": ["IN0001"],
    "FEE": ["CM0001", "CM0002", "CM0003"],
}

# Bancos destino para transferencias
BANCOS = [
    ("007", "BANCOLOMBIA"),
    ("001", "BANCO DE BOGOTA"),
    ("002", "BANCO POPULAR"),
    ("006", "BANCO CORPBANCA"),
    ("009", "CITIBANK"),
    ("012", "BANCO GNB SUDAMERIS"),
    ("013", "BBVA COLOMBIA"),
    ("019", "SCOTIABANK"),
    ("023", "BANCO DE OCCIDENTE"),
    ("051", "DAVIVIENDA"),
    ("052", "BANCO AV VILLAS"),
    ("058", "BANCO PROCREDIT"),
    ("060", "BANCO PICHINCHA"),
    ("061", "BANCOOMEVA"),
    ("062", "BANCO FALABELLA"),
    ("063", "BANCO FINANDINA"),
    ("065", "BANCO SANTANDER"),
    ("066", "BANCO COOPERATIVO"),
]


# =============================================================================
# UTILIDADES
# =============================================================================

def seleccionar_por_peso(opciones: List[Enum]) -> Enum:
    """Selecciona una opción basada en sus probabilidades."""
    r = random.random()
    acumulado = 0.0
    for opcion in opciones:
        acumulado += opcion.probabilidad
        if r <= acumulado:
            return opcion
    return opciones[-1]


def generar_monto() -> float:
    """Genera un monto con distribución log-normal realista."""
    monto = random.lognormvariate(mu=12, sigma=1.8)
    if monto > 100000:
        monto = round(monto / 10000) * 10000
    elif monto > 10000:
        monto = round(monto / 1000) * 1000
    else:
        monto = round(monto / 100) * 100
    return max(1000, min(monto, 500_000_000))


def generar_referencia() -> str:
    """Genera número de referencia único."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_part = random.randint(100000, 999999)
    return f"REF{timestamp}{random_part}"


def generar_ip() -> str:
    """Genera dirección IP de origen."""
    rangos = [
        (10, random.randint(0, 255), random.randint(0, 255), random.randint(1, 254)),
        (172, random.randint(16, 31), random.randint(0, 255), random.randint(1, 254)),
        (192, 168, random.randint(0, 255), random.randint(1, 254)),
    ]
    ip = random.choice(rangos)
    return f"{ip[0]}.{ip[1]}.{ip[2]}.{ip[3]}"


def generar_string_random(longitud: int) -> str:
    """Genera string aleatorio (simulando datos legacy)."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=longitud))


def generar_codigo_legacy(prefijo: str, longitud: int = 6) -> str:
    """Genera código legacy con prefijo."""
    return f"{prefijo}{random.randint(0, 10**(longitud-len(prefijo))-1):0{longitud-len(prefijo)}d}"


# =============================================================================
# GENERADOR DE CAMPOS LEGACY
# =============================================================================

class LegacyFieldsGenerator:
    """
    Genera los ~200 campos legacy que simula el iSeries.
    Estos campos representan 30 años de deuda técnica.
    """
    
    @staticmethod
    def generate_header_legacy() -> Dict[str, Any]:
        """
        Genera campos legacy del fragmento HEADER (~60 campos adicionales).
        Estos campos son típicamente de identificación y routing.
        """
        return {
            # === CAMPOS DE IDENTIFICACIÓN LEGACY (20 campos) ===
            "CODENT": generar_codigo_legacy("E", 4),
            "CODPAI": "CO",
            "CODREG": f"{random.randint(1, 32):02d}",
            "CODCIU": f"{random.randint(1, 999):03d}",
            "CODZON": f"{random.randint(1, 99):02d}",
            "CODSEC": generar_codigo_legacy("S", 5),
            "CODARE": generar_codigo_legacy("A", 4),
            "CODUNI": generar_codigo_legacy("U", 6),
            "CODCEN": generar_codigo_legacy("C", 5),
            "CODPRO": generar_codigo_legacy("P", 4),
            "CODLIN": generar_codigo_legacy("L", 3),
            "CODSUB": generar_codigo_legacy("SB", 4),
            "CODMOD": generar_codigo_legacy("M", 3),
            "CODPLA": generar_codigo_legacy("PL", 4),
            "CODTAR": generar_codigo_legacy("T", 5),
            "CODCONV": generar_codigo_legacy("CV", 6),
            "CODCAM": generar_codigo_legacy("CM", 4),
            "CODSEG": generar_codigo_legacy("SG", 3),
            "CODNIC": generar_codigo_legacy("N", 4),
            "CODCLI": generar_codigo_legacy("CLI", 8),
            
            # === CAMPOS DE ROUTING LEGACY (15 campos) ===
            "RUTEFEC": datetime.now().strftime("%Y%m%d"),
            "RUTEHOR": datetime.now().strftime("%H%M%S"),
            "RUTEORI": generar_codigo_legacy("RO", 6),
            "RUTEDES": generar_codigo_legacy("RD", 6),
            "RUTEINT": generar_codigo_legacy("RI", 6),
            "RUTECOD": generar_codigo_legacy("RC", 8),
            "RUTESEQ": random.randint(1, 999999),
            "RUTEPRI": random.randint(1, 9),
            "RUTEFLG": random.choice(["Y", "N"]),
            "RUTEEST": f"{random.randint(0, 99):02d}",
            "RUTERET": random.randint(0, 5),
            "RUTETIM": random.randint(100, 9999),
            "RUTEQUE": generar_codigo_legacy("Q", 4),
            "RUTEPOL": generar_codigo_legacy("POL", 5),
            "RUTEBAL": generar_codigo_legacy("BAL", 4),
            
            # === CAMPOS DE SISTEMA LEGACY (15 campos) ===
            "SISTCOD": generar_codigo_legacy("SYS", 5),
            "SISTVER": f"{random.randint(1,9)}.{random.randint(0,99)}",
            "SISTMOD": generar_codigo_legacy("MOD", 4),
            "SISTENV": random.choice(["PRD", "QAS", "DEV"]),
            "SISTNOD": f"NODE{random.randint(1,99):02d}",
            "SISTCLU": f"CLU{random.randint(1,9)}",
            "SISTPAR": f"PAR{random.randint(1,99):02d}",
            "SISTJOB": generar_codigo_legacy("JOB", 8),
            "SISTTSK": generar_codigo_legacy("TSK", 6),
            "SISTPGM": generar_codigo_legacy("PGM", 10),
            "SISTLIB": generar_codigo_legacy("LIB", 10),
            "SISTFIL": generar_codigo_legacy("FIL", 10),
            "SISTMBR": generar_codigo_legacy("MBR", 10),
            "SISTUSR": generar_codigo_legacy("USR", 10),
            "SISTPRF": generar_codigo_legacy("PRF", 10),
            
            # === FILLERS Y RESERVADOS (10 campos) ===
            "FILLER_HDR_01": " " * 10,
            "FILLER_HDR_02": " " * 20,
            "FILLER_HDR_03": " " * 15,
            "FILLER_HDR_04": " " * 8,
            "FILLER_HDR_05": " " * 12,
            "RESERV_HDR_01": "0" * 10,
            "RESERV_HDR_02": "0" * 8,
            "RESERV_HDR_03": "0" * 6,
            "RESERV_HDR_04": "0" * 4,
            "RESERV_HDR_05": "0" * 12,
        }
    
    @staticmethod
    def generate_monetary_legacy() -> Dict[str, Any]:
        """
        Genera campos legacy del fragmento MONETARY (~70 campos adicionales).
        Estos campos son de cálculos financieros históricos.
        """
        base_monto = random.uniform(1000, 10000000)
        
        return {
            # === CAMPOS MONETARIOS HISTÓRICOS (25 campos) ===
            "MTOBRUT": base_monto,
            "MTONETO": base_monto * 0.96,
            "MTOBASE": base_monto * 0.95,
            "MTOAJUS": base_monto * 0.02,
            "MTODESC": base_monto * 0.01,
            "MTORECA": base_monto * 0.005,
            "MTOBONI": 0,
            "MTORETE": base_monto * 0.004,
            "MTOIVA1": base_monto * 0.19,
            "MTOIVA2": 0,
            "MTOIVA3": 0,
            "MTOEXEN": 0,
            "MTOGRAV": base_monto,
            "MTONOAP": 0,
            "MTOCOMI": base_monto * 0.003,
            "MTOSEGU": base_monto * 0.001,
            "MTOINTE": base_monto * 0.005,
            "MTOMORA": 0,
            "MTOMULT": 0,
            "MTOINCA": 0,
            "MTOVACA": 0,
            "MTOPRES": 0,
            "MTOAPEN": 0,
            "MTOASAL": 0,
            "MTOARSG": 0,
            
            # === CAMPOS DE TASAS (15 campos) ===
            "TASNOM": round(random.uniform(0.01, 0.25), 6),
            "TASEFE": round(random.uniform(0.01, 0.30), 6),
            "TASMOR": round(random.uniform(0.01, 0.05), 6),
            "TASPEN": round(random.uniform(0.001, 0.02), 6),
            "TASBON": 0,
            "TASDSC": round(random.uniform(0.001, 0.05), 6),
            "TASIVA": 0.19,
            "TASRET": 0.04,
            "TASRFT": 0.004,
            "TASGMF": 0.004,
            "TASCRE": round(random.uniform(0.01, 0.20), 6),
            "TASDEB": round(random.uniform(0.001, 0.01), 6),
            "TASAHF": round(random.uniform(0.01, 0.08), 6),
            "TASCDT": round(random.uniform(0.03, 0.12), 6),
            "TASDTF": round(random.uniform(0.05, 0.15), 6),
            
            # === CAMPOS DE SALDOS HISTÓRICOS (15 campos) ===
            "SLDINI": random.uniform(0, 100000000),
            "SLDFIN": random.uniform(0, 100000000),
            "SLDMED": random.uniform(0, 50000000),
            "SLDMIN": random.uniform(0, 10000000),
            "SLDMAX": random.uniform(10000000, 200000000),
            "SLDPRO": random.uniform(0, 50000000),
            "SLDDIS": random.uniform(0, 100000000),
            "SLDRES": random.uniform(0, 10000000),
            "SLDPIG": random.uniform(0, 5000000),
            "SLDEMB": 0,
            "SLDCAJ": random.uniform(0, 1000000),
            "SLDREM": random.uniform(0, 500000),
            "SLDPDT": 0,
            "SLDCOB": 0,
            "SLDPAG": 0,
            
            # === CAMPOS DE INDICADORES FINANCIEROS (10 campos) ===
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
            
            # === FILLERS MONETARIOS (5 campos) ===
            "FILLER_MON_01": " " * 15,
            "FILLER_MON_02": " " * 10,
            "FILLER_MON_03": "0" * 18,
            "FILLER_MON_04": "0" * 12,
            "FILLER_MON_05": " " * 20,
        }
    
    @staticmethod
    def generate_metadata_legacy() -> Dict[str, Any]:
        """
        Genera campos legacy del fragmento METADATA (~70 campos adicionales).
        Estos campos son de auditoría, tracking y datos históricos.
        """
        ahora = datetime.now()
        
        return {
            # === CAMPOS DE AUDITORÍA (20 campos) ===
            "AUDFCRE": (ahora - timedelta(days=random.randint(0, 365))).strftime("%Y%m%d"),
            "AUDHCRE": ahora.strftime("%H%M%S"),
            "AUDUSRC": generar_codigo_legacy("USR", 10),
            "AUDPGMC": generar_codigo_legacy("PGM", 10),
            "AUDFMOD": ahora.strftime("%Y%m%d"),
            "AUDHMOD": ahora.strftime("%H%M%S"),
            "AUDUSRM": generar_codigo_legacy("USR", 10),
            "AUDPGMM": generar_codigo_legacy("PGM", 10),
            "AUDFELI": "00000000",
            "AUDHELI": "000000",
            "AUDUSRE": "",
            "AUDPGME": "",
            "AUDSECC": random.randint(1, 999999),
            "AUDSECM": random.randint(1, 999999),
            "AUDVERS": random.randint(1, 99),
            "AUDESTA": random.choice(["A", "I", "P", "E"]),
            "AUDTIPO": random.choice(["N", "M", "E", "C"]),
            "AUDORIG": generar_codigo_legacy("ORI", 6),
            "AUDDEST": generar_codigo_legacy("DES", 6),
            "AUDOBS": generar_string_random(50),
            
            # === CAMPOS DE TRACKING (15 campos) ===
            "TRKUUID": generar_string_random(32),
            "TRKCORR": generar_string_random(24),
            "TRKSPAN": generar_string_random(16),
            "TRKPARENT": generar_string_random(16),
            "TRKROOT": generar_string_random(16),
            "TRKSVC": generar_codigo_legacy("SVC", 10),
            "TRKOPER": generar_codigo_legacy("OPR", 8),
            "TRKVER": f"v{random.randint(1,5)}.{random.randint(0,9)}",
            "TRKENV": random.choice(["prd", "stg", "dev"]),
            "TRKHOST": f"host{random.randint(1,99):02d}",
            "TRKPOD": f"pod-{generar_string_random(8).lower()}",
            "TRKNS": random.choice(["default", "banking", "core"]),
            "TRKLAT": random.randint(1, 5000),
            "TRKSTA": random.choice(["OK", "ERR", "WARN"]),
            "TRKCOD": f"{random.randint(200, 599)}",
            
            # === CAMPOS DE DATOS DE CLIENTE LEGACY (15 campos) ===
            "CLINOMB": generar_string_random(30),
            "CLIAPE1": generar_string_random(20),
            "CLIAPE2": generar_string_random(20),
            "CLIDIR1": generar_string_random(40),
            "CLIDIR2": generar_string_random(40),
            "CLIBARR": generar_string_random(20),
            "CLICIUD": generar_string_random(20),
            "CLIDEPA": generar_string_random(20),
            "CLIPAIS": "COLOMBIA",
            "CLITEL1": f"3{random.randint(100000000, 999999999)}",
            "CLITEL2": f"6{random.randint(10000000, 99999999)}",
            "CLIEMAI": f"cliente{random.randint(1000,9999)}@email.com",
            "CLIPROF": generar_codigo_legacy("PRO", 4),
            "CLIOCUP": generar_codigo_legacy("OCU", 4),
            "CLIACTV": generar_codigo_legacy("ACT", 4),
            
            # === CAMPOS HISTÓRICOS MIGRACIONES (10 campos) ===
            "MIGFEC1": "19950615",
            "MIGFEC2": "20010301",
            "MIGFEC3": "20080915",
            "MIGFEC4": "20151120",
            "MIGFEC5": "20200601",
            "MIGCOD1": generar_codigo_legacy("M95", 8),
            "MIGCOD2": generar_codigo_legacy("M01", 8),
            "MIGCOD3": generar_codigo_legacy("M08", 8),
            "MIGCOD4": generar_codigo_legacy("M15", 8),
            "MIGCOD5": generar_codigo_legacy("M20", 8),
            
            # === CAMPOS DE SISTEMAS LEGACY ANTERIORES (10 campos) ===
            "LEGSIS1": generar_codigo_legacy("SIS1", 10),
            "LEGSIS2": generar_codigo_legacy("SIS2", 10),
            "LEGSIS3": generar_codigo_legacy("SIS3", 10),
            "LEGCOD1": generar_codigo_legacy("LC1", 15),
            "LEGCOD2": generar_codigo_legacy("LC2", 15),
            "LEGCOD3": generar_codigo_legacy("LC3", 15),
            "LEGREF1": generar_codigo_legacy("LR1", 20),
            "LEGREF2": generar_codigo_legacy("LR2", 20),
            "LEGFLG1": random.choice(["Y", "N", "P", "X"]),
            "LEGFLG2": random.choice(["Y", "N", "P", "X"]),
            
            # === FILLERS METADATA (largos, típicos de AS/400) ===
            "FILLER_MET_01": " " * 30,
            "FILLER_MET_02": " " * 50,
            "FILLER_MET_03": " " * 20,
            "FILLER_MET_04": "0" * 25,
            "FILLER_MET_05": " " * 40,
            "FILLER_MET_06": " " * 15,
            "FILLER_MET_07": "0" * 10,
            "FILLER_MET_08": " " * 25,
            "FILLER_MET_09": " " * 35,
            "FILLER_MET_10": "0" * 20,
        }


# =============================================================================
# DATACLASS PRINCIPAL - MOVIMIENTO SCIFFMRCMV (~200 campos)
# =============================================================================

@dataclass
class MovimientoSCIFFMRCMV:
    """
    Representa un movimiento completo de la tabla SCIFFMRCMV.
    TOTAL: ~200 campos simulando 30 años de deuda técnica del iSeries.
    """
    # === CAMPOS DE CORRELACIÓN ===
    correlation_key: str = ""
    
    # === FRAGMENTO 1: HEADER ===
    NUMCTA: str = ""
    TIPCTA: str = ""
    FECMOV: str = ""
    HORMOV: str = ""
    CODCAN: str = ""
    CODSUC: str = ""
    TIPMOV: str = ""
    CODTRN: str = ""
    SECMOV: int = 0
    ESTTRN: str = "00"
    header_legacy: Dict[str, Any] = field(default_factory=dict)
    
    # === FRAGMENTO 2: MONETARY ===
    VALTRA: float = 0.0
    CODMON: str = "COP"
    SLDANT: float = 0.0
    SLDNUE: float = 0.0
    SIGNO: str = ""
    TASCAM: float = 1.0
    VALORI: float = 0.0
    MONORI: str = "COP"
    VALIVA: float = 0.0
    VALGMF: float = 0.0
    monetary_legacy: Dict[str, Any] = field(default_factory=dict)
    
    # === FRAGMENTO 3: METADATA ===
    NUMREF: str = ""
    DESCRP: str = ""
    CTADES: str = ""
    BANDES: str = ""
    NOMDES: str = ""
    NUMIDE: str = ""
    TIPIDE: str = ""
    IPORIG: str = ""
    USERAG: str = ""
    LATGEO: float = 0.0
    LONGGEO: float = 0.0
    DISPOS: str = ""
    SESION: str = ""
    metadata_legacy: Dict[str, Any] = field(default_factory=dict)
    
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        if not self.correlation_key and self.NUMCTA:
            self.correlation_key = f"{self.NUMCTA}-{self.FECMOV}-{self.HORMOV}-{self.CODCAN}"
    
    def to_fragment_header(self) -> Dict[str, Any]:
        """Retorna campos del fragmento HEADER (~70 campos)."""
        core = {
            "NUMCTA": self.NUMCTA,
            "TIPCTA": self.TIPCTA,
            "FECMOV": self.FECMOV,
            "HORMOV": self.HORMOV,
            "CODCAN": self.CODCAN,
            "CODSUC": self.CODSUC,
            "TIPMOV": self.TIPMOV,
            "CODTRN": self.CODTRN,
            "SECMOV": self.SECMOV,
            "ESTTRN": self.ESTTRN,
        }
        return {**core, **self.header_legacy}
    
    def to_fragment_monetary(self) -> Dict[str, Any]:
        """Retorna campos del fragmento MONETARY (~80 campos)."""
        core = {
            "VALTRA": self.VALTRA,
            "CODMON": self.CODMON,
            "SLDANT": self.SLDANT,
            "SLDNUE": self.SLDNUE,
            "SIGNO": self.SIGNO,
            "TASCAM": self.TASCAM,
            "VALORI": self.VALORI,
            "MONORI": self.MONORI,
            "VALIVA": self.VALIVA,
            "VALGMF": self.VALGMF,
        }
        return {**core, **self.monetary_legacy}
    
    def to_fragment_metadata(self) -> Dict[str, Any]:
        """Retorna campos del fragmento METADATA (~83 campos)."""
        core = {
            "NUMREF": self.NUMREF,
            "DESCRP": self.DESCRP,
            "CTADES": self.CTADES,
            "BANDES": self.BANDES,
            "NOMDES": self.NOMDES,
            "NUMIDE": self.NUMIDE,
            "TIPIDE": self.TIPIDE,
            "IPORIG": self.IPORIG,
            "USERAG": self.USERAG,
            "LATGEO": self.LATGEO,
            "LONGGEO": self.LONGGEO,
            "DISPOS": self.DISPOS,
            "SESION": self.SESION,
        }
        return {**core, **self.metadata_legacy}
    
    def get_total_fields_count(self) -> Dict[str, int]:
        """Retorna conteo de campos por fragmento."""
        return {
            "header": len(self.to_fragment_header()),
            "monetary": len(self.to_fragment_monetary()),
            "metadata": len(self.to_fragment_metadata()),
            "total": (len(self.to_fragment_header()) + 
                     len(self.to_fragment_monetary()) + 
                     len(self.to_fragment_metadata()))
        }


# =============================================================================
# CDC FRAGMENT
# =============================================================================

@dataclass
class CDCFragment:
    """Fragmento CDC para Kafka."""
    operation: str = "INSERT"
    timestamp: str = ""
    source_system: str = "AS400"
    source_library: str = "SCILIBRAMD"
    source_table: str = "SCIFFMRCMV"
    commit_lsn: str = ""
    transaction_id: str = ""
    
    correlation_key: str = ""
    fragment_type: str = ""
    fragment_sequence: int = 0
    total_fragments: int = 3
    
    before: Optional[Dict[str, Any]] = None
    after: Dict[str, Any] = field(default_factory=dict)
    
    def to_kafka_message(self) -> Dict[str, Any]:
        return {
            "header": {
                "operation": self.operation,
                "timestamp": self.timestamp,
                "source": {
                    "system": self.source_system,
                    "library": self.source_library,
                    "table": self.source_table,
                    "commit_lsn": self.commit_lsn,
                },
                "transaction_id": self.transaction_id,
            },
            "correlation": {
                "key": self.correlation_key,
                "fragment_type": self.fragment_type,
                "fragment_sequence": self.fragment_sequence,
                "total_fragments": self.total_fragments,
            },
            "before": self.before,
            "after": self.after,
        }


# =============================================================================
# FACTORY DE MOVIMIENTOS
# =============================================================================

class MovimientoFactory:
    """Fábrica para crear movimientos con ~200 campos."""
    
    def __init__(self, cuentas: List[Dict[str, Any]]):
        self.cuentas = cuentas
        self.secuencial = 0
        self.legacy_generator = LegacyFieldsGenerator()
    
    def crear_movimiento(self) -> MovimientoSCIFFMRCMV:
        """Crea un movimiento aleatorio completo con ~200 campos."""
        cuenta = random.choice(self.cuentas)
        tipo_mov = seleccionar_por_peso(list(TipoMovimiento))
        canal = seleccionar_por_peso(list(Canal))
        
        ahora = datetime.now()
        fecha = ahora.strftime("%Y%m%d")
        hora = ahora.strftime("%H%M%S")
        
        self.secuencial += 1
        
        monto = generar_monto()
        saldo_anterior = cuenta.get("saldo_disponible", random.uniform(100000, 10000000))
        
        if tipo_mov.signo == "D":
            saldo_nuevo = saldo_anterior - monto
        else:
            saldo_nuevo = saldo_anterior + monto
        
        gmf = monto * 0.004 if tipo_mov.signo == "D" and not cuenta.get("exento_gmf", False) else 0
        
        es_transferencia = tipo_mov.codigo == "TRF"
        banco_destino = random.choice(BANCOS) if es_transferencia else ("", "")
        
        mov = MovimientoSCIFFMRCMV(
            NUMCTA=cuenta["_id"],
            TIPCTA=cuenta.get("tipo_cuenta", "AHO"),
            FECMOV=fecha,
            HORMOV=hora,
            CODCAN=canal.codigo,
            CODSUC=cuenta.get("sucursal_apertura", "0001"),
            TIPMOV=tipo_mov.codigo,
            CODTRN=random.choice(CODIGOS_TRANSACCION.get(tipo_mov.codigo, ["XX0001"])),
            SECMOV=self.secuencial,
            ESTTRN="00",
            header_legacy=self.legacy_generator.generate_header_legacy(),
            
            VALTRA=monto,
            CODMON="COP",
            SLDANT=saldo_anterior,
            SLDNUE=saldo_nuevo,
            SIGNO=tipo_mov.signo,
            TASCAM=1.0,
            VALORI=monto,
            MONORI="COP",
            VALIVA=0,
            VALGMF=gmf,
            monetary_legacy=self.legacy_generator.generate_monetary_legacy(),
            
            NUMREF=generar_referencia(),
            DESCRP=tipo_mov.descripcion[:80],
            CTADES=f"4000{random.randint(100000, 999999):06d}" if es_transferencia else "",
            BANDES=banco_destino[0],
            NOMDES=f"DESTINATARIO {random.randint(1, 9999)}" if es_transferencia else "",
            NUMIDE=cuenta.get("identificacion_cliente", ""),
            TIPIDE=cuenta.get("tipo_identificacion", "CC"),
            IPORIG=generar_ip(),
            USERAG=f"BancolombiaApp/{random.randint(5,9)}.{random.randint(0,9)}.{random.randint(0,99)}",
            LATGEO=random.uniform(4.5, 11.0),
            LONGGEO=random.uniform(-77.0, -72.0),
            DISPOS=random.choice(["iPhone14", "iPhone15", "SamsungS23", "SamsungA54", "XiaomiNote12", "Web"]),
            SESION=f"SES{ahora.strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}",
            metadata_legacy=self.legacy_generator.generate_metadata_legacy(),
            
            created_at=ahora,
        )
        
        mov.correlation_key = f"{mov.NUMCTA}-{mov.FECMOV}-{mov.HORMOV}-{mov.CODCAN}"
        
        return mov


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    import json
    
    print("🧪 Testing Modelo SCIFFMRCMV con ~200 campos legacy")
    print("=" * 60)
    
    cuenta_test = {
        "_id": "40001234567890",
        "tipo_cuenta": "AHO",
        "saldo_disponible": 5_000_000,
        "sucursal_apertura": "0001",
        "identificacion_cliente": "12345678",
        "tipo_identificacion": "CC",
        "exento_gmf": False,
    }
    
    factory = MovimientoFactory([cuenta_test])
    mov = factory.crear_movimiento()
    
    counts = mov.get_total_fields_count()
    print(f"\n📊 CONTEO DE CAMPOS:")
    print(f"   HEADER:   {counts['header']} campos")
    print(f"   MONETARY: {counts['monetary']} campos")
    print(f"   METADATA: {counts['metadata']} campos")
    print(f"   ─────────────────────────")
    print(f"   TOTAL:    {counts['total']} campos")
    
    print(f"\n✅ Modelo listo para generar {counts['total']} campos por transacción")
