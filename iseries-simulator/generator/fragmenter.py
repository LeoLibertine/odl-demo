"""
Fragmenter - División de transacciones en fragmentos CDC
========================================================
Implementa la lógica de fragmentación temporal que simula
cómo los datos llegan del AS/400 de Bancolombia.

Características:
- Divide cada transacción en 3 fragmentos (HEADER, MONETARY, METADATA)
- Aplica delays aleatorios entre fragmentos
- Inyecta anomalías (incompletos, duplicados, desordenados)
"""

import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional
from enum import Enum

from generator.models.sciffmrcmv import (
    MovimientoSCIFFMRCMV,
    CDCFragment,
    FragmentType,
)


class AnomalyType(Enum):
    """Tipos de anomalías a inyectar."""
    NONE = "NONE"
    INCOMPLETE = "INCOMPLETE"       # Falta un fragmento
    DUPLICATE = "DUPLICATE"         # Fragmento duplicado
    OUT_OF_ORDER = "OUT_OF_ORDER"   # Orden alterado


@dataclass
class ScheduledFragment:
    """
    Fragmento programado para emisión.
    Incluye el delay relativo para ordenar la emisión.
    """
    fragment: CDCFragment
    delay_ms: int                   # Delay en milisegundos desde t=0
    emit_at: float = 0.0            # Timestamp absoluto de emisión
    anomaly: AnomalyType = AnomalyType.NONE
    is_duplicate: bool = False
    
    def __lt__(self, other):
        """Para ordenar por tiempo de emisión."""
        return self.emit_at < other.emit_at


class Fragmenter:
    """
    Fragmentador de transacciones.
    Divide movimientos en fragmentos CDC con delays temporales.
    """
    
    def __init__(
        self,
        delay_monetary_min: int = 50,
        delay_monetary_max: int = 300,
        delay_metadata_min: int = 100,
        delay_metadata_max: int = 500,
        pct_incomplete: float = 0.03,
        pct_duplicate: float = 0.02,
        pct_out_of_order: float = 0.10,
    ):
        """
        Inicializa el fragmentador.
        
        Args:
            delay_monetary_min/max: Rango de delay para fragmento MONETARY (ms)
            delay_metadata_min/max: Rango de delay para fragmento METADATA (ms)
            pct_incomplete: Porcentaje de transacciones con fragmento faltante
            pct_duplicate: Porcentaje de transacciones con fragmento duplicado
            pct_out_of_order: Porcentaje de transacciones con orden alterado
        """
        self.delay_monetary_min = delay_monetary_min
        self.delay_monetary_max = delay_monetary_max
        self.delay_metadata_min = delay_metadata_min
        self.delay_metadata_max = delay_metadata_max
        self.pct_incomplete = pct_incomplete
        self.pct_duplicate = pct_duplicate
        self.pct_out_of_order = pct_out_of_order
        
        # Contadores de estadísticas
        self.stats = {
            "total_transactions": 0,
            "total_fragments": 0,
            "anomalies": {
                "incomplete": 0,
                "duplicate": 0,
                "out_of_order": 0,
            }
        }
        
        # LSN counter para simular CDC
        self._lsn_counter = 0
    
    def _generate_lsn(self) -> str:
        """Genera un Log Sequence Number simulado."""
        self._lsn_counter += 1
        return f"{self._lsn_counter:08X}:{random.randint(0, 0xFFFF):04X}:0001"
    
    def _generate_transaction_id(self) -> str:
        """Genera un ID de transacción único."""
        return f"TXN-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
    
    def _determine_anomaly(self) -> AnomalyType:
        """
        Determina qué tipo de anomalía (si alguna) aplicar a la transacción.
        Solo una anomalía por transacción.
        """
        r = random.random()
        
        if r < self.pct_incomplete:
            return AnomalyType.INCOMPLETE
        elif r < self.pct_incomplete + self.pct_duplicate:
            return AnomalyType.DUPLICATE
        elif r < self.pct_incomplete + self.pct_duplicate + self.pct_out_of_order:
            return AnomalyType.OUT_OF_ORDER
        
        return AnomalyType.NONE
    
    def _create_cdc_fragment(
        self,
        movimiento: MovimientoSCIFFMRCMV,
        fragment_type: FragmentType,
        sequence: int,
        transaction_id: str,
    ) -> CDCFragment:
        """Crea un fragmento CDC a partir de un movimiento."""
        
        # Seleccionar los datos según el tipo de fragmento
        if fragment_type == FragmentType.HEADER:
            after_data = movimiento.to_fragment_header()
        elif fragment_type == FragmentType.MONETARY:
            after_data = movimiento.to_fragment_monetary()
        else:
            after_data = movimiento.to_fragment_metadata()
        
        return CDCFragment(
            operation="INSERT",
            timestamp=datetime.utcnow().isoformat() + "Z",
            source_system="AS400",
            source_library="SCILIBRAMD",
            source_table="SCIFFMRCMV",
            commit_lsn=self._generate_lsn(),
            transaction_id=transaction_id,
            correlation_key=movimiento.correlation_key,
            fragment_type=fragment_type.value,
            fragment_sequence=sequence,
            total_fragments=3,
            before=None,
            after=after_data,
        )
    
    def fragment(
        self,
        movimiento: MovimientoSCIFFMRCMV,
        base_time: float,
    ) -> List[ScheduledFragment]:
        """
        Fragmenta un movimiento en fragmentos CDC programados.
        
        Args:
            movimiento: El movimiento a fragmentar
            base_time: Timestamp base para calcular tiempos de emisión
            
        Returns:
            Lista de fragmentos programados
        """
        self.stats["total_transactions"] += 1
        
        transaction_id = self._generate_transaction_id()
        anomaly = self._determine_anomaly()
        
        # Calcular delays para cada fragmento
        delay_header = 0
        delay_monetary = random.randint(self.delay_monetary_min, self.delay_monetary_max)
        delay_metadata = random.randint(self.delay_metadata_min, self.delay_metadata_max)
        
        # Crear los tres fragmentos
        fragments = []
        
        # HEADER (siempre se emite)
        header = self._create_cdc_fragment(
            movimiento, FragmentType.HEADER, 1, transaction_id
        )
        fragments.append(ScheduledFragment(
            fragment=header,
            delay_ms=delay_header,
            emit_at=base_time + (delay_header / 1000),
            anomaly=AnomalyType.NONE,
        ))
        
        # MONETARY
        monetary = self._create_cdc_fragment(
            movimiento, FragmentType.MONETARY, 2, transaction_id
        )
        
        # METADATA
        metadata = self._create_cdc_fragment(
            movimiento, FragmentType.METADATA, 3, transaction_id
        )
        
        # Aplicar anomalías
        if anomaly == AnomalyType.INCOMPLETE:
            # No emitir el fragmento METADATA (simula fragmento perdido)
            self.stats["anomalies"]["incomplete"] += 1
            fragments.append(ScheduledFragment(
                fragment=monetary,
                delay_ms=delay_monetary,
                emit_at=base_time + (delay_monetary / 1000),
                anomaly=anomaly,
            ))
            # METADATA no se agrega
            
        elif anomaly == AnomalyType.DUPLICATE:
            # Emitir MONETARY duplicado
            self.stats["anomalies"]["duplicate"] += 1
            fragments.append(ScheduledFragment(
                fragment=monetary,
                delay_ms=delay_monetary,
                emit_at=base_time + (delay_monetary / 1000),
                anomaly=AnomalyType.NONE,
            ))
            # Duplicado con pequeño delay adicional
            fragments.append(ScheduledFragment(
                fragment=monetary,
                delay_ms=delay_monetary + 50,
                emit_at=base_time + ((delay_monetary + 50) / 1000),
                anomaly=anomaly,
                is_duplicate=True,
            ))
            fragments.append(ScheduledFragment(
                fragment=metadata,
                delay_ms=delay_metadata,
                emit_at=base_time + (delay_metadata / 1000),
                anomaly=AnomalyType.NONE,
            ))
            
        elif anomaly == AnomalyType.OUT_OF_ORDER:
            # METADATA llega antes que MONETARY
            self.stats["anomalies"]["out_of_order"] += 1
            # Swap delays
            fragments.append(ScheduledFragment(
                fragment=metadata,  # Metadata primero
                delay_ms=delay_monetary,
                emit_at=base_time + (delay_monetary / 1000),
                anomaly=anomaly,
            ))
            fragments.append(ScheduledFragment(
                fragment=monetary,  # Monetary después
                delay_ms=delay_metadata,
                emit_at=base_time + (delay_metadata / 1000),
                anomaly=anomaly,
            ))
            
        else:
            # Caso normal: orden correcto
            fragments.append(ScheduledFragment(
                fragment=monetary,
                delay_ms=delay_monetary,
                emit_at=base_time + (delay_monetary / 1000),
                anomaly=AnomalyType.NONE,
            ))
            fragments.append(ScheduledFragment(
                fragment=metadata,
                delay_ms=delay_metadata,
                emit_at=base_time + (delay_metadata / 1000),
                anomaly=AnomalyType.NONE,
            ))
        
        self.stats["total_fragments"] += len(fragments)
        
        return fragments
    
    def get_stats(self) -> dict:
        """Retorna estadísticas del fragmentador."""
        stats = self.stats.copy()
        
        if stats["total_transactions"] > 0:
            stats["pct_incomplete_actual"] = (
                stats["anomalies"]["incomplete"] / stats["total_transactions"] * 100
            )
            stats["pct_duplicate_actual"] = (
                stats["anomalies"]["duplicate"] / stats["total_transactions"] * 100
            )
            stats["pct_out_of_order_actual"] = (
                stats["anomalies"]["out_of_order"] / stats["total_transactions"] * 100
            )
            stats["avg_fragments_per_txn"] = (
                stats["total_fragments"] / stats["total_transactions"]
            )
        
        return stats
    
    def reset_stats(self) -> None:
        """Resetea las estadísticas."""
        self.stats = {
            "total_transactions": 0,
            "total_fragments": 0,
            "anomalies": {
                "incomplete": 0,
                "duplicate": 0,
                "out_of_order": 0,
            }
        }


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    """Test del Fragmenter."""
    import time
    
    print("🧪 Testing Fragmenter...")
    
    # Crear movimiento de prueba
    from generator.models.sciffmrcmv import MovimientoFactory
    
    # Simular una cuenta
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
    fragmenter = Fragmenter()
    
    print("\n📦 Generando 100 transacciones fragmentadas...")
    
    all_fragments = []
    base_time = time.time()
    
    for i in range(100):
        movimiento = factory.crear_movimiento()
        fragments = fragmenter.fragment(movimiento, base_time + (i * 0.1))
        all_fragments.extend(fragments)
    
    print(f"\n📊 Estadísticas:")
    stats = fragmenter.get_stats()
    print(f"   Transacciones: {stats['total_transactions']}")
    print(f"   Fragmentos totales: {stats['total_fragments']}")
    print(f"   Promedio frags/txn: {stats.get('avg_fragments_per_txn', 0):.2f}")
    print(f"\n   Anomalías:")
    print(f"   - Incompletos: {stats['anomalies']['incomplete']} ({stats.get('pct_incomplete_actual', 0):.1f}%)")
    print(f"   - Duplicados: {stats['anomalies']['duplicate']} ({stats.get('pct_duplicate_actual', 0):.1f}%)")
    print(f"   - Desordenados: {stats['anomalies']['out_of_order']} ({stats.get('pct_out_of_order_actual', 0):.1f}%)")
    
    # Mostrar ejemplo de fragmento
    print("\n📄 Ejemplo de fragmento CDC:")
    if all_fragments:
        ejemplo = all_fragments[0].fragment.to_kafka_message()
        import json
        print(json.dumps(ejemplo, indent=2, default=str)[:1000] + "...")