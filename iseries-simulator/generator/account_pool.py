"""
Account Pool - Gestión del pool de cuentas desde MongoDB
========================================================
Lee un sample de cuentas desde MongoDB Atlas (CLUSTER ORIGEN)
para usarlas en la generación de movimientos.

NOTA: Este módulo lee del CLUSTER ORIGEN (simulación iSeries),
que está separado del cluster ODL destino para evitar contención.
"""

import random
import sys
from typing import List, Dict, Any, Optional

from pymongo import MongoClient

sys.path.insert(0, '..')
import sys; sys.path.insert(0, "."); from config.settings import get_mongodb_origen_config, MongoDBOrigenConfig


class AccountPool:
    """
    Pool de cuentas para generación de movimientos.
    Carga un sample de cuentas desde MongoDB (CLUSTER ORIGEN) a memoria.
    """
    
    def __init__(
        self,
        config: Optional[MongoDBOrigenConfig] = None,
        sample_size: int = 100_000
    ):
        """
        Inicializa el pool de cuentas.
        
        Args:
            config: Configuración de MongoDB ORIGEN (opcional, usa env vars si no se provee)
            sample_size: Cantidad de cuentas a cargar en memoria
        """
        self.config = config or get_mongodb_origen_config()
        self.sample_size = sample_size
        self.cuentas: List[Dict[str, Any]] = []
        self._client: Optional[MongoClient] = None
        
        # Estadísticas
        self.stats = {
            "total_loaded": 0,
            "by_type": {},
            "by_estado": {},
        }
    
    def connect(self) -> None:
        """Establece conexión con MongoDB (CLUSTER ORIGEN)."""
        if self._client is None:
            self._client = MongoClient(self.config.uri)
            # Verificar conexión
            self._client.admin.command('ping')
    
    def disconnect(self) -> None:
        """Cierra conexión con MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
    
    def load(self, weighted_by_activity: bool = True) -> int:
        """
        Carga cuentas desde MongoDB (CLUSTER ORIGEN) a memoria.
        
        Args:
            weighted_by_activity: Si True, da preferencia a cuentas activas
            
        Returns:
            Número de cuentas cargadas
        """
        print(f"📂 Cargando {self.sample_size:,} cuentas desde MongoDB (ORIGEN)...")
        
        self.connect()
        db = self._client[self.config.database]
        collection = db[self.config.collection_cuentas]
        
        # Verificar que hay cuentas
        total_cuentas = collection.count_documents({})
        if total_cuentas == 0:
            raise ValueError(
                "No hay cuentas en la colección. "
                "Ejecuta primero: python setup/setup_maestro_cuentas.py"
            )
        
        print(f"   Total cuentas en BD: {total_cuentas:,}")
        
        # Estrategia de carga
        if weighted_by_activity:
            # Cargar más cuentas activas (85%) que inactivas
            self.cuentas = self._load_weighted(collection)
        else:
            # Sample aleatorio simple
            self.cuentas = self._load_random(collection)
        
        # Calcular estadísticas
        self._calculate_stats()
        
        print(f"✅ {len(self.cuentas):,} cuentas cargadas en memoria")
        
        return len(self.cuentas)
    
    def _load_random(self, collection) -> List[Dict[str, Any]]:
        """Carga sample aleatorio simple."""
        pipeline = [
            {"$sample": {"size": self.sample_size}},
            {"$project": {
                "_id": 1,
                "numero_cuenta": 1,
                "tipo_cuenta": 1,
                "estado_codigo": 1,
                "saldo_disponible": 1,
                "sucursal_apertura": 1,
                "identificacion_cliente": 1,
                "tipo_identificacion": 1,
                "exento_gmf": 1,
            }}
        ]
        return list(collection.aggregate(pipeline))
    
    def _load_weighted(self, collection) -> List[Dict[str, Any]]:
        """Carga dando más peso a cuentas activas."""
        cuentas = []
        
        # 85% cuentas activas
        activas_size = int(self.sample_size * 0.85)
        pipeline_activas = [
            {"$match": {"estado_codigo": "01"}},
            {"$sample": {"size": activas_size}},
            {"$project": {
                "_id": 1,
                "numero_cuenta": 1,
                "tipo_cuenta": 1,
                "estado_codigo": 1,
                "saldo_disponible": 1,
                "sucursal_apertura": 1,
                "identificacion_cliente": 1,
                "tipo_identificacion": 1,
                "exento_gmf": 1,
            }}
        ]
        cuentas.extend(list(collection.aggregate(pipeline_activas)))
        
        # 15% otras cuentas
        otras_size = self.sample_size - len(cuentas)
        if otras_size > 0:
            pipeline_otras = [
                {"$match": {"estado_codigo": {"$ne": "01"}}},
                {"$sample": {"size": otras_size}},
                {"$project": {
                    "_id": 1,
                    "numero_cuenta": 1,
                    "tipo_cuenta": 1,
                    "estado_codigo": 1,
                    "saldo_disponible": 1,
                    "sucursal_apertura": 1,
                    "identificacion_cliente": 1,
                    "tipo_identificacion": 1,
                    "exento_gmf": 1,
                }}
            ]
            cuentas.extend(list(collection.aggregate(pipeline_otras)))
        
        return cuentas
    
    def _calculate_stats(self) -> None:
        """Calcula estadísticas del pool cargado."""
        self.stats["total_loaded"] = len(self.cuentas)
        
        # Por tipo
        by_type = {}
        for cuenta in self.cuentas:
            tipo = cuenta.get("tipo_cuenta", "UNKNOWN")
            by_type[tipo] = by_type.get(tipo, 0) + 1
        self.stats["by_type"] = by_type
        
        # Por estado
        by_estado = {}
        for cuenta in self.cuentas:
            estado = cuenta.get("estado_codigo", "XX")
            by_estado[estado] = by_estado.get(estado, 0) + 1
        self.stats["by_estado"] = by_estado
    
    def get_random_cuenta(self) -> Dict[str, Any]:
        """Obtiene una cuenta aleatoria del pool."""
        if not self.cuentas:
            raise ValueError("Pool vacío. Ejecuta load() primero.")
        return random.choice(self.cuentas)
    
    def get_cuenta_by_tipo(self, tipo: str) -> Optional[Dict[str, Any]]:
        """Obtiene una cuenta aleatoria de un tipo específico."""
        cuentas_tipo = [c for c in self.cuentas if c.get("tipo_cuenta") == tipo]
        if cuentas_tipo:
            return random.choice(cuentas_tipo)
        return None
    
    def print_stats(self) -> None:
        """Imprime estadísticas del pool."""
        print("\n📊 ESTADÍSTICAS DEL POOL")
        print("-" * 40)
        print(f"Total cuentas cargadas: {self.stats['total_loaded']:,}")
        
        print("\nPor tipo de cuenta:")
        for tipo, count in self.stats["by_type"].items():
            pct = count / self.stats["total_loaded"] * 100
            print(f"  {tipo}: {count:,} ({pct:.1f}%)")
        
        print("\nPor estado:")
        estados = {"01": "ACTIVA", "02": "INACTIVA", "03": "BLOQUEADA", 
                   "04": "EMBARGADA", "05": "CANCELADA"}
        for codigo, count in self.stats["by_estado"].items():
            nombre = estados.get(codigo, codigo)
            pct = count / self.stats["total_loaded"] * 100
            print(f"  {nombre}: {count:,} ({pct:.1f}%)")
    
    def __len__(self) -> int:
        return len(self.cuentas)
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    """Test del AccountPool."""
    print("🧪 Testing AccountPool...")
    
    with AccountPool(sample_size=10_000) as pool:
        pool.load()
        pool.print_stats()
        
        print("\n📌 Cuentas de ejemplo:")
        for i in range(3):
            cuenta = pool.get_random_cuenta()
            print(f"  {i+1}. {cuenta['_id']} | {cuenta['tipo_cuenta']} | "
                  f"${cuenta.get('saldo_disponible', 0):,.0f}")
