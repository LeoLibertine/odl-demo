"""
Kafka Producer - Envío de fragmentos a AWS MSK
======================================================
Productor de Kafka optimizado para alto throughput (hasta 30K+ TPS).
Configurado para maximizar batching y minimizar latencia de red.
"""

import json
import time
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from datetime import datetime
import threading

from confluent_kafka import Producer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

import sys
sys.path.insert(0, '..')
from config.settings import get_kafka_config, KafkaConfig


@dataclass
class ProducerStats:
    """Estadísticas del productor (thread-safe con locks)."""
    messages_sent: int = 0
    messages_failed: int = 0
    bytes_sent: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    def increment_sent(self, count: int = 1, bytes_count: int = 0):
        with self._lock:
            self.messages_sent += count
            self.bytes_sent += bytes_count
    
    def increment_failed(self, count: int = 1):
        with self._lock:
            self.messages_failed += count
    
    @property
    def messages_per_second(self) -> float:
        if self.start_time and self.end_time:
            elapsed = (self.end_time - self.start_time).total_seconds()
            if elapsed > 0:
                return self.messages_sent / elapsed
        return 0.0
    
    @property
    def success_rate(self) -> float:
        total = self.messages_sent + self.messages_failed
        if total > 0:
            return self.messages_sent / total * 100
        return 0.0


class ConfluentKafkaProducer:
    """
    Productor de Kafka para AWS MSK.
    Optimizado para alto throughput (30K+ TPS).
    
    Configuración de batching agresivo:
    - batch.num.messages: 10000
    - linger.ms: 50
    - queue.buffering.max.messages: 500000
    """
    
    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        worker_id: int = 0,
        high_throughput: bool = False,
    ):
        """
        Inicializa el productor.
        
        Args:
            config: Configuración de Kafka
            worker_id: ID del worker (para logging)
            high_throughput: Si True, usa configuración agresiva para 30K+ TPS
        """
        self.config = config or get_kafka_config()
        self.worker_id = worker_id
        self.high_throughput = high_throughput
        
        self._producer: Optional[Producer] = None
        self.stats = ProducerStats()
        
        # Callbacks
        self._on_success: Optional[Callable] = None
        self._on_error: Optional[Callable] = None
    
    def _get_producer_config(self) -> dict:
        """Genera configuración para el productor."""
        
        # Configuración base - siempre usar buffers grandes
        config = {
            # Conexión a AWS MSK
            'bootstrap.servers': self.config.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-512',
            'sasl.username': self.config.api_key,
            'sasl.password': self.config.api_secret,

            # === BUFFER GRANDE (siempre) ===
            'queue.buffering.max.messages': 1000000,  # 1M mensajes en buffer
            'queue.buffering.max.kbytes': 2097152,    # 2GB buffer
            
            # === THROUGHPUT ===
            'acks': '1',                          # Solo líder (más rápido)
            'enable.idempotence': False,          # Desactivar para velocidad
            'max.in.flight.requests.per.connection': 10,
            
            # === REINTENTOS ===
            'retries': 5,
            'retry.backoff.ms': 100,
            
            # === METADATA ===
            'client.id': f'iseries-simulator-w{self.worker_id}',
            
            # === SOCKET ===
            'socket.send.buffer.bytes': 1048576,  # 1MB send buffer
            'socket.receive.buffer.bytes': 1048576,
            'socket.nagle.disable': True,
        }
        
        if self.high_throughput:
            # Configuración agresiva para 5K+ TPS
            config.update({
                'batch.num.messages': 10000,
                'batch.size': 1048576,            # 1MB por batch
                'linger.ms': 100,                 # Esperar 100ms para acumular
                'queue.buffering.max.ms': 200,
                'compression.type': 'lz4',
            })
        else:
            # Configuración estándar pero con buenos buffers
            config.update({
                'batch.num.messages': 5000,
                'batch.size': 524288,             # 512KB por batch
                'linger.ms': 50,                  # Esperar 50ms para acumular
                'queue.buffering.max.ms': 100,
                'compression.type': 'lz4',
            })
        
        return config
    
    def connect(self, silent: bool = False) -> None:
        """Establece conexión con Kafka."""
        if self._producer is None:
            if not silent:
                print(f"🔌 [W{self.worker_id}] Conectando a AWS MSK...")
                print(f"   Bootstrap: {self.config.bootstrap_servers}")
                print(f"   Topic: {self.config.topic}")
                print(f"   Mode: {'HIGH THROUGHPUT' if self.high_throughput else 'STANDARD'}")
            
            self._producer = Producer(self._get_producer_config())
            
            if not silent:
                # Verificar conexión listando topics
                admin = AdminClient({
                    'bootstrap.servers': self.config.bootstrap_servers,
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'SCRAM-SHA-512',
                    'sasl.username': self.config.api_key,
                    'sasl.password': self.config.api_secret,
                })
                
                try:
                    metadata = admin.list_topics(timeout=10)
                    print(f"✅ [W{self.worker_id}] Conectado. Topics disponibles: {len(metadata.topics)}")
                except Exception as e:
                    print(f"⚠️  [W{self.worker_id}] Conectado pero no pudo listar topics: {e}")
    
    def create_topic_if_not_exists(
        self,
        num_partitions: int = 6,
        replication_factor: int = 2,
    ) -> bool:
        """Crea el topic si no existe."""
        admin = AdminClient({
            'bootstrap.servers': self.config.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-512',
            'sasl.username': self.config.api_key,
            'sasl.password': self.config.api_secret,
        })
        
        metadata = admin.list_topics(timeout=10)
        if self.config.topic in metadata.topics:
            print(f"📋 Topic '{self.config.topic}' ya existe")
            return False
        
        print(f"📝 Creando topic '{self.config.topic}'...")
        new_topic = NewTopic(
            self.config.topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        
        futures = admin.create_topics([new_topic])
        
        for topic, future in futures.items():
            try:
                future.result()
                print(f"✅ Topic '{topic}' creado")
                return True
            except Exception as e:
                print(f"❌ Error creando topic: {e}")
                raise
    
    def _delivery_callback(self, err, msg):
        """Callback invocado cuando un mensaje es entregado (o falla)."""
        if err:
            self.stats.increment_failed()
            if self._on_error:
                self._on_error(err, msg)
        else:
            self.stats.increment_sent(1, len(msg.value()) if msg.value() else 0)
            if self._on_success:
                self._on_success(msg)
    
    def send(
        self,
        message: Dict[str, Any],
        key: Optional[str] = None,
        max_retries: int = 10,
    ) -> bool:
        """
        Envía un mensaje a Kafka con retry automático si la cola está llena.
        
        Returns:
            True si se envió, False si falló después de todos los reintentos
        """
        if self._producer is None:
            raise RuntimeError("Productor no conectado. Llama connect() primero.")
        
        value = json.dumps(message, default=str).encode('utf-8')
        key_bytes = key.encode('utf-8') if key else None
        
        for attempt in range(max_retries):
            try:
                self._producer.produce(
                    topic=self.config.topic,
                    value=value,
                    key=key_bytes,
                    callback=self._delivery_callback,
                )
                return True
            except BufferError:
                # Cola llena - poll para liberar espacio y reintentar
                self._producer.poll(0.1)
                time.sleep(0.05 * (attempt + 1))  # Backoff incremental
            except Exception as e:
                self.stats.increment_failed()
                if self._on_error:
                    self._on_error(e, None)
                return False
        
        # Si llegamos aquí, fallaron todos los reintentos
        self.stats.increment_failed()
        return False
    
    def send_batch(
        self,
        messages: list,
        key_field: str = "correlation_key",
    ) -> int:
        """Envía un batch de mensajes."""
        count = 0
        for msg in messages:
            key = None
            if key_field:
                if isinstance(msg, dict):
                    key = msg.get(key_field)
                    if not key and 'correlation' in msg:
                        key = msg['correlation'].get('key')
            
            self.send(msg, key)
            count += 1
            
            # Poll cada 1000 mensajes para procesar callbacks
            if count % 1000 == 0:
                self._producer.poll(0)
        
        return count
    
    def poll(self, timeout: float = 0) -> int:
        """Poll para procesar callbacks pendientes."""
        if self._producer:
            return self._producer.poll(timeout)
        return 0
    
    def flush(self, timeout: float = 30.0) -> int:
        """Espera a que todos los mensajes sean enviados."""
        if self._producer:
            return self._producer.flush(timeout)
        return 0
    
    def close(self) -> None:
        """Cierra el productor."""
        if self._producer:
            self.flush()
            self._producer = None
    
    def get_stats(self) -> ProducerStats:
        """Retorna estadísticas del productor."""
        return self.stats
    
    def reset_stats(self) -> None:
        """Resetea estadísticas."""
        self.stats = ProducerStats()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class KafkaLagMonitor:
    """
    Monitor de lag para el topic de Kafka.
    Permite verificar cuántos mensajes hay pendientes de consumir.
    """
    
    def __init__(self, config: Optional[KafkaConfig] = None):
        self.config = config or get_kafka_config()
        self._admin: Optional[AdminClient] = None
    
    def _get_admin(self) -> AdminClient:
        if self._admin is None:
            self._admin = AdminClient({
                'bootstrap.servers': self.config.bootstrap_servers,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'SCRAM-SHA-512',
                'sasl.username': self.config.api_key,
                'sasl.password': self.config.api_secret,
            })
        return self._admin
    
    def get_topic_offsets(self) -> Dict[str, Any]:
        """
        Obtiene los offsets actuales del topic.
        Retorna high watermark (último mensaje) por partición.
        """
        from confluent_kafka import Consumer
        
        # Crear consumer temporal para obtener offsets
        consumer = Consumer({
            'bootstrap.servers': self.config.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'SCRAM-SHA-512',
            'sasl.username': self.config.api_key,
            'sasl.password': self.config.api_secret,
            'group.id': 'lag-monitor-temp',
            'auto.offset.reset': 'latest',
        })
        
        try:
            # Obtener metadata del topic
            metadata = consumer.list_topics(self.config.topic, timeout=10)
            topic_metadata = metadata.topics.get(self.config.topic)
            
            if not topic_metadata:
                return {"error": f"Topic {self.config.topic} no encontrado"}
            
            partitions = []
            total_messages = 0
            
            for partition_id in topic_metadata.partitions.keys():
                tp = TopicPartition(self.config.topic, partition_id)
                
                # Obtener low y high watermark
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                messages_in_partition = high - low
                total_messages += messages_in_partition
                
                partitions.append({
                    "partition": partition_id,
                    "low_offset": low,
                    "high_offset": high,
                    "messages": messages_in_partition,
                })
            
            return {
                "topic": self.config.topic,
                "partitions": len(partitions),
                "total_messages": total_messages,
                "partition_details": partitions,
            }
        
        finally:
            consumer.close()
    
    def print_lag_report(self) -> Dict[str, Any]:
        """Imprime y retorna reporte de lag."""
        offsets = self.get_topic_offsets()
        
        if "error" in offsets:
            print(f"❌ Error obteniendo offsets: {offsets['error']}")
            return offsets
        
        print(f"\n📊 ESTADO DEL TOPIC: {offsets['topic']}")
        print(f"   Particiones: {offsets['partitions']}")
        print(f"   Mensajes totales: {offsets['total_messages']:,}")
        print(f"\n   Por partición:")
        for p in offsets['partition_details']:
            print(f"   [P{p['partition']}] {p['messages']:,} msgs (offset {p['low_offset']:,} → {p['high_offset']:,})")
        
        return offsets


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    import time
    
    print("🧪 Testing ConfluentKafkaProducer (High Throughput)...")
    print("=" * 50)
    
    try:
        # Test producer
        with ConfluentKafkaProducer(high_throughput=True) as producer:
            producer.create_topic_if_not_exists()
            
            print("\n📤 Enviando 1000 mensajes de prueba...")
            producer.stats.start_time = datetime.now()
            
            for i in range(1000):
                msg = {
                    "test": True,
                    "sequence": i,
                    "timestamp": datetime.now().isoformat(),
                    "data": f"Mensaje de prueba #{i}" * 10,  # Mensaje más grande
                }
                producer.send(msg, key=f"test-key-{i % 10}")
                
                if i % 100 == 0:
                    producer.poll(0)
            
            pending = producer.flush(timeout=10)
            producer.stats.end_time = datetime.now()
            
            print(f"\n📊 Resultados:")
            stats = producer.get_stats()
            print(f"   Mensajes enviados: {stats.messages_sent}")
            print(f"   Mensajes fallidos: {stats.messages_failed}")
            print(f"   Bytes enviados: {stats.bytes_sent:,}")
            print(f"   Tasa: {stats.messages_per_second:.1f} msg/s")
            print(f"   Éxito: {stats.success_rate:.1f}%")
        
        # Test lag monitor
        print("\n" + "=" * 50)
        monitor = KafkaLagMonitor()
        monitor.print_lag_report()
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Asegúrate de configurar las variables de entorno:")
        print("   export KAFKA_BOOTSTRAP_SERVERS='...'")
        print("   export KAFKA_API_KEY='...'")
        print("   export KAFKA_API_SECRET='...'")
