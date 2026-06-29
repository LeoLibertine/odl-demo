#!/usr/bin/env python3
"""
Generador de Eventos iSeries - TFW Bancolombia (High Throughput)
================================================================
Soporta hasta 30,000+ TPS usando multiprocessing.

Modos de ejecución:
    # Modo simple (single process)
    python main.py --tps 500 --duration 300

    # Modo escenarios (3 escenarios predefinidos)
    python main.py --scenario burst-test

    # Modo multiprocessing manual
    python main.py --tps 10000 --duration 120 --workers 8

Escenarios predefinidos (--scenario burst-test):
    1. Normal:     1,000 TPS × 3 min  =  180,000 transacciones
    2. Pico:       5,000 TPS × 2 min  =  600,000 transacciones
    3. Burst:     30,000 TPS × 1 min  = 1,800,000 transacciones
"""

import argparse
import multiprocessing as mp
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

sys.path.insert(0, '.')
sys.path.insert(0, '..')

from config.settings import get_generator_config, GeneratorConfig
from generator.account_pool import AccountPool
from generator.models.sciffmrcmv import MovimientoFactory
from generator.fragmenter import Fragmenter, ScheduledFragment
from generator.kafka_producer import ConfluentKafkaProducer, KafkaLagMonitor, ProducerStats


# =============================================================================
# ESCENARIOS PREDEFINIDOS
# =============================================================================

@dataclass
class Scenario:
    """Define un escenario de carga."""
    name: str
    tps: int
    duration_seconds: int
    description: str
    
    @property
    def total_transactions(self) -> int:
        return self.tps * self.duration_seconds
    
    @property
    def estimated_fragments(self) -> int:
        return self.total_transactions * 3


SCENARIOS = {
    "burst-test": [
        Scenario("Normal", 1000, 180, "Carga normal de producción"),
        Scenario("Pico Horario", 5000, 120, "Pico de fin de mes / nóminas"),
        Scenario("Burst Post-Caída", 30000, 60, "ODL estuvo caído 2h, todo llega de golpe"),
    ],
    "demo-quick": [
        Scenario("Demo Suave", 500, 60, "Demo inicial para mostrar flujo"),
        Scenario("Demo Carga", 2000, 60, "Demo con carga real"),
    ],
    "stress-test": [
        Scenario("Warm-up", 1000, 60, "Calentamiento"),
        Scenario("Stress Medio", 10000, 120, "Carga media sostenida"),
        Scenario("Stress Alto", 20000, 120, "Carga alta sostenida"),
        Scenario("Stress Extremo", 30000, 120, "Carga extrema"),
    ],
}


# =============================================================================
# WORKER PROCESS
# =============================================================================

def worker_process(
    worker_id: int,
    tps_per_worker: int,
    duration_seconds: int,
    accounts_sample: List[Dict[str, Any]],
    result_queue: mp.Queue,
    stop_event: mp.Event,
    high_throughput: bool = False,
):
    """
    Proceso worker que genera y envía fragmentos.
    
    Args:
        worker_id: ID único del worker
        tps_per_worker: TPS objetivo para este worker
        duration_seconds: Duración en segundos
        accounts_sample: Lista de cuentas (ya cargadas, pasadas por el padre)
        result_queue: Cola para reportar estadísticas
        stop_event: Evento para señalar parada
        high_throughput: Si usar configuración de alto throughput
    """
    import time
    
    # Inicializar componentes
    factory = MovimientoFactory(accounts_sample)
    fragmenter = Fragmenter()
    producer = ConfluentKafkaProducer(
        worker_id=worker_id,
        high_throughput=high_throughput,
    )
    producer.connect(silent=True)
    
    # Estadísticas locales
    transactions_generated = 0
    fragments_sent = 0
    queue_full_retries = 0
    
    # Control de rate - usar batches más pequeños y frecuentes
    interval = 0.05  # 50ms batches (más frecuente)
    transactions_per_batch = max(1, int(tps_per_worker * interval))
    
    start_time = time.time()
    last_report = start_time
    last_poll = start_time
    
    try:
        while not stop_event.is_set():
            batch_start = time.time()
            elapsed = batch_start - start_time
            
            # Verificar si terminamos
            if elapsed >= duration_seconds:
                break
            
            # Generar batch de transacciones
            for _ in range(transactions_per_batch):
                movimiento = factory.crear_movimiento()
                transactions_generated += 1
                
                # Fragmentar
                fragments = fragmenter.fragment(movimiento, batch_start)
                
                # Enviar cada fragmento con retry
                for scheduled in fragments:
                    message = scheduled.fragment.to_kafka_message()
                    message["_produced_at"] = datetime.utcnow().isoformat() + "Z"
                    key = scheduled.fragment.correlation_key
                    
                    success = producer.send(message, key=key)
                    if success:
                        fragments_sent += 1
                    else:
                        queue_full_retries += 1
            
            # Poll más frecuente para liberar callbacks
            now = time.time()
            if now - last_poll >= 0.02:  # Poll cada 20ms
                producer.poll(0)
                last_poll = now
            
            # Reportar cada segundo
            if now - last_report >= 1.0:
                result_queue.put({
                    "worker_id": worker_id,
                    "type": "progress",
                    "transactions": transactions_generated,
                    "fragments": fragments_sent,
                    "elapsed": elapsed,
                })
                last_report = now
            
            # Mantener rate
            batch_elapsed = time.time() - batch_start
            sleep_time = max(0, interval - batch_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
    
    except Exception as e:
        result_queue.put({
            "worker_id": worker_id,
            "type": "error",
            "error": str(e),
        })
    
    finally:
        # Flush final con timeout largo
        producer.flush(timeout=60)
        
        # Reportar resultado final
        stats = producer.get_stats()
        frag_stats = fragmenter.get_stats()
        
        result_queue.put({
            "worker_id": worker_id,
            "type": "final",
            "transactions": transactions_generated,
            "fragments_generated": frag_stats["total_fragments"],
            "fragments_sent": stats.messages_sent,
            "fragments_failed": stats.messages_failed,
            "bytes_sent": stats.bytes_sent,
            "anomalies": frag_stats["anomalies"],
            "queue_full_retries": queue_full_retries,
        })
        
        producer.close()


# =============================================================================
# ORQUESTADOR PRINCIPAL
# =============================================================================

class MultiProcessGenerator:
    """
    Orquestador de generación multiprocessing.
    Coordina múltiples workers para alcanzar alto TPS.
    """
    
    def __init__(
        self,
        num_workers: int = None,
        accounts_sample_size: int = 100_000,
    ):
        """
        Inicializa el orquestador.
        
        Args:
            num_workers: Número de workers (default: CPUs - 1)
            accounts_sample_size: Cuentas a cargar por worker
        """
        self.num_workers = num_workers or max(1, mp.cpu_count() - 1)
        self.accounts_sample_size = accounts_sample_size
        
        # Componentes compartidos
        self.accounts_sample: List[Dict[str, Any]] = []
        self.lag_monitor = KafkaLagMonitor()
        
        # Estado
        self.workers: List[mp.Process] = []
        self.result_queue: Optional[mp.Queue] = None
        self.stop_event: Optional[mp.Event] = None
    
    def setup(self) -> None:
        """Carga el pool de cuentas (una sola vez)."""
        print("=" * 70)
        print("🏦 GENERADOR DE EVENTOS iSERIES - TFW BANCOLOMBIA (HIGH THROUGHPUT)")
        print("=" * 70)
        print()
        
        print(f"⚙️  Configuración:")
        print(f"   Workers: {self.num_workers}")
        print(f"   CPUs disponibles: {mp.cpu_count()}")
        print()
        
        # Cargar pool de cuentas
        print("📂 Cargando pool de cuentas...")
        pool = AccountPool(sample_size=self.accounts_sample_size)
        pool.load()
        self.accounts_sample = pool.cuentas
        pool.disconnect()
        
        print(f"✅ {len(self.accounts_sample):,} cuentas cargadas")
        print()
    
    def run_scenario(self, scenario: Scenario) -> Dict[str, Any]:
        """
        Ejecuta un escenario de carga.
        
        Args:
            scenario: Escenario a ejecutar
            
        Returns:
            Diccionario con resultados
        """
        print()
        print("═" * 70)
        print(f"🚀 ESCENARIO: {scenario.name}")
        print(f"   {scenario.description}")
        print("═" * 70)
        print(f"   TPS objetivo: {scenario.tps:,}")
        print(f"   Duración: {scenario.duration_seconds}s ({scenario.duration_seconds/60:.1f} min)")
        print(f"   Transacciones estimadas: {scenario.total_transactions:,}")
        print(f"   Fragmentos estimados: {scenario.estimated_fragments:,}")
        print()
        
        # Determinar configuración
        high_throughput = scenario.tps >= 2000
        
        # Calcular workers necesarios
        # Regla: usar multiprocessing desde 1,000 TPS
        # ~2,000-3,000 TPS por worker es el sweet spot
        if scenario.tps <= 500:
            num_workers = 1
        elif scenario.tps <= 1500:
            num_workers = min(self.num_workers, 2)
        elif scenario.tps <= 3000:
            num_workers = min(self.num_workers, 3)
        elif scenario.tps <= 6000:
            num_workers = min(self.num_workers, 4)
        elif scenario.tps <= 12000:
            num_workers = min(self.num_workers, 6)
        else:
            # Para 30K TPS, usar todos los workers disponibles
            num_workers = self.num_workers
        
        tps_per_worker = scenario.tps // num_workers
        
        print(f"   Workers activos: {num_workers}")
        print(f"   TPS por worker: {tps_per_worker:,}")
        print(f"   Modo: {'HIGH THROUGHPUT' if high_throughput else 'STANDARD'}")
        print()
        print("-" * 70)
        
        # Crear cola y evento
        self.result_queue = mp.Queue()
        self.stop_event = mp.Event()
        
        # Lanzar workers
        self.workers = []
        for i in range(num_workers):
            p = mp.Process(
                target=worker_process,
                args=(
                    i,
                    tps_per_worker,
                    scenario.duration_seconds,
                    self.accounts_sample,
                    self.result_queue,
                    self.stop_event,
                    high_throughput,
                ),
            )
            p.start()
            self.workers.append(p)
        
        # Monitorear progreso
        start_time = time.time()
        worker_stats = {i: {"transactions": 0, "fragments": 0} for i in range(num_workers)}
        final_results = []
        
        last_print = start_time
        
        while True:
            # Verificar si todos terminaron
            alive = [w for w in self.workers if w.is_alive()]
            if not alive and self.result_queue.empty():
                break
            
            # Procesar mensajes de la cola
            while not self.result_queue.empty():
                try:
                    msg = self.result_queue.get_nowait()
                    
                    if msg["type"] == "progress":
                        worker_stats[msg["worker_id"]] = {
                            "transactions": msg["transactions"],
                            "fragments": msg["fragments"],
                        }
                    
                    elif msg["type"] == "final":
                        final_results.append(msg)
                    
                    elif msg["type"] == "error":
                        print(f"\n❌ [W{msg['worker_id']}] Error: {msg['error']}")
                
                except:
                    break
            
            # Imprimir progreso cada segundo
            now = time.time()
            if now - last_print >= 1.0:
                elapsed = now - start_time
                total_txns = sum(s["transactions"] for s in worker_stats.values())
                total_frags = sum(s["fragments"] for s in worker_stats.values())
                actual_tps = total_txns / elapsed if elapsed > 0 else 0
                
                # Barra de progreso
                pct = min(100, elapsed / scenario.duration_seconds * 100)
                bar_width = 35
                filled = int(bar_width * pct / 100)
                bar = "█" * filled + "░" * (bar_width - filled)
                
                print(f"\r[{bar}] {pct:5.1f}% | "
                      f"TPS: {actual_tps:,.0f}/{scenario.tps:,} | "
                      f"Txns: {total_txns:,} | "
                      f"Frags: {total_frags:,} | "
                      f"⏱️ {int(elapsed)}s", end="", flush=True)
                
                last_print = now
            
            time.sleep(0.1)
        
        # Esperar a que todos terminen
        for w in self.workers:
            w.join(timeout=10)
        
        # Calcular resultados agregados
        elapsed = time.time() - start_time
        total_txns = sum(r["transactions"] for r in final_results)
        total_frags_sent = sum(r["fragments_sent"] for r in final_results)
        total_frags_failed = sum(r["fragments_failed"] for r in final_results)
        total_bytes = sum(r["bytes_sent"] for r in final_results)
        
        # Agregar anomalías
        total_anomalies = {"incomplete": 0, "duplicate": 0, "out_of_order": 0}
        for r in final_results:
            for k in total_anomalies:
                total_anomalies[k] += r["anomalies"].get(k, 0)
        
        result = {
            "scenario": scenario.name,
            "target_tps": scenario.tps,
            "actual_tps": total_txns / elapsed if elapsed > 0 else 0,
            "duration_seconds": elapsed,
            "transactions": total_txns,
            "fragments_sent": total_frags_sent,
            "fragments_failed": total_frags_failed,
            "bytes_sent": total_bytes,
            "workers": num_workers,
            "anomalies": total_anomalies,
        }
        
        # Imprimir resumen
        print()
        print()
        print(f"✅ ESCENARIO '{scenario.name}' COMPLETADO")
        print(f"   Duración: {elapsed:.1f}s")
        print(f"   TPS real: {result['actual_tps']:,.0f} ({result['actual_tps']/scenario.tps*100:.1f}% del objetivo)")
        print(f"   Transacciones: {total_txns:,}")
        print(f"   Fragmentos enviados: {total_frags_sent:,}")
        print(f"   Fragmentos fallidos: {total_frags_failed:,}")
        print(f"   Datos enviados: {total_bytes/1024/1024:.1f} MB")
        print(f"   Anomalías: {total_anomalies['incomplete']} incompletos, "
              f"{total_anomalies['duplicate']} duplicados, "
              f"{total_anomalies['out_of_order']} desordenados")
        
        return result
    
    def run_scenarios(self, scenario_name: str) -> List[Dict[str, Any]]:
        """
        Ejecuta una secuencia de escenarios.
        
        Args:
            scenario_name: Nombre del conjunto de escenarios
            
        Returns:
            Lista de resultados por escenario
        """
        if scenario_name not in SCENARIOS:
            raise ValueError(f"Escenario '{scenario_name}' no existe. "
                           f"Disponibles: {list(SCENARIOS.keys())}")
        
        scenarios = SCENARIOS[scenario_name]
        results = []
        
        print()
        print("╔" + "═" * 68 + "╗")
        print(f"║ {'INICIANDO SECUENCIA DE ESCENARIOS: ' + scenario_name:^66} ║")
        print("╠" + "═" * 68 + "╣")
        for i, s in enumerate(scenarios, 1):
            print(f"║  {i}. {s.name:<20} {s.tps:>6,} TPS × {s.duration_seconds:>3}s = {s.total_transactions:>10,} txns ║")
        print("╚" + "═" * 68 + "╝")
        
        total_transactions = sum(s.total_transactions for s in scenarios)
        total_duration = sum(s.duration_seconds for s in scenarios)
        print(f"\n📊 Total estimado: {total_transactions:,} transacciones en {total_duration/60:.1f} minutos")
        
        for i, scenario in enumerate(scenarios, 1):
            print(f"\n{'─' * 70}")
            print(f"📍 ESCENARIO {i}/{len(scenarios)}")
            
            result = self.run_scenario(scenario)
            results.append(result)
            
            # Reporte de lag entre escenarios
            print(f"\n📊 Estado de Kafka después de '{scenario.name}':")
            self.lag_monitor.print_lag_report()
            
            # Pausa entre escenarios (excepto el último)
            if i < len(scenarios):
                print(f"\n⏸️  Pausa de 10 segundos antes del siguiente escenario...")
                time.sleep(10)
        
        # Resumen final
        print()
        print("═" * 70)
        print("📊 RESUMEN FINAL DE TODOS LOS ESCENARIOS")
        print("═" * 70)
        
        total_txns = sum(r["transactions"] for r in results)
        total_frags = sum(r["fragments_sent"] for r in results)
        total_bytes = sum(r["bytes_sent"] for r in results)
        total_time = sum(r["duration_seconds"] for r in results)
        
        print(f"\n{'Escenario':<25} {'TPS Real':>12} {'Txns':>12} {'Fragmentos':>12} {'MB':>8}")
        print("-" * 70)
        for r in results:
            print(f"{r['scenario']:<25} {r['actual_tps']:>12,.0f} {r['transactions']:>12,} "
                  f"{r['fragments_sent']:>12,} {r['bytes_sent']/1024/1024:>8.1f}")
        print("-" * 70)
        print(f"{'TOTAL':<25} {total_txns/total_time:>12,.0f} {total_txns:>12,} "
              f"{total_frags:>12,} {total_bytes/1024/1024:>8.1f}")
        
        return results
    
    def run_single(self, tps: int, duration_seconds: int) -> Dict[str, Any]:
        """Ejecuta una carga simple (no escenario)."""
        scenario = Scenario(
            name="Custom",
            tps=tps,
            duration_seconds=duration_seconds,
            description=f"Carga personalizada: {tps} TPS × {duration_seconds}s",
        )
        return self.run_scenario(scenario)
    
    def stop(self) -> None:
        """Detiene todos los workers."""
        if self.stop_event:
            self.stop_event.set()
        
        for w in self.workers:
            if w.is_alive():
                w.terminate()
                w.join(timeout=5)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generador de eventos iSeries para TFW Bancolombia (High Throughput)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Modo simple
  python main.py --tps 500 --duration 300

  # Modo escenarios (recomendado para TFW)
  python main.py --scenario burst-test

  # Modo escenarios disponibles:
  - burst-test:  Normal (1K) → Pico (5K) → Burst (30K)
  - demo-quick:  Demo suave (500) → Demo carga (2K)
  - stress-test: Warm-up → Stress medio → alto → extremo
        """,
    )
    
    # Modo escenario
    parser.add_argument(
        "--scenario", "-s",
        type=str,
        choices=list(SCENARIOS.keys()),
        help="Ejecutar secuencia de escenarios predefinida",
    )
    
    # Modo simple
    parser.add_argument(
        "--tps", "-t",
        type=int,
        default=500,
        help="Transacciones por segundo (default: 500)",
    )
    parser.add_argument(
        "--duration", "-d",
        type=int,
        default=300,
        help="Duración en segundos (default: 300)",
    )
    
    # Configuración
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=None,
        help="Número de workers (default: CPUs - 1)",
    )
    parser.add_argument(
        "--accounts", "-a",
        type=int,
        default=100000,
        help="Cuentas a cargar en memoria (default: 100,000)",
    )
    
    args = parser.parse_args()
    
    # Crear generador
    generator = MultiProcessGenerator(
        num_workers=args.workers,
        accounts_sample_size=args.accounts,
    )
    
    # Manejar señales
    def signal_handler(sig, frame):
        print("\n\n⚠️  Señal de terminación recibida...")
        generator.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Setup
        generator.setup()
        
        # Verificar topic
        producer = ConfluentKafkaProducer()
        producer.connect()
        producer.create_topic_if_not_exists()
        producer.close()
        
        # Ejecutar
        if args.scenario:
            generator.run_scenarios(args.scenario)
        else:
            generator.run_single(args.tps, args.duration)
        
        print("\n" + "═" * 70)
        print("🎉 GENERACIÓN COMPLETADA EXITOSAMENTE")
        print("═" * 70)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrumpido por usuario")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Requerido para multiprocessing en macOS
    mp.set_start_method('spawn', force=True)
    main()
