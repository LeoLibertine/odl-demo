# Atlas Stream Processors — ODL CDC Assembler

Reproduce el flujo de ensamblaje en tiempo real de la demo:

```
Simulador iSeries → Kafka (3 fragmentos CDC) → Atlas Stream Processing → MongoDB (movimientos)
```

El Stream Processor lee 3 fragmentos CDC (HEADER, MONETARY, METADATA) del topic
Kafka, los agrupa por `correlation.key` en una ventana de tiempo, ensambla el
documento final y lo escribe en `bancolombia_odl.movimientos` vía `$merge`.

> ⚠️ **Opcional / avanzado.** No necesitas esto para ver la demo: la app y el
> dashboard funcionan con los datos del seed (`../seed/`). Esto es solo para
> reproducir el flujo CDC en vivo, y requiere **Atlas Stream Processing** + un
> **Kafka/MSK** propio.

## Requisitos
- Una instancia de **Atlas Stream Processing** en tu proyecto.
- Un cluster Kafka/MSK con el topic `bancolombia.depositos.cdc.raw`.
- API keys de Atlas (Admin API).

## Pasos

```bash
# 1. Variables de entorno (tus credenciales, NADA hardcodeado)
export ATLAS_PUBLIC_KEY=...   ATLAS_PRIVATE_KEY=...   ATLAS_PROJECT_ID=...
export SP_INSTANCE_NAME=spinstance
export KAFKA_BOOTSTRAP="broker1:9196,broker2:9196"
export KAFKA_USER=...   KAFKA_PASSWORD=...

# 2. Crear la conexión Kafka en el SP instance
./create_connection.sh

# 3. Crear el processor (ver create_processor.sh)
./create_processor.sh

# 4. Arrancar el processor desde mongosh conectado al SP instance:
#    sp.createStreamProcessor("bancolombia-assembler", pipeline)   // ver pipeline.js
#    sp["bancolombia-assembler"].start()
```

## Archivos
- `pipeline.js` — el pipeline de agregación del Stream Processor (ensamblaje CDC).
- `create_connection.sh` — crea la conexión Kafka vía Atlas Admin API.
- `create_processor.sh` — crea el processor vía Atlas Admin API.
