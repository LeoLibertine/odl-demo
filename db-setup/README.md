# db-setup — Replicar el modelo de datos del ODL

Todo lo necesario para reconstruir la base `bancolombia_odl` en **tu propio**
cluster de MongoDB Atlas, sin depender del cluster original.

> Un `mongodump` restaura datos pero **no** trae los Atlas Search/Vector indexes
> ni los Stream Processors. Aquí está todo eso, explícito y reproducible.

## Requisitos
- Un cluster **MongoDB Atlas** (Vector Search y Atlas Search son features de Atlas).
- `mongosh` instalado.
- `export MONGODB_ODL_URI="mongodb+srv://USUARIO:PASSWORD@tu-cluster.mongodb.net/"`

## Pasos (en orden)

```bash
# 1. Crear índices clásicos
mongosh "$MONGODB_ODL_URI" indexes/01-classic-indexes.js

# 2. Crear Atlas Search + Vector Search indexes (requiere Atlas)
mongosh "$MONGODB_ODL_URI" indexes/02-search-indexes.js

# 3. Cargar datos de ejemplo (muestra real: 300 movimientos + 100 métricas + 50 DLQ)
mongosh "$MONGODB_ODL_URI" seed/movimientos.seed.js
mongosh "$MONGODB_ODL_URI" seed/metricas_asp.seed.js
mongosh "$MONGODB_ODL_URI" seed/movimientos_dlq.seed.js
```

Con esto, la **app (8788)** y el **dashboard (8787)** ya funcionan apuntando a tu cluster.

## Opcionales (avanzado)

- **Búsqueda vectorial:** el seed viene **sin** el campo `comercio.embedding` (para
  ser liviano). Para activar Vector Search, genera los embeddings con Voyage AI
  sobre `comercio.descripcion_busqueda` y guárdalos en `comercio.embedding`
  (1024 dims). El índice ya queda creado en el paso 2.

- **Flujo CDC en vivo** (Simulador → Kafka → Stream Processor → Mongo): ver
  `stream-processors/`. Requiere una instancia de Atlas Stream Processing + Kafka propio.

## Contenido
```
DATA_MODEL.md          Estructura de las 4 colecciones e índices
indexes/               Índices clásicos + Atlas Search/Vector Search
seed/                  Muestra real de datos (scripts mongosh auto-cargables)
stream-processors/     Pipeline ASP + scripts de setup (flujo CDC en vivo)
```
