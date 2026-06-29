// ═══════════════════════════════════════════════════════════════════════════
//  ODL — Índices clásicos (B-tree)
//  Carga:  mongosh "$MONGODB_ODL_URI" db-setup/indexes/01-classic-indexes.js
//  Extraído del cluster real de la demo (bancolombia_odl).
// ═══════════════════════════════════════════════════════════════════════════

const db = db.getSiblingDB("bancolombia_odl");

// ── movimientos ──
db.movimientos.createIndex({ "comercio.descripcion_busqueda": 1 }, { name: "idx_desc_busqueda" });
db.movimientos.createIndex({ "movimiento.canal.codigo": 1, "movimiento.fecha_iso": -1 }, { name: "idx_canal_fecha" });
db.movimientos.createIndex({ "movimiento.tipo": 1, "movimiento.fecha_iso": -1 }, { name: "idx_tipo_fecha" });
db.movimientos.createIndex({ "movimiento.sucursal": 1, "movimiento.fecha_iso": -1 }, { name: "idx_sucursal_fecha" });
db.movimientos.createIndex({ "cuenta.tipo": 1, "movimiento.fecha_iso": -1 }, { name: "idx_tipo_cuenta_fecha" });
db.movimientos.createIndex({ "movimiento.fecha_iso": -1 }, { name: "movimiento.fecha_iso_-1" });
db.movimientos.createIndex({ "_asp_metadata.e2e_latency_ms": 1 }, { name: "_asp_metadata.e2e_latency_ms_1" });
db.movimientos.createIndex({ "cuenta.sucursal": 1 }, { name: "idx_sucursal" });
db.movimientos.createIndex({ "movimiento.canal.nombre": 1 }, { name: "idx_canal" });
db.movimientos.createIndex({ "movimiento.hora": 1 }, { name: "idx_hora" });
db.movimientos.createIndex({ "_asp_metadata.assembly_latency_ms": 1 }, { name: "idx_asm_latency" });
db.movimientos.createIndex({ "comercio.categoria": 1, "cuenta.numero": 1 }, { name: "idx_comercio_categoria_cuenta" });
db.movimientos.createIndex({ "comercio.nombre_normalizado": 1, "cuenta.numero": 1 }, { name: "idx_comercio_nombre_cuenta" });
db.movimientos.createIndex({ "comercio.es_recurrente": 1, "cuenta.numero": 1 }, { name: "idx_comercio_recurrente_cuenta" });
db.movimientos.createIndex({ "cuenta.numero": 1, "movimiento.fecha_iso": -1, "comercio.categoria": 1 }, { name: "idx_cuenta_fecha_categoria" });
db.movimientos.createIndex({ "correlation_key": 1 }, { name: "correlation_key_1" });

// ── metricas_asp ──
db.metricas_asp.createIndex({ "recorded_at": -1 }, { name: "idx_metricas_fecha" });

print("✅ Índices clásicos creados");
