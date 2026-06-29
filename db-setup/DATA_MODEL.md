# Modelo de datos — ODL (`bancolombia_odl`)

Extraído del cluster real de la demo. Cuatro colecciones.

## `movimientos` (principal)

Documento bancario ensamblado (resultado del CDC). Estructura:

| Campo | Tipo | Notas |
|-------|------|-------|
| `_id` | String | |
| `correlation_key` | String | Clave de correlación del ensamblaje CDC |
| `cuenta` | Document | `.numero`, `.tipo`, `.tipo_legacy` |
| `movimiento` | Document | `.fecha` (str), `.hora` (str), `.fecha_iso` (Date), `.tipo`, `.codigo_transaccion`, `.secuencia` (num), `.canal{codigo,nombre}`, `.sucursal`, `.estado`, `.estado_legacy` |
| `monetario` | Document | `.valor`, `.moneda`, `.signo`, `.saldo_anterior`, `.saldo_nuevo`, `.tasa_cambio`, `.valor_original`, `.moneda_original`, `.impuestos{iva,gmf}` |
| `referencia` | Document | `.numero`, `.descripcion` |
| `destinatario` | Document | `.cuenta`, `.banco`, `.nombre`, `.identificacion{numero,tipo}` |
| `origen_digital` | Document | `.ip`, `.user_agent`, `.geolocalizacion{type,coordinates}`, `.dispositivo`, `.sesion` |
| `_asp_metadata` | Document | Metadatos del Stream Processor: `.processed_at`, `.pipeline`, `.version`, `.source_system`, `.cdc_operation`, `.correlation_key`, `.fragment_count`, `.assembly_latency_ms`, `.e2e_latency_ms`, `.legacy_fields_discarded` |
| `_enrichment_metadata` | Document | `.enriched_at`, `.version`, `.source` |
| `comercio` | Document | `.nombre`, `.nombre_normalizado`, `.mcc`, `.categoria`, `.subcategoria`, `.tags[]`, `.descripcion_busqueda`, `.es_recurrente`, `.embedding[]` (1024 floats, Voyage) |

**Índices:** 16 clásicos + `movimientos_search` (Atlas Search) + `movimientos_vector` (Vector Search, 1024 dims cosine sobre `comercio.embedding`). Ver `indexes/`.

## `metricas_asp`

Métricas agregadas por ventana (alimenta el dashboard).

| Campo | Tipo |
|-------|------|
| `total_transactions`, `tps_avg`, `unique_accounts_count`, `unique_branches_count`, `window_seconds` | Number |
| `window_start`, `window_end`, `pipeline`, `version` | String |
| `recorded_at` | Date |
| `channels` | Document `{APP,WEB,SUC,ATM,API}` (counts) |
| `movement_types` | Document `{TRF,DEP,WDR,PSE,PAY,INT,FEE}` (counts) |
| `account_types` | Document `{AHO,COR,CDT,AFC}` (counts) |

**Índice:** `idx_metricas_fecha` sobre `{recorded_at: -1}`.

## `movimientos_dlq`

Dead-letter queue: fragmentos que no se pudieron ensamblar (mismo shape parcial que `movimientos`).

## `movimientos_dlq_validation`

Colección auxiliar de validación de la DLQ.

---

### Cómo se puebla
- **Vía seed** (rápido, sin infra): `seed/*.seed.js` — muestra real de documentos.
- **Vía flujo CDC en vivo** (avanzado): Simulador → Kafka → Stream Processor → `movimientos`. Ver `stream-processors/`.
