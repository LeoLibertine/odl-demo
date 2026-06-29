// ═══════════════════════════════════════════════════════════════════════════
//  ODL — Atlas Search + Vector Search indexes
//  Carga:  mongosh "$MONGODB_ODL_URI" db-setup/indexes/02-search-indexes.js
//  ⚠️ Requiere MongoDB Atlas (no funciona en Mongo community).
//  Estas definiciones NO vienen en un mongodump — por eso se entregan aparte.
// ═══════════════════════════════════════════════════════════════════════════

const db = db.getSiblingDB("bancolombia_odl");

// ── Atlas Search: movimientos_search ──
db.movimientos.createSearchIndex("movimientos_search", {
  mappings: {
    dynamic: false,
    fields: {
      movimiento: {
        type: "document",
        fields: {
          tipo: [
            { type: "string" },
            { type: "autocomplete", tokenization: "edgeGram", minGrams: 2, maxGrams: 15 }
          ],
          canal: {
            type: "document",
            fields: {
              nombre: [
                { type: "string" },
                { type: "autocomplete", tokenization: "edgeGram", minGrams: 2, maxGrams: 15 }
              ],
              codigo: { type: "string" }
            }
          },
          estado: { type: "string" }
        }
      },
      cuenta: {
        type: "document",
        fields: {
          tipo: { type: "string" },
          sucursal: { type: "string" },
          numero: { type: "string" }
        }
      }
    }
  }
});

// ── Vector Search: movimientos_vector (1024 dims, cosine) ──
// El campo comercio.embedding se genera con Voyage AI (voyage-4-large, 1024 dims).
db.movimientos.createSearchIndex("movimientos_vector", "vectorSearch", {
  fields: [
    { type: "vector", path: "comercio.embedding", numDimensions: 1024, similarity: "cosine" },
    { type: "filter", path: "cuenta.numero" },
    { type: "filter", path: "movimiento.fecha_iso" },
    { type: "filter", path: "comercio.categoria" },
    { type: "filter", path: "movimiento.tipo" }
  ]
});

print("✅ Search/Vector indexes creados (pueden tardar minutos en quedar READY)");
