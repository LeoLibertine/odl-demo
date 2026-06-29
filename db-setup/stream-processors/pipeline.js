// ═══════════════════════════════════════════════════════════════════════
// Atlas Stream Processor Pipeline — Ensamblaje CDC desde MSK
// ═══════════════════════════════════════════════════════════════════════
//
// Lee 3 fragmentos CDC (HEADER, MONETARY, METADATA) del topic Kafka,
// los agrupa por correlation.key en ventana de 10s, ensambla el
// documento final y lo escribe en bancolombia_odl.movimientos.
//
// Uso con mongosh conectado al SP instance:
//   sp.createStreamProcessor("bancolombia-assembler", <pipeline>)
//   sp["bancolombia-assembler"].start()
//
// Conexiones requeridas en el registry:
//   - "msk-bancolombia" (Kafka, tipo source)
//   - La conexión Atlas default del SP instance (para $merge)

const pipeline = [
  // ── 1. SOURCE: Leer del topic Kafka en MSK ──
  {
    $source: {
      connectionName: "msk-bancolombia",
      topic: "bancolombia.depositos.cdc.raw",
      tsFieldName: "_produced_at"
    }
  },

  // ── 2. VALIDATE: Asegurar estructura mínima ──
  {
    $match: {
      "correlation.key": { $exists: true },
      "correlation.fragment_type": { $exists: true },
      "after": { $exists: true }
    }
  },

  // ── 3. TUMBLING WINDOW: Agrupar fragmentos por correlation key ──
  {
    $tumblingWindow: {
      interval: { size: 1, unit: "second" },
      allowedLateness: { size: 1, unit: "second" },
      idleTimeout: { size: 1, unit: "second" },
      pipeline: [
        {
          $group: {
            _id: "$correlation.key",
            fragments: { $push: "$$ROOT" },
            fragment_count: { $sum: 1 },
            first_ts: { $min: "$_produced_at" }
          }
        }
      ]
    }
  },

  // ── 4. EXTRACT: Separar cada tipo de fragmento ──
  {
    $addFields: {
      _h: {
        $arrayElemAt: [
          { $filter: { input: "$fragments", cond: { $eq: ["$$this.correlation.fragment_type", "HEADER"] } } },
          0
        ]
      },
      _m: {
        $arrayElemAt: [
          { $filter: { input: "$fragments", cond: { $eq: ["$$this.correlation.fragment_type", "MONETARY"] } } },
          0
        ]
      },
      _d: {
        $arrayElemAt: [
          { $filter: { input: "$fragments", cond: { $eq: ["$$this.correlation.fragment_type", "METADATA"] } } },
          0
        ]
      }
    }
  },

  // ── 5. FILTER: Solo transacciones completas (3 fragmentos) ──
  {
    $match: {
      fragment_count: 3,
      "_h": { $ne: null },
      "_m": { $ne: null },
      "_d": { $ne: null }
    }
  },

  // ── 6. ASSEMBLE: Construir documento final ──
  {
    $project: {
      _id: 0,
      cuenta: {
        numero: "$_h.after.NUMCTA",
        tipo: "$_h.after.TIPCTA",
        sucursal: "$_h.after.CODSUC"
      },
      movimiento: {
        tipo: "$_h.after.TIPMOV",
        fecha_iso: "$$NOW",
        hora: {
          $concat: [
            { $substrBytes: ["$_h.after.HORMOV", 0, 2] }, ":",
            { $substrBytes: ["$_h.after.HORMOV", 2, 2] }, ":",
            { $substrBytes: ["$_h.after.HORMOV", 4, 2] }
          ]
        },
        canal: {
          codigo: "$_h.after.CODCAN",
          nombre: "$_h.after.CODCAN"
        },
        estado: {
          $switch: {
            branches: [
              { case: { $eq: ["$_h.after.ESTTRN", "00"] }, then: "APPROVED" },
              { case: { $eq: ["$_h.after.ESTTRN", "01"] }, then: "PENDING" }
            ],
            default: "REJECTED"
          }
        },
        codtrn: "$_h.after.CODTRN",
        secuencia: "$_h.after.SECMOV"
      },
      monetario: {
        valor: { $toDouble: "$_m.after.VALTRA" },
        moneda: "$_m.after.CODMON",
        saldo_anterior: { $toDouble: "$_m.after.SLDANT" },
        saldo_nuevo: { $toDouble: "$_m.after.SLDNUE" },
        signo: "$_m.after.SIGNO",
        gmf: { $toDouble: { $ifNull: ["$_m.after.VALGMF", 0] } }
      },
      metadata: {
        referencia: "$_d.after.NUMREF",
        descripcion: "$_d.after.DESCRP",
        cuenta_destino: "$_d.after.CTADES",
        banco_destino: "$_d.after.BANDES",
        nombre_destino: "$_d.after.NOMDES",
        identificacion: "$_d.after.NUMIDE",
        tipo_id: "$_d.after.TIPIDE",
        ip_origen: "$_d.after.IPORIG",
        user_agent: "$_d.after.USERAG",
        dispositivo: "$_d.after.DISPOS",
        geo: {
          lat: { $toDouble: { $ifNull: ["$_d.after.LATGEO", 0] } },
          lon: { $toDouble: { $ifNull: ["$_d.after.LONGGEO", 0] } }
        }
      },
      _asp_metadata: {
        correlation_key: "$_id",
        assembly_timestamp: "$$NOW",
        is_complete: true,
        fragment_count: "$fragment_count",
        source: "ASP_MSK",
        first_fragment_at: "$first_ts"
      }
    }
  },

  // ── 7. MERGE: Escribir en MongoDB Atlas ──
  {
    $merge: {
      into: {
        connectionName: "StreamsAtlasConnection",
        db: "bancolombia_odl",
        coll: "movimientos"
      }
    }
  }
];

// Para usar en mongosh:
// sp.createStreamProcessor("bancolombia-assembler", pipeline);
// sp["bancolombia-assembler"].start();
