"""
══════════════════════════════════════════════════════════════════════════════════
  BANCOLOMBIA ODL — API de Canales Digitales + Simulación Read-Your-Writes
  Technical Feasibility Workshop (TFW)
══════════════════════════════════════════════════════════════════════════════════

  Simula lo que los canales digitales de Bancolombia (App Móvil, Banca Web,
  ATM, Sucursales, Nequi) consumirían en producción contra el ODL.

  ENDPOINTS DE LECTURA (9):
    GET  /api/v1/odl/health              → Estado del ODL y estadísticas
    GET  /api/v1/odl/cuentas-ejemplo     → N cuentas aleatorias para demo
    GET  /api/v1/cuentas/{n}/saldo       → Último saldo de una cuenta
    GET  /api/v1/cuentas/{n}/movimientos → Histórico paginado con filtros
    GET  /api/v1/cuentas/{n}/estado-cuenta → Extracto completo
    GET  /api/v1/cuentas/{n}/resumen     → Análisis de actividad
    GET  /api/v1/busqueda/texto          → Atlas Search sobre movimientos
    GET  /api/v1/canales/distribucion    → Distribución por canal digital
    GET  /api/v1/odl/stats               → Métricas del pipeline

  ENDPOINTS DE SIMULACIÓN (2) — Read-Your-Writes:
    POST /api/v1/cuentas/{n}/simular-movimiento → Produce 3 fragmentos CDC a Kafka
    GET  /api/v1/cuentas/{n}/verificar-cambio   → Polling para detectar cambio de saldo

  EJECUCIÓN:
    export MONGODB_ODL_URI="mongodb+srv://..."
    export KAFKA_BOOTSTRAP_SERVERS="..."    # (opcional — fallback a write directo)
    export KAFKA_API_KEY="..."              # (opcional)
    export KAFKA_API_SECRET="..."           # (opcional)
    python api_canales.py

  Swagger UI: http://0.0.0.0:8788/docs
══════════════════════════════════════════════════════════════════════════════════
"""

import os
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response
from pymongo import MongoClient

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN — importada desde config.py
# ═══════════════════════════════════════════════════════════════════════════════

from config import (
    MONGODB_ODL_URI, DB_NAME_ODL, COLLECTION_MOVIMIENTOS,
    API_PORT, API_HOST, KAFKA_BOOTSTRAP_SERVERS,
    print_config_status,
)

# TTS config (optional)
try:
    from config import OPENAI_API_KEY, OPENAI_TTS_URL, OPENAI_TTS_MODEL, OPENAI_TTS_VOICE
except ImportError:
    OPENAI_API_KEY = ""
    OPENAI_TTS_URL = "https://api.openai.com/v1/audio/speech"
    OPENAI_TTS_MODEL = "tts-1"
    OPENAI_TTS_VOICE = "nova"

MONGODB_URI = MONGODB_ODL_URI
DB_NAME = DB_NAME_ODL
COLLECTION = COLLECTION_MOVIMIENTOS
PORT = API_PORT

# ═══════════════════════════════════════════════════════════════════════════════
#  MONGODB CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

if not MONGODB_URI:
    print("⚠️  MONGODB_ODL_URI no configurada en config.py")
    print("   La API arrancará pero los endpoints fallarán.\n")

client = None
db = None
col = None

def connect_db():
    """Conecta a MongoDB Atlas ODL."""
    global client, db, col
    if not MONGODB_URI:
        return False
    try:
        client = MongoClient(MONGODB_URI, maxPoolSize=20, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        col = db[COLLECTION]
        # Verify connection
        count = col.estimated_document_count()
        print(f"✅ MongoDB conectado — {DB_NAME}.{COLLECTION}: {count:,} documentos")
        # Compartir colección con módulo de Chat IA
        if chat_set_col:
            chat_set_col(col)
            print("✅ Colección compartida con Chat IA")
        return True
    except Exception as e:
        print(f"❌ Error conectando a MongoDB: {e}")
        return False

def get_col():
    """Retorna la colección o lanza error."""
    if col is None:
        raise HTTPException(503, "MongoDB ODL no disponible. Verificar MONGODB_ODL_URI.")
    return col

# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def fmt_cop(valor):
    """Formatea un número como moneda COP."""
    if valor is None:
        return "$0"
    return f"${int(valor):,}".replace(",", ".")

def meta_block(query_time_ms, index_used="IXSCAN", source="ODL"):
    """Bloque _meta estándar para cada respuesta."""
    return {
        "query_time_ms": round(query_time_ms, 2),
        "sla_target_ms": 400,
        "sla_cumple": query_time_ms < 400,
        "index_used": index_used,
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mejora_vs_legacy": f"{round(400 / max(query_time_ms, 0.1), 1)}×",
    }

def safe_get(doc, *keys):
    """Navegación segura en documentos anidados."""
    current = doc
    for k in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(k)
    return current

# ═══════════════════════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="Bancolombia ODL — API de Canales Digitales",
    description=(
        "Simulación de los endpoints que los canales digitales de Bancolombia "
        "consumirían contra el Operational Data Layer (MongoDB Atlas). "
        "TFW — Technical Feasibility Workshop."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Query-Time-Ms", "X-Index-Used"],
)


# ═══════════════════════════════════════════════════════════════════════════════
#  INTEGRACIÓN DEL MÓDULO DE SIMULACIÓN (Read-Your-Writes)
# ═══════════════════════════════════════════════════════════════════════════════

try:
    from api_simulate import router as simulate_router
    app.include_router(simulate_router)
    print("✅ Módulo Read-Your-Writes integrado (api_simulate.py)")
except ImportError:
    print("ℹ️  api_simulate.py no encontrado — endpoints de simulación no disponibles")
    print("   Para habilitarlos, coloca api_simulate.py en la misma carpeta.\n")


# ═══════════════════════════════════════════════════════════════════════════════
#  INTEGRACIÓN DEL MÓDULO DE CHAT IA (Vector Search + Claude)
# ═══════════════════════════════════════════════════════════════════════════════

try:
    from api_chat import router as chat_router, set_collection as chat_set_col
    app.include_router(chat_router)
    print("✅ Módulo Chat IA integrado (api_chat.py)")
except ImportError:
    chat_set_col = None
    print("ℹ️  api_chat.py no encontrado — endpoints de Chat IA no disponibles")
    print("   Para habilitarlos, coloca api_chat.py en la misma carpeta.\n")

# ── Auto-Enrich: Change Stream → comercio{} + embedding automático ──
try:
    from auto_enrich import start_watcher as _start_enrich_watcher
    _has_auto_enrich = True
    print("✅ Módulo Auto-Enrich disponible (auto_enrich.py)")
except ImportError:
    _has_auto_enrich = False
    print("ℹ️  auto_enrich.py no encontrado — enriquecimiento automático no disponible")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT TTS: Text-to-Speech via OpenAI
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_text_for_tts(text: str) -> str:
    """
    Limpia texto para que OpenAI TTS pronuncie correctamente en español.
    - Convierte $1.234.567 COP → 1 millón 234 mil 567 pesos
    - Remueve markdown (**bold**, _italic_, etc.)
    - Remueve emojis que se leen raro
    - Limpia formato
    """
    import re

    if not text:
        return text

    # 1. Remover emojis y caracteres especiales
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d\ufe0f"
        "]+", flags=re.UNICODE
    )
    text = emoji_pattern.sub(" ", text)

    # 2. Remover markdown
    text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)  # **bold**
    text = re.sub(r'\*([^*]+)\*', r'\1', text)       # *italic*
    text = re.sub(r'__([^_]+)__', r'\1', text)       # __bold__
    text = re.sub(r'_([^_]+)_', r'\1', text)         # _italic_
    text = re.sub(r'#+\s*', '', text)                 # ### headers
    text = re.sub(r'[-•]\s+', '', text)               # bullet points

    # 3. Función para convertir número a palabras en español
    def num_to_words_es(n):
        """Convierte número a palabras en español (simplificado para montos bancarios)."""
        if n == 0:
            return "cero"
        
        parts = []
        if n >= 1_000_000_000:
            billones = n // 1_000_000_000
            n %= 1_000_000_000
            parts.append(f"{billones} mil millones" if billones > 1 else "mil millones")
        if n >= 1_000_000:
            millones = n // 1_000_000
            n %= 1_000_000
            parts.append(f"{millones} millones" if millones > 1 else "un millón")
        if n >= 1_000:
            miles = n // 1_000
            n %= 1_000
            if miles == 1:
                parts.append("mil")
            else:
                parts.append(f"{miles} mil")
        if n > 0:
            parts.append(str(int(n)))

        return " ".join(parts)

    # 4. Convertir montos colombianos: $1.234.567 COP → "1 millón 234 mil 567 pesos"
    # Formato: $XXX.XXX.XXX o $XXX.XXX.XXX COP o $XXX.XXX.XXX,XX COP
    def replace_money(m):
        prefix = m.group(1) or ""
        amount_str = m.group(2)
        decimals = m.group(3) or ""

        # Remover puntos de miles: 1.234.567 → 1234567
        clean_num = amount_str.replace(".", "")
        try:
            num = int(clean_num)
        except ValueError:
            return m.group(0)

        words = num_to_words_es(num)
        result = f"{words} pesos"
        if decimals:
            result += f" con {decimals.lstrip(',')} centavos"
        return f"{prefix}{result} "

    # Capturar $1.234.567 COP, $1.234.567,50, $1.234.567
    text = re.sub(
        r'(\s|^)\$?([\d]{1,3}(?:\.[\d]{3})+)(,\d{1,2})?\s*(?:COP|pesos)?\s*',
        replace_money, text
    )

    # También manejar montos sin puntos: $897000 COP
    def replace_plain_money(m):
        prefix = m.group(1) or ""
        num = int(m.group(2))
        if num < 100:  # Probablemente no es dinero
            return m.group(0)
        words = num_to_words_es(num)
        return f"{prefix}{words} pesos "

    text = re.sub(r'(\s|^)\$(\d{4,})\s*(?:COP|pesos)?\s*', replace_plain_money, text)

    # 5. Convertir porcentajes: 45.3% → "45 punto 3 por ciento"
    text = re.sub(r'(\d+)\.(\d+)%', r'\1 punto \2 por ciento', text)
    text = re.sub(r'(\d+)%', r'\1 por ciento', text)

    # 6. Limpiar espacios múltiples y newlines excesivos
    text = re.sub(r'\n{2,}', '. ', text)
    text = re.sub(r'\n', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'\.\s*\.', '.', text)  # Dobles puntos

    return text.strip()


@app.post("/api/v1/tts", tags=["Chat IA"])
async def text_to_speech(body: dict):
    """
    🔊 Convierte texto a audio usando OpenAI TTS.
    Retorna audio MP3 de alta calidad con voz natural.
    Incluye limpieza inteligente de texto para español colombiano.
    """
    import json
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

    text = body.get("text", "").strip()
    if not text:
        raise HTTPException(400, "Campo 'text' requerido")
    if not OPENAI_API_KEY:
        raise HTTPException(503, "OPENAI_API_KEY no configurada")

    # Limpiar texto para pronunciación natural
    text = _clean_text_for_tts(text)

    # Limitar largo para evitar costos excesivos
    if len(text) > 4000:
        text = text[:4000]

    voice = body.get("voice", OPENAI_TTS_VOICE)

    t0 = time.time()
    try:
        payload = json.dumps({
            "model": OPENAI_TTS_MODEL,
            "input": text,
            "voice": voice,
            "response_format": "mp3",
            "speed": 1.05,
        }).encode()

        req = Request(OPENAI_TTS_URL, data=payload, headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        }, method="POST")

        with urlopen(req, timeout=30) as resp:
            audio_bytes = resp.read()

        elapsed = round((time.time() - t0) * 1000)
        print(f"  🔊 TTS: {len(text)} chars → {len(audio_bytes)//1024}KB MP3, {elapsed}ms")

        return Response(
            content=audio_bytes,
            media_type="audio/mpeg",
            headers={
                "X-TTS-Latency-Ms": str(elapsed),
                "X-TTS-Voice": voice,
                "X-TTS-Chars": str(len(text)),
            }
        )
    except HTTPError as e:
        err_body = e.read().decode() if e.fp else ""
        raise HTTPException(502, f"OpenAI TTS error: {e.code} — {err_body}")
    except Exception as e:
        raise HTTPException(502, f"OpenAI TTS no disponible: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 1: HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/odl/health", tags=["Sistema"])
async def health():
    """
    🏥 Estado del ODL — conexión, conteos, índices.
    Primer endpoint para verificar que todo funciona.
    """
    t0 = time.time()
    c = get_col()

    try:
        count = c.estimated_document_count()
        indexes = list(c.list_indexes())
        index_names = [idx["name"] for idx in indexes]

        # Check collections
        collections = db.list_collection_names()

        # DLQ count
        dlq_count = 0
        if "movimientos_dlq" in collections:
            dlq_count = db.movimientos_dlq.estimated_document_count()

        elapsed = round((time.time() - t0) * 1000, 2)

        return {
            "status": "healthy",
            "odl": {
                "database": DB_NAME,
                "collection": COLLECTION,
                "documentos": count,
                "indices": len(index_names),
                "nombres_indices": index_names,
                "colecciones": collections,
            },
            "dlq": {
                "documentos": dlq_count,
                "tasa_error": f"{(dlq_count / max(count, 1) * 100):.2f}%",
            },
            "kafka_simulacion": {
                "configurado": bool(KAFKA_BOOTSTRAP_SERVERS),
                "nota": "Si no está configurado, la simulación usa write directo a MongoDB",
            },
            "_meta": meta_block(elapsed, "METADATA", "SYSTEM"),
        }
    except Exception as e:
        raise HTTPException(500, f"Error verificando ODL: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 2: CUENTAS DE EJEMPLO
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/odl/cuentas-ejemplo", tags=["Sistema"])
async def cuentas_ejemplo(n: int = Query(6, ge=1, le=20, description="Cantidad de cuentas")):
    """
    🎲 Retorna N cuentas aleatorias del ODL para pruebas.
    La app de demo las usa en la pantalla de login.
    """
    t0 = time.time()
    c = get_col()

    pipeline = [
        {"$sample": {"size": n * 3}},
        {"$group": {
            "_id": "$cuenta.numero",
            "tipo": {"$first": "$cuenta.tipo"},
            "sucursal": {"$first": "$cuenta.sucursal"},
            "saldo": {"$first": "$monetario.saldo_nuevo"},
        }},
        {"$match": {"_id": {"$ne": None}}},
        {"$limit": n},
        {"$project": {
            "_id": 0,
            "numero": "$_id",
            "tipo": 1,
            "sucursal": 1,
            "saldo": {"$ifNull": ["$saldo", 0]},
        }},
    ]

    results = list(c.aggregate(pipeline))
    cuentas = [
        {**r, "saldo_formateado": fmt_cop(r.get("saldo", 0))}
        for r in results
    ]
    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "cuentas": cuentas,
        "total": len(cuentas),
        "_meta": meta_block(elapsed, "$sample + $group", "ODL"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 3: CONSULTA DE SALDO ⭐ (La más importante)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/cuentas/{numero}/saldo", tags=["Canales Digitales"])
async def consulta_saldo(numero: str):
    """
    💰 Consulta de saldo — Simula GET /saldo de la App Bancolombia.

    Retorna el saldo actual, datos de la cuenta y último movimiento.
    Esta es la query más frecuente (60%+ del tráfico de canales).
    Target: < 5ms con IXSCAN.
    """
    t0 = time.time()
    c = get_col()

    # findOne ordenado por fecha desc → último movimiento = saldo actual
    doc = c.find_one(
        {"cuenta.numero": numero},
        sort=[("movimiento.fecha_iso", -1)],
    )

    if not doc:
        raise HTTPException(404, f"Cuenta {numero} no encontrada en el ODL")

    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "cuenta": {
            "numero": numero,
            "tipo": safe_get(doc, "cuenta", "tipo"),
            "sucursal": safe_get(doc, "cuenta", "sucursal"),
        },
        "saldo": {
            "actual": safe_get(doc, "monetario", "saldo_nuevo"),
            "anterior": safe_get(doc, "monetario", "saldo_anterior"),
            "moneda": "COP",
            "formateado": fmt_cop(safe_get(doc, "monetario", "saldo_nuevo")),
        },
        "ultimo_movimiento": {
            "tipo": safe_get(doc, "movimiento", "tipo"),
            "fecha": safe_get(doc, "movimiento", "fecha_iso"),
            "hora": safe_get(doc, "movimiento", "hora"),
            "canal": safe_get(doc, "movimiento", "canal", "nombre"),
            "canal_codigo": safe_get(doc, "movimiento", "canal", "codigo"),
            "monto": safe_get(doc, "monetario", "valor"),
            "signo": safe_get(doc, "monetario", "signo"),
            "monto_formateado": fmt_cop(safe_get(doc, "monetario", "valor")),
        },
        "_meta": meta_block(elapsed),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 4: HISTÓRICO DE MOVIMIENTOS (Paginado + Filtros)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/cuentas/{numero}/movimientos", tags=["Canales Digitales"])
async def historico_movimientos(
    numero: str,
    meses: int = Query(5, ge=1, le=12, description="Meses hacia atrás"),
    page: int = Query(1, ge=1, description="Página"),
    size: int = Query(12, ge=1, le=50, description="Movimientos por página"),
    canal: Optional[str] = Query(None, description="Filtro por canal: APP, WEB, ATM, SUC, NEQ"),
):
    """
    📋 Histórico de movimientos — Cristian pidió "hasta 4 o 5 meses hacia atrás".
    Paginado, filtrable por canal y período.
    """
    t0 = time.time()
    c = get_col()

    fecha_desde = datetime.now(timezone.utc) - timedelta(days=meses * 30)

    filtro = {
        "cuenta.numero": numero,
        "movimiento.fecha_iso": {"$gte": fecha_desde},
    }
    if canal:
        filtro["movimiento.canal.codigo"] = canal.upper()

    # Total para paginación
    total = c.count_documents(filtro)
    total_pages = max(1, (total + size - 1) // size)

    # Fetch page
    skip = (page - 1) * size
    docs = list(
        c.find(filtro, {"_id": 0})
        .sort("movimiento.fecha_iso", -1)
        .skip(skip)
        .limit(size)
    )

    elapsed = round((time.time() - t0) * 1000, 2)

    movimientos = []
    for d in docs:
        movimientos.append({
            "tipo": safe_get(d, "movimiento", "tipo"),
            "fecha": safe_get(d, "movimiento", "fecha_iso"),
            "hora": safe_get(d, "movimiento", "hora"),
            "canal": safe_get(d, "movimiento", "canal"),
            "monto": safe_get(d, "monetario", "valor"),
            "signo": safe_get(d, "monetario", "signo"),
            "saldo": safe_get(d, "monetario", "saldo_nuevo"),
            "monto_formateado": fmt_cop(safe_get(d, "monetario", "valor")),
        })

    return {
        "cuenta": numero,
        "periodo": {"meses": meses, "desde": fecha_desde.isoformat()},
        "filtro_canal": canal,
        "movimientos": movimientos,
        "total_movimientos": total,
        "paginacion": {
            "pagina_actual": page,
            "total_paginas": total_pages,
            "por_pagina": size,
            "total_registros": total,
        },
        "_meta": meta_block(elapsed),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 5: ESTADO DE CUENTA (Extracto)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/cuentas/{numero}/estado-cuenta", tags=["Canales Digitales"])
async def estado_cuenta(
    numero: str,
    meses: int = Query(1, ge=1, le=12, description="Período en meses"),
):
    """
    📄 Estado de cuenta completo — resumen de créditos/débitos, saldo inicial/final,
    canales utilizados, y detalle de movimientos. Listo para renderizar.
    """
    t0 = time.time()
    c = get_col()

    fecha_desde = datetime.now(timezone.utc) - timedelta(days=meses * 30)

    pipeline = [
        {"$match": {
            "cuenta.numero": numero,
            "movimiento.fecha_iso": {"$gte": fecha_desde},
        }},
        {"$sort": {"movimiento.fecha_iso": 1}},
        {"$group": {
            "_id": None,
            "total_creditos": {
                "$sum": {"$cond": [{"$eq": ["$monetario.signo", "C"]}, "$monetario.valor", 0]}
            },
            "total_debitos": {
                "$sum": {"$cond": [{"$eq": ["$monetario.signo", "D"]}, "$monetario.valor", 0]}
            },
            "num_transacciones": {"$sum": 1},
            "saldo_inicial": {"$first": "$monetario.saldo_anterior"},
            "saldo_final": {"$last": "$monetario.saldo_nuevo"},
            "fecha_desde": {"$first": "$movimiento.fecha_iso"},
            "fecha_hasta": {"$last": "$movimiento.fecha_iso"},
            "canales_usados": {"$addToSet": "$movimiento.canal.nombre"},
            "tipo_cuenta": {"$first": "$cuenta.tipo"},
            "sucursal": {"$first": "$cuenta.sucursal"},
            "movimientos": {"$push": {
                "tipo": "$movimiento.tipo",
                "fecha": "$movimiento.fecha_iso",
                "hora": "$movimiento.hora",
                "canal": "$movimiento.canal.nombre",
                "monto": "$monetario.valor",
                "signo": "$monetario.signo",
                "saldo": "$monetario.saldo_nuevo",
            }},
        }},
    ]

    results = list(c.aggregate(pipeline))
    elapsed = round((time.time() - t0) * 1000, 2)

    if not results:
        raise HTTPException(404, f"No hay movimientos para cuenta {numero} en los últimos {meses} meses")

    r = results[0]
    total_movs = len(r.get("movimientos", []))

    return {
        "estado_cuenta": {
            "cuenta": {
                "numero": numero,
                "tipo": r.get("tipo_cuenta"),
                "sucursal": r.get("sucursal"),
            },
            "periodo": {
                "meses": meses,
                "desde": r.get("fecha_desde"),
                "hasta": r.get("fecha_hasta"),
            },
            "resumen": {
                "total_creditos": r.get("total_creditos", 0),
                "total_debitos": r.get("total_debitos", 0),
                "num_transacciones": r.get("num_transacciones", 0),
                "saldo_inicial": r.get("saldo_inicial"),
                "saldo_final": r.get("saldo_final"),
                "canales_usados": r.get("canales_usados", []),
            },
            "movimientos": r.get("movimientos", [])[-30:],  # Últimos 30
            "movimientos_mostrados": min(30, total_movs),
            "total_movimientos_periodo": total_movs,
        },
        "_meta": meta_block(elapsed, "IXSCAN + $group"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 6: RESUMEN DE ACTIVIDAD
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/cuentas/{numero}/resumen", tags=["Canales Digitales"])
async def resumen_actividad(
    numero: str,
    meses: int = Query(3, ge=1, le=12),
):
    """
    📊 Resumen analítico — distribución por canal, promedio de montos, frecuencia.
    Útil para el equipo de operaciones y BackOffice.
    """
    t0 = time.time()
    c = get_col()

    fecha_desde = datetime.now(timezone.utc) - timedelta(days=meses * 30)

    pipeline = [
        {"$match": {
            "cuenta.numero": numero,
            "movimiento.fecha_iso": {"$gte": fecha_desde},
        }},
        {"$facet": {
            "por_canal": [
                {"$group": {
                    "_id": "$movimiento.canal.nombre",
                    "count": {"$sum": 1},
                    "total": {"$sum": "$monetario.valor"},
                    "promedio": {"$avg": "$monetario.valor"},
                }},
                {"$sort": {"count": -1}},
            ],
            "por_tipo": [
                {"$group": {
                    "_id": "$movimiento.tipo",
                    "count": {"$sum": 1},
                    "total": {"$sum": "$monetario.valor"},
                }},
                {"$sort": {"count": -1}},
            ],
            "general": [
                {"$group": {
                    "_id": None,
                    "total_movimientos": {"$sum": 1},
                    "monto_total": {"$sum": "$monetario.valor"},
                    "monto_promedio": {"$avg": "$monetario.valor"},
                    "monto_max": {"$max": "$monetario.valor"},
                    "monto_min": {"$min": "$monetario.valor"},
                }},
            ],
        }},
    ]

    results = list(c.aggregate(pipeline))
    elapsed = round((time.time() - t0) * 1000, 2)

    r = results[0] if results else {"por_canal": [], "por_tipo": [], "general": []}
    general = r["general"][0] if r["general"] else {}

    return {
        "cuenta": numero,
        "periodo_meses": meses,
        "resumen_general": {
            "total_movimientos": general.get("total_movimientos", 0),
            "monto_total": general.get("monto_total", 0),
            "monto_promedio": round(general.get("monto_promedio", 0)),
            "monto_max": general.get("monto_max", 0),
            "monto_min": general.get("monto_min", 0),
        },
        "por_canal": [
            {"canal": x["_id"], "movimientos": x["count"], "total": x["total"],
             "promedio": round(x.get("promedio", 0))}
            for x in r["por_canal"]
        ],
        "por_tipo": [
            {"tipo": x["_id"], "movimientos": x["count"], "total": x["total"]}
            for x in r["por_tipo"]
        ],
        "_meta": meta_block(elapsed, "IXSCAN + $facet"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 7: BÚSQUEDA DE TEXTO (Atlas Search)  ⭐ Diferenciador
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/busqueda/texto", tags=["Atlas Search"])
async def busqueda_texto(
    q: str = Query(..., min_length=1, description="Texto a buscar: 'nómina APP', 'retiro ATM enero'"),
    limit: int = Query(15, ge=1, le=50),
):
    """
    🔍 Búsqueda inteligente sobre movimientos — Atlas Search con fuzzy matching.

    EL DIFERENCIADOR: Esto es algo que el iSeries NO puede hacer. Nunca.
    Búsqueda de texto libre con relevancia, fuzzy matching y autocompletado.

    Requiere el Atlas Search Index 'movimientos_search' creado.
    Si el índice no existe, hace fallback a regex (funcional pero sin relevancia).
    """
    t0 = time.time()
    c = get_col()

    motor = "Atlas Search"
    movimientos = []

    # Intentar Atlas Search primero
    try:
        pipeline = [
            {"$search": {
                "index": "movimientos_search",
                "text": {
                    "query": q,
                    "path": [
                        "movimiento.tipo",
                        "movimiento.canal.nombre",
                        "cuenta.sucursal",
                        "cuenta.tipo",
                    ],
                    "fuzzy": {"maxEdits": 1},
                },
            }},
            {"$limit": limit},
            {"$addFields": {"relevancia": {"$meta": "searchScore"}}},
            {"$project": {
                "_id": 0,
                "cuenta": 1,
                "movimiento": 1,
                "monetario": 1,
                "relevancia": {"$round": ["$relevancia", 3]},
            }},
        ]
        docs = list(c.aggregate(pipeline))

        for d in docs:
            movimientos.append({
                "tipo": safe_get(d, "movimiento", "tipo"),
                "fecha": safe_get(d, "movimiento", "fecha_iso"),
                "hora": safe_get(d, "movimiento", "hora"),
                "canal": safe_get(d, "movimiento", "canal"),
                "cuenta": safe_get(d, "cuenta"),
                "monto": safe_get(d, "monetario", "valor"),
                "signo": safe_get(d, "monetario", "signo"),
                "saldo": safe_get(d, "monetario", "saldo_nuevo"),
                "relevancia": d.get("relevancia"),
            })

    except Exception as search_err:
        # Fallback a regex si Atlas Search no está disponible
        motor = "Regex (fallback — Atlas Search Index no disponible)"
        import re
        pattern = re.compile(re.escape(q), re.IGNORECASE)

        docs = list(
            c.find(
                {"$or": [
                    {"movimiento.tipo": pattern},
                    {"movimiento.canal.nombre": pattern},
                    {"cuenta.sucursal": pattern},
                ]},
                {"_id": 0},
            )
            .sort("movimiento.fecha_iso", -1)
            .limit(limit)
        )

        for d in docs:
            movimientos.append({
                "tipo": safe_get(d, "movimiento", "tipo"),
                "fecha": safe_get(d, "movimiento", "fecha_iso"),
                "hora": safe_get(d, "movimiento", "hora"),
                "canal": safe_get(d, "movimiento", "canal"),
                "cuenta": safe_get(d, "cuenta"),
                "monto": safe_get(d, "monetario", "valor"),
                "signo": safe_get(d, "monetario", "signo"),
                "saldo": safe_get(d, "monetario", "saldo_nuevo"),
                "relevancia": None,
            })

    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "query": q,
        "motor": motor,
        "resultados": len(movimientos),
        "movimientos": movimientos,
        "_meta": meta_block(elapsed, "$search" if "Atlas" in motor else "$regex"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 8: DISTRIBUCIÓN POR CANAL
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/canales/distribucion", tags=["Analíticos"])
async def distribucion_canales():
    """
    📊 Distribución de transacciones por canal digital.
    Usa $sample para eficiencia sobre 2M+ documentos.
    """
    t0 = time.time()
    c = get_col()

    pipeline = [
        {"$sample": {"size": 50000}},
        {"$group": {
            "_id": {
                "codigo": "$movimiento.canal.codigo",
                "nombre": "$movimiento.canal.nombre",
            },
            "count": {"$sum": 1},
            "monto_total": {"$sum": "$monetario.valor"},
            "monto_promedio": {"$avg": "$monetario.valor"},
        }},
        {"$sort": {"count": -1}},
        {"$project": {
            "_id": 0,
            "canal": "$_id.nombre",
            "codigo": "$_id.codigo",
            "transacciones": "$count",
            "monto_total": "$monto_total",
            "monto_promedio": {"$round": ["$monto_promedio", 0]},
            "porcentaje": {
                "$round": [{"$multiply": [{"$divide": ["$count", 50000]}, 100]}, 1]
            },
        }},
    ]

    results = list(c.aggregate(pipeline))
    elapsed = round((time.time() - t0) * 1000, 2)

    return {
        "canales": results,
        "muestra_size": 50000,
        "nota": "Basado en muestra aleatoria de 50K docs (precisión estadística ~99%)",
        "_meta": meta_block(elapsed, "$sample + $group", "ODL Analytics"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 9: ESTADÍSTICAS DEL PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/v1/odl/stats", tags=["Sistema"])
async def odl_stats():
    """
    📈 Métricas del pipeline — documentos, cuentas únicas, latencias ASP,
    tasas de éxito. Vista rápida del estado del TFW.
    """
    t0 = time.time()
    c = get_col()

    pipeline = [
        {"$sample": {"size": 30000}},
        {"$group": {
            "_id": None,
            "cuentas_unicas": {"$addToSet": "$cuenta.numero"},
            "avg_assembly_ms": {"$avg": "$_asp_metadata.assembly_latency_ms"},
            "avg_e2e_ms": {"$avg": "$_asp_metadata.e2e_latency_ms"},
            "completos": {
                "$sum": {"$cond": [{"$eq": ["$_asp_metadata.is_complete", True]}, 1, 0]}
            },
            "total_sample": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0,
            "cuentas_unicas": {"$size": "$cuentas_unicas"},
            "avg_assembly_ms": {"$round": ["$avg_assembly_ms", 1]},
            "avg_e2e_ms": {"$round": ["$avg_e2e_ms", 1]},
            "tasa_exito": {
                "$round": [{"$multiply": [{"$divide": ["$completos", "$total_sample"]}, 100]}, 2]
            },
        }},
    ]

    results = list(c.aggregate(pipeline))
    total_docs = c.estimated_document_count()
    elapsed = round((time.time() - t0) * 1000, 2)

    stats = results[0] if results else {}

    return {
        "total_documentos": total_docs,
        "cuentas_unicas": stats.get("cuentas_unicas", 0),
        "latencias": {
            "assembly_promedio_ms": stats.get("avg_assembly_ms"),
            "e2e_promedio_ms": stats.get("avg_e2e_ms"),
        },
        "tasa_exito": stats.get("tasa_exito"),
        "_meta": meta_block(elapsed, "$sample + $group", "ODL Metrics"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ROOT
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/", tags=["Sistema"])
async def root():
    """Información del servicio y lista de endpoints."""
    return {
        "servicio": "Bancolombia ODL — API de Canales Digitales",
        "version": "2.0.0",
        "tfw": "Technical Feasibility Workshop — MongoDB Atlas",
        "swagger": "/docs",
        "endpoints": {
            "lectura": [
                "GET  /api/v1/odl/health",
                "GET  /api/v1/odl/cuentas-ejemplo?n=6",
                "GET  /api/v1/cuentas/{numero}/saldo",
                "GET  /api/v1/cuentas/{numero}/movimientos?meses=5&page=1&size=12&canal=APP",
                "GET  /api/v1/cuentas/{numero}/estado-cuenta?meses=1",
                "GET  /api/v1/cuentas/{numero}/resumen?meses=3",
                "GET  /api/v1/busqueda/texto?q=nomina+APP",
                "GET  /api/v1/canales/distribucion",
                "GET  /api/v1/odl/stats",
            ],
            "simulacion": [
                "POST /api/v1/cuentas/{numero}/simular-movimiento",
                "GET  /api/v1/cuentas/{numero}/verificar-cambio?saldo_anterior=X",
            ],
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  APP FRONTEND — Sirve el HTML de la App Bancolombia
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/app", tags=["Sistema"], include_in_schema=False)
async def serve_app():
    """Sirve la App Bancolombia (bancolombia_odl_demo.html)."""
    import pathlib
    html = pathlib.Path(__file__).parent / "bancolombia_odl_demo.html"
    if not html.exists():
        raise HTTPException(404, "bancolombia_odl_demo.html no encontrado en la misma carpeta que api_canales.py")
    return FileResponse(str(html), media_type="text/html")


@app.get("/dashboard", tags=["Sistema"], include_in_schema=False)
async def serve_dashboard():
    """Sirve el Dashboard ODL Live (bancolombia-odl-dashboard-live.html)."""
    import pathlib
    html = pathlib.Path(__file__).parent / "bancolombia-odl-dashboard-live.html"
    if not html.exists():
        raise HTTPException(404, "bancolombia-odl-dashboard-live.html no encontrado")
    return FileResponse(str(html), media_type="text/html")


# ═══════════════════════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    connect_db()
    # Iniciar auto-enrich watcher (Change Stream → comercio + embedding)
    if _has_auto_enrich:
        try:
            _start_enrich_watcher()
        except Exception as e:
            print(f"⚠️  Auto-enrich no pudo iniciar: {e}")


if __name__ == "__main__":
    print_config_status()
    print("  BANCOLOMBIA ODL — API de Canales Digitales v2.0")
    print(f"  Swagger:  http://0.0.0.0:{PORT}/docs")
    print()
    print("  Endpoints de lectura:    9 (canales digitales)")
    print("  Endpoints de simulación: 2 (Read-Your-Writes)")
    print("  Endpoints de Chat IA:    3 (Vector Search + Claude)")
    print("  Auto-Enrich:             Change Stream → Voyage AI")
    print()

    uvicorn.run(app, host=API_HOST, port=PORT)
