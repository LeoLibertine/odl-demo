"""
══════════════════════════════════════════════════════════════════════════════════
  BANCOLOMBIA ODL — Chat IA con Vector Search + Claude
  Technical Feasibility Workshop (TFW)
══════════════════════════════════════════════════════════════════════════════════

  Módulo que agrega capacidad de chat en lenguaje natural al ODL.
  Se integra como router en api_canales.py (mismo patrón que api_simulate.py).

  FLUJO:
    1. Usuario pregunta: "¿Cuánto gasté en restaurantes este mes?"
    2. Voyage AI genera embedding de la pregunta (1024 dims)
    3. $vectorSearch encuentra movimientos semánticamente relevantes
    4. Claude analiza los datos y genera respuesta natural en español
    5. Retorna respuesta + datos de soporte + métricas

  ENDPOINTS:
    POST /api/v1/chat                    → Chat completo (Vector Search + Claude)
    POST /api/v1/chat/vector-only        → Solo Vector Search (sin LLM, para debug)
    GET  /api/v1/chat/sugerencias        → Preguntas sugeridas para demo

  CONFIGURACIÓN (variables de entorno — ver .env.example):
    VOYAGE_API_KEY        → Voyage AI via MongoDB Atlas
    ANTHROPIC_API_KEY     → Claude API

  EJECUCIÓN:
    Colocar api_chat.py junto a api_canales.py. Se detecta automáticamente.
══════════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN — importada desde config.py
# ═══════════════════════════════════════════════════════════════════════════════

log = logging.getLogger("chat-ia")

from config import (
    VOYAGE_API_URL, VOYAGE_API_KEY, VOYAGE_MODEL, VOYAGE_DIMENSIONS,
    ANTHROPIC_API_URL, ANTHROPIC_API_KEY, ANTHROPIC_MODEL,
    VECTOR_INDEX_NAME,
)

# Aliases locales para compatibilidad
VOYAGE_DIMS = VOYAGE_DIMENSIONS
CLAUDE_MODEL = ANTHROPIC_MODEL
CLAUDE_MAX_TOKENS = 1024

# Vector Search config
VECTOR_INDEX = VECTOR_INDEX_NAME
NUM_CANDIDATES = 150
RESULTS_LIMIT = 20

# Patrones que indican consulta cronológica (no semántica)
import re
_CHRONO_PATTERNS = re.compile(
    r"("
    r"[uú]ltim[oa]s?\s+\d+\s*(movimiento|transac|operac|mov)?"
    r"|[uú]ltim[oa]s?\s+(movimiento|transac|operac|mov)"
    r"|m[aá]s\s+recientes?"
    r"|movimientos?\s+recientes?"
    r"|recientes?\s+movimiento"
    r"|movimientos?\s+de\s+(hoy|ayer|esta\s+(semana|mañana|tarde|noche))"
    r"|qu[eé]\s+(he\s+hecho|hice|movimientos\s+tengo|movimientos\s+hice)"
    r"|qu[eé]\s+pas[oó]\s+(hoy|ayer)"
    r"|d[ae]me\s+(los?\s+)?(últimos?\s+)?movimientos?"
    r"|d[ae]me\s+(los?\s+)?recientes?"
    r"|mu[eé]strame\s+(los?\s+)?(últimos?\s+|recientes?\s+)?movimientos?"
    r"|mu[eé]strame\s+(los?\s+)?recientes?"
    r"|listame\s+(los?\s+)?movimientos?"
    r"|qu[eé]\s+movimientos\s+tengo"
    r"|historial\s+reciente"
    r"|actividad\s+reciente"
    r")",
    re.IGNORECASE,
)


def _is_chronological_query(pregunta: str) -> bool:
    return bool(_CHRONO_PATTERNS.search(pregunta))


def _chronological_search(cuenta: str, limit: int = 20) -> list:
    col = get_col()
    pipeline = [
        {"$match": {"cuenta.numero": cuenta}},
        {"$sort": {"movimiento.fecha_iso": -1}},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "cuenta.numero": 1,
            "cuenta.tipo": 1,
            "movimiento.tipo": 1,
            "movimiento.fecha_iso": 1,
            "movimiento.hora": 1,
            "movimiento.canal": 1,
            "monetario.valor": 1,
            "monetario.signo": 1,
            "monetario.saldo_nuevo": 1,
            "monetario.moneda": 1,
            "comercio.nombre": 1,
            "comercio.nombre_normalizado": 1,
            "comercio.categoria": 1,
            "comercio.subcategoria": 1,
            "comercio.descripcion_busqueda": 1,
            "comercio.es_recurrente": 1,
        }},
    ]
    return list(col.aggregate(pipeline))


# ═══════════════════════════════════════════════════════════════════════════════
#  ROUTER + MONGO REF
# ═══════════════════════════════════════════════════════════════════════════════

router = APIRouter(tags=["Chat IA — Vector Search + Claude"])

# Referencia a la colección MongoDB (la comparte api_canales.py)
_col = None


def set_collection(col):
    """Llamado por api_canales.py al importar el módulo."""
    global _col
    _col = col


def get_col():
    if _col is None:
        raise HTTPException(503, "MongoDB no disponible para Chat IA")
    return _col


# ═══════════════════════════════════════════════════════════════════════════════
#  MODELOS Pydantic (Request / Response)
# ═══════════════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    """Cuerpo del request para el endpoint de chat."""
    pregunta: str = Field(..., min_length=3, max_length=500,
                          example="¿Cuánto gasté en restaurantes este mes?")
    cuenta: str = Field(..., min_length=5, max_length=20,
                        example="40000000528607")
    meses: int = Field(3, ge=1, le=12,
                       description="Período de búsqueda en meses")
    limit: int = Field(20, ge=5, le=50,
                       description="Máximo de movimientos para contexto")
    voice: bool = Field(False, description="Modo voz: respuestas cortas y directas")


class VectorOnlyRequest(BaseModel):
    """Request para endpoint de debug (sin LLM)."""
    pregunta: str = Field(..., min_length=3, max_length=500)
    cuenta: Optional[str] = Field(None, description="Filtrar por cuenta (opcional)")
    limit: int = Field(10, ge=1, le=50)


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS: VOYAGE AI (Embeddings)
# ═══════════════════════════════════════════════════════════════════════════════

def generate_query_embedding(text: str) -> list:
    """
    Genera embedding para la pregunta del usuario usando Voyage AI
    via el endpoint de MongoDB Atlas.
    Usa input_type='query' (optimizado para búsqueda, diferente a 'document').
    """
    if not VOYAGE_API_KEY:
        raise HTTPException(500, "VOYAGE_API_KEY no configurada")

    payload = json.dumps({
        "input": [text],
        "model": VOYAGE_MODEL,
        "input_type": "query",  # ← query, no document
    }).encode()

    req = Request(VOYAGE_API_URL, data=payload, headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {VOYAGE_API_KEY}",
    }, method="POST")

    try:
        with urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
        return result["data"][0]["embedding"]
    except HTTPError as e:
        body = e.read().decode() if e.fp else ""
        raise HTTPException(502, f"Voyage AI error: {e.code} — {body}")
    except Exception as e:
        raise HTTPException(502, f"Voyage AI no disponible: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS: VECTOR SEARCH (MongoDB Atlas)
# ═══════════════════════════════════════════════════════════════════════════════

def vector_search(embedding: list, cuenta: str = None,
                  fecha_desde: datetime = None, limit: int = 20) -> list:
    """
    Ejecuta $vectorSearch contra el índice movimientos_vector.
    Opcionalmente filtra por cuenta y fecha.
    """
    col = get_col()

    # Pre-filter (se ejecuta ANTES del vector search → muy eficiente)
    pre_filter = {}
    filter_parts = []

    if cuenta:
        filter_parts.append({"cuenta.numero": {"$eq": cuenta}})
    if fecha_desde:
        filter_parts.append({"movimiento.fecha_iso": {"$gte": fecha_desde}})

    if len(filter_parts) == 1:
        pre_filter = filter_parts[0]
    elif len(filter_parts) > 1:
        pre_filter = {"$and": filter_parts}

    # Pipeline
    pipeline = [
        {
            "$vectorSearch": {
                "index": VECTOR_INDEX,
                "path": "comercio.embedding",
                "queryVector": embedding,
                "numCandidates": NUM_CANDIDATES,
                "limit": limit,
                **({"filter": pre_filter} if pre_filter else {}),
            }
        },
        {
            "$addFields": {
                "score_vector": {"$meta": "vectorSearchScore"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "correlation_key": 1,
                "cuenta.numero": 1,
                "cuenta.tipo": 1,
                "movimiento.tipo": 1,
                "movimiento.fecha_iso": 1,
                "movimiento.hora": 1,
                "movimiento.canal": 1,
                "monetario.valor": 1,
                "monetario.signo": 1,
                "monetario.saldo_nuevo": 1,
                "monetario.moneda": 1,
                "comercio.nombre": 1,
                "comercio.nombre_normalizado": 1,
                "comercio.categoria": 1,
                "comercio.subcategoria": 1,
                "comercio.descripcion_busqueda": 1,
                "comercio.es_recurrente": 1,
                "score_vector": 1,
            }
        },
    ]

    return list(col.aggregate(pipeline))


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS: DATOS → CONTEXTO PARA CLAUDE
# ═══════════════════════════════════════════════════════════════════════════════

def build_context(docs: list, pregunta: str, cuenta: str) -> str:
    """
    Construye el contexto estructurado que recibe Claude para generar
    la respuesta. Incluye resumen agregado + detalle de movimientos.
    """
    if not docs:
        return f"No se encontraron movimientos relevantes para la cuenta {cuenta}."

    # ── Agregaciones ──────────────────────────────────────────────
    total_monto = 0
    por_categoria = {}
    por_comercio = {}
    por_tipo = {}
    fechas = []

    for d in docs:
        monto = d.get("monetario", {}).get("valor", 0) or 0
        signo = d.get("monetario", {}).get("signo", "")
        categoria = d.get("comercio", {}).get("categoria", "Sin categoría")
        comercio = d.get("comercio", {}).get("nombre", "Desconocido")
        tipo = d.get("movimiento", {}).get("tipo", "")
        fecha = d.get("movimiento", {}).get("fecha_iso")
        score = d.get("score_vector", 0)

        total_monto += monto

        # Por categoría
        if categoria not in por_categoria:
            por_categoria[categoria] = {"count": 0, "total": 0}
        por_categoria[categoria]["count"] += 1
        por_categoria[categoria]["total"] += monto

        # Por comercio
        if comercio not in por_comercio:
            por_comercio[comercio] = {"count": 0, "total": 0}
        por_comercio[comercio]["count"] += 1
        por_comercio[comercio]["total"] += monto

        # Por tipo
        if tipo not in por_tipo:
            por_tipo[tipo] = {"count": 0, "total": 0}
        por_tipo[tipo]["count"] += 1
        por_tipo[tipo]["total"] += monto

        if fecha:
            fechas.append(fecha)

    # ── Formatear contexto ────────────────────────────────────────
    lines = []
    lines.append(f"PREGUNTA DEL CLIENTE: {pregunta}")
    lines.append(f"CUENTA: {cuenta}")
    lines.append(f"MOVIMIENTOS ENCONTRADOS: {len(docs)}")
    lines.append(f"MONTO TOTAL: ${total_monto:,.0f} COP")
    lines.append("")

    if fechas:
        f_min = min(fechas)
        f_max = max(fechas)
        lines.append(f"PERÍODO: {f_min} a {f_max}")
        lines.append("")

    # Top categorías
    lines.append("DESGLOSE POR CATEGORÍA:")
    for cat, data in sorted(por_categoria.items(), key=lambda x: -x[1]["total"]):
        lines.append(f"  - {cat}: {data['count']} movimientos, ${data['total']:,.0f} COP")

    lines.append("")
    lines.append("DESGLOSE POR COMERCIO:")
    top_comercios = sorted(por_comercio.items(), key=lambda x: -x[1]["total"])[:10]
    for com, data in top_comercios:
        lines.append(f"  - {com}: {data['count']} visitas, ${data['total']:,.0f} COP")

    lines.append("")
    lines.append("DETALLE DE MOVIMIENTOS (más relevantes):")
    for i, d in enumerate(docs[:15], 1):
        mov = d.get("movimiento", {})
        mon = d.get("monetario", {})
        com = d.get("comercio", {})
        score = d.get("score_vector", 0)
        lines.append(
            f"  {i}. {com.get('nombre', '?')} | {com.get('categoria', '?')} | "
            f"${mon.get('valor', 0):,.0f} COP | {mov.get('tipo', '?')} | "
            f"{str(mov.get('fecha_iso', ''))[:10]} | "
            f"Canal: {mov.get('canal', {}).get('nombre', '?')} | "
            f"Score: {score:.3f}"
        )

    return "\n".join(lines)


def build_chronological_context(docs: list, pregunta: str, cuenta: str) -> str:
    """Contexto plano sin agrupaciones — para queries cronológicas."""
    if not docs:
        return f"No se encontraron movimientos para la cuenta {cuenta}."

    lines = []
    lines.append(f"PREGUNTA DEL CLIENTE: {pregunta}")
    lines.append(f"CUENTA: {cuenta}")
    lines.append(f"MOVIMIENTOS DEVUELTOS: {len(docs)} (orden: más reciente primero)")
    lines.append("")
    lines.append("LISTA CRONOLÓGICA (NO REORDENAR, NO AGRUPAR):")
    lines.append("")

    for i, d in enumerate(docs, 1):
        mov = d.get("movimiento", {}) or {}
        mon = d.get("monetario", {}) or {}
        com = d.get("comercio", {}) or {}

        fecha_iso = mov.get("fecha_iso")
        fecha_str = ""
        hora_str = mov.get("hora") or ""
        if fecha_iso:
            try:
                if hasattr(fecha_iso, "strftime"):
                    fecha_str = fecha_iso.strftime("%d %b %Y")
                    if not hora_str:
                        hora_str = fecha_iso.strftime("%H:%M:%S")
                else:
                    fecha_str = str(fecha_iso)[:10]
            except Exception:
                fecha_str = str(fecha_iso)[:10]

        valor = mon.get("valor", 0) or 0
        signo_raw = (mon.get("signo") or "").lower()
        signo = "+" if signo_raw in ("c", "credito", "crédito", "+") else "−"
        saldo = mon.get("saldo_nuevo", 0) or 0
        moneda = mon.get("moneda") or "COP"

        tipo = mov.get("tipo") or "Movimiento"
        canal_raw = mov.get("canal")
        if isinstance(canal_raw, dict):
            canal = canal_raw.get("nombre") or canal_raw.get("codigo") or "—"
        else:
            canal = canal_raw or "—"

        comercio = com.get("nombre") or com.get("nombre_normalizado") or "—"
        categoria = com.get("categoria") or ""

        try:
            valor_fmt = f"{valor:,.0f}".replace(",", ".")
            saldo_fmt = f"{saldo:,.0f}".replace(",", ".")
        except Exception:
            valor_fmt = str(valor)
            saldo_fmt = str(saldo)

        lines.append(
            f"{i}. FECHA={fecha_str} HORA={hora_str[:8]} | TIPO={tipo} | "
            f"COMERCIO={comercio} | CATEGORIA={categoria} | "
            f"MONTO={signo}${valor_fmt} {moneda} | SALDO_RESULTANTE=${saldo_fmt} | "
            f"CANAL={canal}"
        )

    lines.append("")
    lines.append("RECUERDA: lista UNO POR UNO en este mismo orden. NO sumarices, NO agrupes.")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS: CLAUDE LLM
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """Eres el asistente financiero inteligente de Bancolombia, integrado en la App Móvil.
Tu rol es responder preguntas sobre los movimientos bancarios del cliente de forma clara, 
precisa y amigable.

REGLAS:
- Responde SIEMPRE en español colombiano, tono profesional pero cercano.
- Usa formato de moneda colombiana: $1.234.567 COP (puntos como separador de miles).
- Si los datos muestran información relevante, sé específico con números y nombres de comercios.
- Si no hay datos suficientes, dilo honestamente: "No encontré movimientos que coincidan con..."
- Mantén las respuestas concisas (máximo 3-4 párrafos).
- NO inventes datos. Solo usa la información proporcionada en el contexto.
- Cuando sea útil, ofrece un resumen con totales y promedios.
- Si el cliente pregunta algo fuera del ámbito financiero, redirige amablemente.
- NO incluyas disclaimers técnicos sobre Vector Search o embeddings. Eres un asistente, no un sistema.
- Puedes usar emojis con moderación para ser más amigable (💰 📊 🏪).

FORMATO DE RESPUESTA:
- Empieza con la respuesta directa a la pregunta.
- Luego agrega detalles relevantes (top comercios, tendencias, etc).
- Si aplica, cierra con un dato útil o sugerencia."""


VOICE_SYSTEM_PROMPT = """Eres el asistente de voz de Bancolombia.
Responde en MÁXIMO 2-3 oraciones cortas y directas. Sin listas, sin bullets, sin markdown.
Usa español colombiano natural, como si hablaras por teléfono.
Di los montos en palabras naturales: "ochocientos noventa y siete mil pesos" en vez de "$897.000 COP".
NO uses emojis, asteriscos, ni formato especial — tu respuesta será leída en voz alta.
Si no hay datos, dilo en una oración.
Ve directo al grano."""


CHRONO_SYSTEM_PROMPT = """Eres el asistente financiero de Bancolombia.
El cliente pidió ver sus ÚLTIMOS MOVIMIENTOS en orden cronológico.

REGLA ABSOLUTA — NO LA ROMPAS:
- DEBES listar los movimientos UNO POR UNO, en el orden EXACTO en que aparecen en los DATOS
  (ya vienen del más reciente al más antiguo).
- NO agrupes por categoría, comercio ni tipo.
- NO sumarices ni mezcles movimientos.
- NO digas "los más grandes son…" — el orden es por FECHA, no por monto.
- NO inventes movimientos que no estén en los datos.

FORMATO OBLIGATORIO:
1. Una frase corta de apertura: "Estos son tus últimos N movimientos:"
2. UNA línea por movimiento, con bullet, así:
   • {fecha corta} {hora} — {tipo} {comercio o descripción} · {±$monto COP} · saldo: ${saldo}
   Ejemplos:
   • 05 may 06:21 — TRF Transferencia recibida · +$3.061.000 COP · saldo: $46.569.000
   • 05 may 06:16 — PSE Gas Natural Vanti · −$2.633.000 COP · saldo: $43.508.000
3. Cierra con UNA línea breve opcional: "Si quieres, puedo filtrar por canal o categoría."

REGLAS DE FORMATO:
- Español colombiano, montos con punto de miles ($1.234.567 COP).
- Signo "−" para débitos (signo D), "+" para créditos (signo C).
- Máximo un emoji al inicio (📋) — nada más.
- NO uses tablas markdown, NO títulos H1/H2, NO párrafos largos.
- NO incluyas totales ni promedios — el cliente quiere ver el orden, no el resumen."""


CHRONO_VOICE_SYSTEM_PROMPT = """Eres el asistente de voz de Bancolombia.
El cliente pidió oír sus últimos movimientos.

REGLAS:
- Lee los 3 movimientos MÁS RECIENTES (los primeros del contexto), en orden.
- Una oración por movimiento, narrativa y natural, como si hablaras por teléfono.
- Usa expresiones temporales relativas cuando ayuden: "hace unos minutos",
  "esta mañana", "ayer en la tarde". Si no es claro, di la fecha y hora corta.
- Di los montos en palabras: "cuatrocientos mil pesos", no "$400.000".
- NO leas más de 3 movimientos. NO listes saldos. NO uses bullets.
- Cierra con una frase corta tipo: "¿Quieres que te lea más movimientos?"
- Total: máximo 3 a 4 oraciones, sin markdown, sin emojis, sin asteriscos."""


def call_claude(context: str, pregunta: str, system_prompt: str = None, max_tokens: int = None) -> str:
    """
    Llama a Claude API con el contexto de movimientos + pregunta.
    Retorna la respuesta en texto.
    """
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY no configurada")

    user_message = f"""Basándote en los siguientes datos de movimientos bancarios del cliente,
responde su pregunta.

--- DATOS ---
{context}
--- FIN DATOS ---

Responde la pregunta del cliente de forma natural y útil."""

    payload = json.dumps({
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens or CLAUDE_MAX_TOKENS,
        "system": system_prompt or SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": user_message}
        ],
    }).encode()

    req = Request(ANTHROPIC_API_URL, data=payload, headers={
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }, method="POST")

    try:
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())

        content = result.get("content", [])
        text_parts = [block["text"] for block in content if block.get("type") == "text"]
        return "\n".join(text_parts)

    except HTTPError as e:
        body = e.read().decode() if e.fp else ""
        log.error(f"Claude API error: {e.code} — {body}")
        raise HTTPException(502, f"Claude API error: {e.code}")
    except Exception as e:
        log.error(f"Claude API no disponible: {e}")
        raise HTTPException(502, f"Claude API no disponible: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 12: CHAT COMPLETO (Vector Search + Claude)  ⭐
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/api/v1/chat")
async def chat(req: ChatRequest):
    """
    🤖 Chat IA — Pregunta en lenguaje natural sobre tus movimientos.

    El diferenciador: combina MongoDB Atlas Vector Search para encontrar
    movimientos relevantes + Claude AI para generar respuestas naturales.

    Ejemplo: "¿Cuánto gasté en restaurantes este mes?"
    """
    t_total = time.time()
    timings = {}
    search_mode = "vector"

    is_chrono = _is_chronological_query(req.pregunta)

    if is_chrono:
        # ── Query cronológica: últimos N movimientos por fecha ────
        search_mode = "chronological"
        t0 = time.time()
        docs = _chronological_search(cuenta=req.cuenta, limit=req.limit)
        timings["chrono_search_ms"] = round((time.time() - t0) * 1000, 1)
        timings["embedding_ms"] = 0
        timings["vector_search_ms"] = 0
    else:
        # ── 1. Embedding de la pregunta ───────────────────────────
        t0 = time.time()
        query_embedding = generate_query_embedding(req.pregunta)
        timings["embedding_ms"] = round((time.time() - t0) * 1000, 1)

        # ── 2. Vector Search ──────────────────────────────────────
        t0 = time.time()
        fecha_desde = datetime.now(timezone.utc) - timedelta(days=req.meses * 30)

        docs = vector_search(
            embedding=query_embedding,
            cuenta=req.cuenta,
            fecha_desde=fecha_desde,
            limit=req.limit,
        )
        timings["vector_search_ms"] = round((time.time() - t0) * 1000, 1)

        recent = _chronological_search(cuenta=req.cuenta, limit=3)
        recent_dates = {str(r.get("movimiento", {}).get("fecha_iso")) for r in recent}
        existing_dates = {str(d.get("movimiento", {}).get("fecha_iso")) for d in docs}
        new_recent = [r for r in recent if str(r.get("movimiento", {}).get("fecha_iso")) not in existing_dates]
        if new_recent:
            docs = new_recent + docs

    # ── 3. Construir contexto ─────────────────────────────────────
    if is_chrono:
        context = build_chronological_context(docs, req.pregunta, req.cuenta)
    else:
        context = build_context(docs, req.pregunta, req.cuenta)

    # ── 4. Claude genera respuesta ────────────────────────────────
    t0 = time.time()
    if req.voice:
        sp = CHRONO_VOICE_SYSTEM_PROMPT if is_chrono else VOICE_SYSTEM_PROMPT
        respuesta = call_claude(context, req.pregunta,
                                system_prompt=sp,
                                max_tokens=300)
    else:
        sp = CHRONO_SYSTEM_PROMPT if is_chrono else SYSTEM_PROMPT
        # En cronológico subimos tokens: 20 movs × ~25 tok ≈ 500
        max_tok = 1200 if is_chrono else CLAUDE_MAX_TOKENS
        respuesta = call_claude(context, req.pregunta,
                                system_prompt=sp,
                                max_tokens=max_tok)
    timings["llm_ms"] = round((time.time() - t0) * 1000, 1)

    # ── 5. Datos de soporte (para el frontend) ────────────────────
    # En modo cronológico devolvemos todos los docs (req.limit), no solo 10
    soporte_slice = req.limit if is_chrono else 10
    movimientos_soporte = []
    for d in docs[:soporte_slice]:
        mov = d.get("movimiento", {})
        mon = d.get("monetario", {})
        com = d.get("comercio", {})
        movimientos_soporte.append({
            "comercio": com.get("nombre"),
            "categoria": com.get("categoria"),
            "monto": mon.get("valor"),
            "signo": mon.get("signo"),
            "tipo": mov.get("tipo"),
            "fecha": mov.get("fecha_iso"),
            "canal": mov.get("canal", {}).get("nombre"),
            "score": round(d.get("score_vector", 0), 4),
        })

    elapsed = round((time.time() - t_total) * 1000, 1)
    timings["total_ms"] = elapsed

    return {
        "pregunta": req.pregunta,
        "respuesta": respuesta,
        "cuenta": req.cuenta,
        "movimientos_encontrados": len(docs),
        "movimientos_soporte": movimientos_soporte,
        "_meta": {
            "timings": timings,
            "models": {
                "embeddings": VOYAGE_MODEL,
                "llm": CLAUDE_MODEL,
            },
            "vector_index": VECTOR_INDEX,
            "periodo_meses": req.meses,
            "search_mode": search_mode,
            "sla_target_ms": 5000,
            "sla_cumple": elapsed < 5000,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 13: VECTOR SEARCH ONLY (Debug / Demo)
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/api/v1/chat/vector-only")
async def chat_vector_only(req: VectorOnlyRequest):
    """
    🔍 Solo Vector Search — retorna movimientos relevantes sin pasar por LLM.
    Útil para debug, validar el índice, y mostrar la búsqueda semántica en bruto.
    """
    t_total = time.time()

    # Embedding
    t0 = time.time()
    query_embedding = generate_query_embedding(req.pregunta)
    embedding_ms = round((time.time() - t0) * 1000, 1)

    # Vector Search
    t0 = time.time()
    docs = vector_search(
        embedding=query_embedding,
        cuenta=req.cuenta,
        limit=req.limit,
    )
    vs_ms = round((time.time() - t0) * 1000, 1)

    # Formatear resultados
    resultados = []
    for d in docs:
        mov = d.get("movimiento", {})
        mon = d.get("monetario", {})
        com = d.get("comercio", {})
        resultados.append({
            "comercio": com.get("nombre"),
            "categoria": com.get("categoria"),
            "subcategoria": com.get("subcategoria"),
            "descripcion_busqueda": com.get("descripcion_busqueda"),
            "es_recurrente": com.get("es_recurrente"),
            "monto": mon.get("valor"),
            "signo": mon.get("signo"),
            "saldo": mon.get("saldo_nuevo"),
            "tipo": mov.get("tipo"),
            "fecha": mov.get("fecha_iso"),
            "hora": mov.get("hora"),
            "canal": mov.get("canal", {}).get("nombre"),
            "cuenta": d.get("cuenta", {}).get("numero"),
            "score_vector": round(d.get("score_vector", 0), 4),
        })

    elapsed = round((time.time() - t_total) * 1000, 1)

    return {
        "pregunta": req.pregunta,
        "cuenta_filtro": req.cuenta,
        "resultados": len(resultados),
        "movimientos": resultados,
        "_meta": {
            "timings": {
                "embedding_ms": embedding_ms,
                "vector_search_ms": vs_ms,
                "total_ms": elapsed,
            },
            "model": VOYAGE_MODEL,
            "vector_index": VECTOR_INDEX,
            "num_candidates": NUM_CANDIDATES,
            "nota": "Sin LLM — resultados crudos de Vector Search",
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 14: SUGERENCIAS DE PREGUNTAS (Demo)
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/api/v1/chat/sugerencias")
async def sugerencias():
    """
    💡 Preguntas sugeridas para el demo — el frontend las muestra como chips
    clickeables en el tab de Chat IA.
    """
    return {
        "sugerencias": [
            {
                "texto": "¿Cuánto gasté en restaurantes este mes?",
                "categoria": "Gastos por categoría",
                "icono": "🍽️",
            },
            {
                "texto": "¿Cuánto llevo pagando de suscripciones?",
                "categoria": "Pagos recurrentes",
                "icono": "🔄",
            },
            {
                "texto": "¿Cuáles son mis comercios más frecuentes?",
                "categoria": "Análisis de hábitos",
                "icono": "🏪",
            },
            {
                "texto": "¿Cuánto gasto en transporte al mes?",
                "categoria": "Gastos por categoría",
                "icono": "🚗",
            },
            {
                "texto": "¿Mis gastos en delivery subieron este mes?",
                "categoria": "Tendencias",
                "icono": "📈",
            },
            {
                "texto": "¿Cuáles fueron mis compras más grandes esta semana?",
                "categoria": "Movimientos destacados",
                "icono": "💰",
            },
            {
                "texto": "¿Cuánto gasté en supermercados?",
                "categoria": "Gastos por categoría",
                "icono": "🛒",
            },
            {
                "texto": "¿Tengo pagos de servicios públicos pendientes?",
                "categoria": "Servicios",
                "icono": "💡",
            },
        ],
        "nota": "Selecciona una pregunta o escribe la tuya",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  STATUS CHECK (al importar)
# ═══════════════════════════════════════════════════════════════════════════════

def check_config():
    """Verifica configuración al arrancar."""
    status = []
    if VOYAGE_API_KEY:
        status.append("✅ Voyage AI configurado")
    else:
        status.append("⚠️  VOYAGE_API_KEY no configurada — embeddings no disponibles")

    if ANTHROPIC_API_KEY:
        status.append("✅ Claude API configurado")
    else:
        status.append("⚠️  ANTHROPIC_API_KEY no configurada — chat completo no disponible")

    for s in status:
        print(f"   {s}")

    return bool(VOYAGE_API_KEY and ANTHROPIC_API_KEY)


# Print status on import
print("🤖 Módulo Chat IA cargado")
check_config()
