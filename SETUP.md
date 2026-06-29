# 🚀 ODL Demo — Guía de arranque

> **Para levantar esta demo con Claude Code:** abre esta carpeta en Claude Code y
> escribe: *"Sigue el SETUP.md y levanta la demo"*. Claude Code ejecutará los pasos
> de abajo por ti. También puedes seguirlos a mano.

---

## ¿Qué es esto?

Una demo de **Operational Data Layer (ODL)** sobre MongoDB con tres piezas:

| Pieza | Qué hace | Puerto |
|-------|----------|--------|
| **App** (`api_canales.py`) | API REST + frontend de la demo (ruta `/app`) | 8788 |
| **Dashboard** (`api_dashboard.py`) | Dashboard en vivo con métricas del ODL | 8787 |
| **Simulador** (`iseries-simulator/`) | Genera transacciones simuladas (opcional, avanzado) | — |

La **app y el dashboard funcionan con los datos ya poblados en MongoDB** — no
necesitas correr el simulador para ver la demo completa.

---

## Requisitos

- **Python 3.11+**
- Acceso a un **cluster MongoDB** con los datos de la demo (te lo entrega quien
  comparte la demo: un usuario y las bases `bancolombia_odl` e `iseries_sim`).
- *(Opcional)* Llaves propias de **Anthropic** (chat IA), **Voyage** (búsqueda
  vectorial) y **OpenAI** (voz).
- *(Opcional)* `cloudflared` si quieres exponer la demo en una URL pública.

---

## Pasos

### 1. Configura tus credenciales

```bash
cp .env.example .env
```

Edita `.env` y completa **como mínimo** `MONGODB_ODL_URI` (la cadena de conexión a
Mongo). Las llaves de IA son opcionales: sin ellas la app y el dashboard funcionan
igual; solo se deshabilitan el chat y la voz.

> ⚠️ **Nunca subas `.env` a git.** Ya está en `.gitignore`.

### 2. Levanta app + dashboard

```bash
./start.sh
```

La primera vez crea un entorno virtual e instala dependencias automáticamente.
Cuando termine verás:

- **App:**       http://localhost:8788/app
- **API docs:**  http://localhost:8788/docs
- **Dashboard:** http://localhost:8787/

> El dashboard tarda **~30–60 s** en el primer cálculo (procesa millones de
> documentos). Es normal; los refrescos siguientes son instantáneos.

Para detener todo:

```bash
./start.sh stop
```

### 3. *(Opcional)* Exponer en una URL pública

```bash
cloudflared tunnel --url http://localhost:8788   # app/API
cloudflared tunnel --url http://localhost:8787   # dashboard
```

Cada comando imprime una URL `https://….trycloudflare.com`. Son **efímeras**:
cambian cada vez que reinicias el túnel.

---

## Simulador en vivo (opcional, avanzado)

El simulador (`iseries-simulator/`) genera transacciones y las publica a **Kafka**,
desde donde un procesador las escribe a MongoDB. **No es necesario para la demo
visual** (la app/dashboard ya leen datos poblados).

Si quieres correrlo, necesitas proveer **tu propia infraestructura Kafka** y un
consumidor que escriba a Mongo. Configura `KAFKA_*` en tu `.env` y luego:

```bash
cd iseries-simulator
pip install -r requirements.txt
python main.py --scenario demo-quick
```

---

## Solución de problemas

| Síntoma | Causa probable | Solución |
|---------|----------------|----------|
| `MONGODB_ODL_URI vacío` | No completaste `.env` | Edita `.env` y pon la cadena de Mongo |
| Dashboard no carga / `HTTP 000` | Aún en el primer cálculo | Espera ~60 s y reintenta |
| El chat responde error 500 | Falta `ANTHROPIC_API_KEY` | Es opcional; agrégala al `.env` si la quieres |
| `command not found: lsof` | Falta `lsof` | `sudo apt-get install lsof` (Linux) |

---

## Seguridad

Este repo **no contiene ninguna credencial**. Todas se leen de tu `.env` local,
que nunca se versiona. Usa tus propias llaves de IA y tu propio usuario de Mongo.
