# ODL Demo — App + Dashboard + Simulador iSeries

Demo de un **Operational Data Layer (ODL)** sobre MongoDB Atlas: muestra cómo un
ODL ensambla, sirve y enriquece movimientos bancarios en tiempo real.

> **¿La vas a levantar con Claude Code?** Abre esta carpeta en Claude Code y
> escribe: *"Sigue el README y levanta la demo"*. Ejecutará los pasos por ti.

---

## 📦 Qué incluye este repo

Las **tres piezas viven en este mismo repositorio**:

| Pieza | Carpeta / archivo | Puerto | ¿Necesaria para la demo visual? |
|-------|-------------------|--------|--------------------------------|
| **App** (API + frontend) | `api_canales.py` | 8788 | ✅ Sí |
| **Dashboard** en vivo | `api_dashboard.py` | 8787 | ✅ Sí |
| **Simulador iSeries** | `iseries-simulator/` | — | ⚙️ Opcional (genera carga en vivo) |

La **app y el dashboard funcionan con los datos ya poblados en MongoDB** — no
necesitas correr el simulador para ver la demo completa.

---

## ✅ Requisitos

- **Python 3.11+**
- Acceso a un **cluster MongoDB** con los datos de la demo (te entregan un usuario
  con acceso a las bases `bancolombia_odl` e `iseries_sim`).
- *(Opcional)* Llaves propias de **Anthropic** (chat IA), **Voyage** (búsqueda
  vectorial) y **OpenAI** (voz).
- *(Opcional)* `cloudflared` para exponer la demo en una URL pública.

---

## 🚀 Arranque rápido (app + dashboard)

```bash
# 1. Configura tus credenciales
cp .env.example .env
#    edita .env y completa MÍNIMO: MONGODB_ODL_URI

# 2. Levanta todo (crea venv e instala dependencias la primera vez)
./start.sh
```

Cuando termine verás:

- **App:**       http://localhost:8788/app
- **API docs:**  http://localhost:8788/docs
- **Dashboard:** http://localhost:8787/

> ⏳ El dashboard tarda **~30–60 s** en el primer cálculo (procesa millones de
> documentos). Es normal; los refrescos siguientes son instantáneos.

Para detener todo:

```bash
./start.sh stop
```

Las llaves de IA son **opcionales**: sin ellas la app y el dashboard funcionan
igual; solo se deshabilitan el chat y la voz.

---

## ⚙️ Simulador iSeries (opcional, avanzado)

El simulador (`iseries-simulator/`) genera transacciones bancarias simuladas y las
publica a **Kafka**, desde donde un procesador las escribe a MongoDB. Reproduce el
flujo "legacy iSeries → ODL".

> **No es necesario para la demo visual.** La app/dashboard ya leen datos poblados.
> Córrelo solo si quieres mostrar generación de carga **en vivo**, y necesitas
> proveer **tu propia infraestructura Kafka** (configura `KAFKA_*` en el `.env`).

```bash
cd iseries-simulator
pip install -r requirements.txt

# 1. (Una vez) Poblar el pool de cuentas en Mongo
python setup/setup_maestro_cuentas.py --cuentas 100000     # 100k para pruebas
#   python setup/setup_maestro_cuentas.py --cuentas 1000000 # 1M (escala completa)

# 2. Generar transacciones — escenarios predefinidos
python main.py --scenario demo-quick      # demo corta
python main.py --scenario burst-test      # ráfaga de carga

# o configuración manual:
python main.py --tps 500 --duration 60    # 500 transacciones/s durante 60 s
```

---

## 🌐 Exponer en una URL pública (opcional)

```bash
cloudflared tunnel --url http://localhost:8788   # app / API
cloudflared tunnel --url http://localhost:8787   # dashboard
```

Cada comando imprime una URL `https://….trycloudflare.com`. Son **efímeras**:
cambian cada vez que reinicias el túnel.

---

## 🗂️ Estructura del repo

```
config.py                  Configuración central (lee TODO de variables de entorno)
.env.example               Plantilla de credenciales → copiar a .env
start.sh                   Arranca/detiene app + dashboard
requirements.txt           Dependencias (app + simulador)
api_canales.py             App: API REST + frontend (/app) + Swagger (/docs)
api_dashboard.py           Dashboard en vivo (métricas del ODL)
api_chat.py                Chat IA (Vector Search + Claude) — router opcional
api_simulate.py            Endpoints read-your-writes — router opcional
*.html                     Frontends estáticos servidos por la app
iseries-simulator/         Simulador iSeries (generador → Kafka)
CLAUDE.md                  Contexto del repo para Claude Code
```

---

## 🔒 Seguridad

Este repo **no contiene ninguna credencial**. Todas se leen de tu `.env` local
(que está en `.gitignore` y nunca se versiona). Usa tus propias llaves de IA y tu
propio usuario de Mongo.

---

## 🧰 Solución de problemas

| Síntoma | Causa probable | Solución |
|---------|----------------|----------|
| `MONGODB_ODL_URI vacío` | No completaste `.env` | Edita `.env` y pon la cadena de Mongo |
| Dashboard no carga / `HTTP 000` | Aún en el primer cálculo | Espera ~60 s y reintenta |
| El chat responde error 500 | Falta `ANTHROPIC_API_KEY` | Es opcional; agrégala al `.env` si la quieres |
| `command not found: lsof` | Falta `lsof` | `sudo apt-get install lsof` (Linux) |
