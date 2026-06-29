# CLAUDE.md — Contexto del repo para Claude Code

Demo de **Operational Data Layer (ODL)** sobre MongoDB Atlas. Muestra cómo un ODL
ensambla, sirve y enriquece movimientos bancarios en tiempo real.

## Cómo levantar

```bash
cp .env.example .env     # completar MONGODB_ODL_URI (mínimo)
./start.sh               # levanta API:8788 + Dashboard:8787
./start.sh stop          # detiene ambos
```

Guía completa para el usuario: **`README.md`**.

## Arquitectura

| Componente | Archivo | Puerto | Notas |
|------------|---------|--------|-------|
| API + frontend | `api_canales.py` | 8788 | Sirve la app en `/app`, dashboard live en otra ruta, Swagger en `/docs`. Monta routers de `api_simulate.py` y `api_chat.py` si están presentes. |
| Dashboard live | `api_dashboard.py` | 8787 | Agrega métricas del ODL; refresca cada 3 s con `$sample`. Primer cálculo ~30–60 s. |
| Chat IA | `api_chat.py` | — | Router montado en la API. Vector Search (Voyage) + Claude (Anthropic). |
| Simulación read-your-writes | `api_simulate.py` | — | Router montado en la API. |
| Simulador iSeries | `iseries-simulator/` | — | Genera transacciones → Kafka. Opcional, requiere infra Kafka propia. |

## Configuración

- **Toda** la config y las credenciales se leen de variables de entorno vía
  `config.py` (app) y `iseries-simulator/config/settings.py` (simulador).
- **No hay secretos hardcodeados.** Nunca los agregues: usa `.env` (gitignored).
- Variable obligatoria: `MONGODB_ODL_URI`. Resto opcionales (degradan con gracia).

## Datos

MongoDB, base `bancolombia_odl`, colección `movimientos` (millones de docs ya
poblados). La app y el dashboard leen de ahí; no requieren el simulador ni Kafka.

## Convenciones

- Frontend = HTML estático servido por la API (`bancolombia_odl_demo.html`,
  `bancolombia-odl-dashboard-live.html`). No hay build step.
- Python stdlib + FastAPI/uvicorn/pymongo. El chat usa `urllib`, no `requests`.
- Logs: `logs_api.log`, `logs_dashboard.log` (gitignored).
