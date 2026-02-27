# SH-APK-API

**Health Connect Ingestion API (v3)**

A minimal, robust FastAPI service that receives raw Health Connect JSON from Android devices, stores it in PostgreSQL, and exposes query endpoints for downstream analysis.

Built for personal health data pipelines — own your data, query it freely.

---

## What It Does

1. **Ingests** raw Health Connect JSON from your Android device (via custom bridge app)
2. **Stores** it in PostgreSQL with full fidelity — no data loss, no forced schema
3. **Notifies** via Telegram on every sync (optional)
4. **Serves** query endpoints for building dashboards, analysis, or agentic systems

---

## API Endpoints

### Ingestion (POST)

```http
POST /v1/ingest/daily
POST /v1/ingest/intraday
X-API-Key: <your-key>
Content-Type: application/json

{
  "date": "2026-02-26",
  "source": {
    "device_id": "uuid",
    "collected_at": "2026-02-26T23:30:00Z",
    "source_app": "health_connect",
    "schema_version": 3
  },
  "raw_json": "{...full Health Connect JSON...}"
}
```

- **`/daily`** — Canonical daily record (one per day, multiple syncs create history)
- **`/intraday`** — Audit log of every sync (append-only, for time-series analysis)
- **`/debug`** — Logs full payload for development

### Query (GET)

```http
GET /health                              → Database health check
GET /v1/records/latest                   → Most recent daily record
GET /v1/records/2026-02-26               → Specific date
GET /v1/records?start_date=...&end_date=...  → Date range
GET /v1/dates                            → All available dates with metadata
GET /v1/logs?date=2026-02-26&limit=10    → Intraday audit logs
```

All endpoints require `X-API-Key` header.

---

## Data Storage

### Tables

**`health_connect_daily`** — Canonical records (one row per unique date)
- `date`, `device_id`, `raw_json` (JSONB), `received_at`, `schema_version`
- Multiple syncs per day = multiple rows (full history preserved)

**`health_connect_intraday_logs`** — Audit trail
- Every sync creates a row
- For debugging sync issues, building time-series visualizations

### Raw JSON Structure

The `raw_json` field contains full Health Connect data:

```json
{
  "StepsRecord": [...],
  "WeightRecord": [...],
  "HeartRateRecord": [...],
  "ExerciseSessionRecord": [...],
  "SleepSessionRecord": [...],
  "NutritionRecord": [...],
  "DistanceRecord": [...],
  "BodyFatRecord": [...],
  ...
}
```

Query endpoints return this parsed back into JSON under the `data` key.

---

## Architecture

```
Android Device
  └── Health Connect (Samsung Health, Google Fit, etc.)
        └── Custom Bridge App
              └── POST /v1/ingest/daily
                    └── FastAPI
                          ├── PostgreSQL (raw storage)
                          └── Telegram (sync notification)
```

### Key Design Decisions

- **Raw storage** — No forced schema. Health Connect v4, v5, whatever — we handle it.
- **Source-aware** — Knows Samsung Health vs Google Fit, prioritizes前者 for step counts
- **Hash deduplication** — Bridge app skips unchanged data to save battery/bandwidth
- **Double storage** — Daily table for current state, intraday logs for audit trail

---

## Deployment

Hosted on Railway. Deploy via CLI:

```bash
railway up --service SH-APK-API
```

Environment variables:
```
DATABASE_URL=postgresql+asyncpg://...
API_KEY=your-secret-key
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

---

## Client (Android)

See the companion Android app in `../health-connect-bridge/` (separate repo).

Features:
- Polls Health Connect API every hour
- Hash-based deduplication (skips unchanged data)
- Background sync with battery optimization
- Local hash cache for backfill support

---

## Future: ContextKernel Integration

This API feeds into a broader personal context system:

- **Ingest** → sh-apk-api (this service)
- **Synthesize** → ContextKernel (aggregation, goal tracking, card generation)
- **Consume** → Dashboards, agents, insights

Raw endpoints here enable any downstream analysis without lock-in.

---

## License

MIT — Personal use. Data stays yours.
