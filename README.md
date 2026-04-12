# Talk to Data

Self-service intelligence platform for the NatWest Group Code for Purpose Hackathon. Users upload one or more datasets, build a connected analytics workspace, and ask everyday-language questions to receive aggregate-only narratives, charts, and transparent source notes.

## Architecture

- `frontend`: React + Vite connected-workspace dashboard with multi-file upload, charts, joins, and chat.
- `backend`: Node.js + Express orchestration API with `multer`, MongoDB models, upload forwarding, query history, and PII response scrubbing.
- `ai_engine`: FastAPI + Pandas/OpenPyXL/SQLite service for secure ingestion, relationship detection, merged workspace previews, and aggregate-only analysis.
- `docker-compose.yml`: MongoDB helper for local development.

## Compliance Notes

- Source files include the ISA / Apache-2.0 header. Python and HTML use language-safe comment wrappers so the services remain executable.
- Uploaded files are never passed directly to the UI. Node stores incoming uploads only long enough to forward them to the AI service, then removes the temporary copy.
- The AI service stores datasets in `ai_engine/storage/datasets` and reads SQLite files through read-only connections.
- The analysis layer avoids detected PII columns and returns grouped aggregate outputs only.
- Secrets are read from environment variables. Use the `.env.example` files as templates.

## Run Locally

1. Start MongoDB:

   ```bash
   docker compose up -d mongo
   ```

2. Start the AI engine:

   ```bash
   cd ai_engine
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   copy .env.example .env
   uvicorn app.main:app --reload --port 8000
   ```

3. Start the backend:

   ```bash
   cd backend
   npm install
   copy .env.example .env
   npm run dev
   ```

4. Start the frontend:

   ```bash
   cd frontend
   npm install
   copy .env.example .env
   npm run dev
   ```

The frontend runs on Vite's local URL, usually `http://localhost:5173`.

## Current Workflow

1. Upload one or more CSV / JSON / Excel / SQLite files.
2. The AI engine infers schema and, for multiple flat files, detects likely relationships and builds a merged workspace.
3. The backend stores workspace metadata, generated metric definitions, relationships, and query history in MongoDB.
4. The frontend shows upload-time dashboard previews, detected joins, recent prompts, and chat-driven aggregate insights.

## Deploy

### Vercel + Render

- Deploy `frontend` to Vercel with root directory `frontend`.
- Set `VITE_API_BASE_URL` to your backend public URL, for example:

  ```bash
  https://your-backend.onrender.com/api
  ```

- Deploy `ai_engine` and `backend` to Render as Docker web services.
- Use MongoDB Atlas or another managed MongoDB and point the backend `MONGODB_URI` there.

### Docker Compose

For a VPS or local Docker host, copy the production env template and run:

```bash
copy .env.production.example .env.production
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
```

Set `CORS_ORIGIN` in `.env.production` to your Vercel domain.

## API Contract

The backend returns this shape for chat responses:

```json
{
  "query_status": "success",
  "insight_narrative": "Revenue decreased by 11% in Feb. The biggest contributor was the South region.",
  "analytics_sidebar": {
    "chart_type": "bar",
    "data_points": [{ "label": "North", "value": 40 }],
    "outliers_noted": ["South region drop"]
  },
  "transparency": {
    "data_sources": ["Uploaded File: sales_data.csv", "Columns used: Region, Revenue"],
    "metric_definition_used": "Dynamic schema extraction"
  }
}
```

The backend also supports connected workspace upload through:

```json
POST /api/upload-multiple
```

which returns workspace metadata including `relationships`, `source_files`, `dashboard_preview`, and `upload_insight`.
