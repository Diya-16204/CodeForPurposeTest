# Talk to Data

Self-service intelligence platform for the NatWest Group Code for Purpose Hackathon. Users upload a dataset, ask everyday-language questions, and receive aggregate-only narratives, charts, and transparent source notes.

## Architecture

- `frontend`: React + Vite upload and 3-pane dashboard.
- `backend`: Node.js + Express orchestration API with `multer`, MongoDB models, upload forwarding, query history, and PII response scrubbing.
- `ai_engine`: FastAPI + Pandas/OpenPyXL/SQLite service for secure ingestion and aggregate-only analysis.
- `docker-compose.yml`: MongoDB helper for local development.

## Compliance Notes

- Source files include the ISA / Apache-2.0 header. Python and HTML use language-safe comment wrappers so the services remain executable.
- Uploaded files are never passed directly to the UI. Node stores the incoming upload only long enough to forward it to the AI service, then removes the temporary copy.
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

## Deployed Safely

Deployed this repository as four separate pieces:

1. `frontend` on Vercel
2. `backend` as a Node web service on Render
3. `ai_engine` as a Python web service on Render
4. MongoDB Atlas for the database

Deployment order:

1. Created MongoDB Atlas and copy the connection string.
2. Deployed `ai_engine` and confirm `/health` returns `ok`.
3. Deployed `backend` with the Atlas URI and AI engine URL.
4. Deployed `frontend` with the backend `/api` URL.
5. Updated backend `CORS_ORIGIN` to the final frontend URL and redeployed once.

Minimum production environment variables:

- `frontend`
  - `VITE_API_BASE_URL=https://<your-backend-domain>/api`
- `backend`
  - `PORT=4000`
  - `NODE_ENV=production`
  - `MONGODB_URI=<your-atlas-connection-string>`
  - `AI_ENGINE_URL=https://<your-ai-engine-domain>`
  - `UPLOAD_DIR=uploads/tmp`
  - `MAX_UPLOAD_MB=50`
  - `CORS_ORIGIN=https://<your-frontend-domain>`
- `ai_engine`
  - `AI_STORAGE_DIR=storage/datasets`
  - `MAX_ANALYSIS_ROWS=10000`
  - `LLM_PROVIDER=none`
  - `GEMINI_API_KEY=`
  - `GEMINI_MODEL=`
  - `GROQ_API_KEY=`
  - `GROQ_MODEL=llama-3.1-8b-instant`

Production start commands:

- `backend`
  - Build: `npm install`
  - Start: `npm start`
- `ai_engine`
  - Build: `pip install -r requirements.txt`
  - Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- `frontend`
  - Build: `npm install && npm run build`
  - Publish directory: `dist`

## Run On Live Deployment

Use this order every time you test the deployed app:

1. Open the backend health URL:

   ```bash
   https://<your-backend-domain>/health
   ```

   Wait until it returns a JSON response with `status: "ok"`.

2. Open the AI engine health URL:

   ```bash
   https://<your-ai-engine-domain>/health
   ```

   Wait until it returns a JSON response with `status: "ok"`.

3. If either service is still waking up or restarting, wait and refresh until both health checks return `ok`.

4. After both services are healthy, open the deployed frontend URL:

   ```bash
   https://<your-frontend-domain>
   ```

5. Upload sample data and click `Build workspace`.

6. Analyze the data then.

Example with deployed URLs:

```bash
Backend health: https://codeforpurposetest.onrender.com/health
AI health: https://code-for-purpose-ai-2.onrender.com/health
Frontend: https://code-for-purpose-test.vercel.app
```

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
