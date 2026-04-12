# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from .analysis import analyze_chat, dashboard_preview_for_dataframe
from .ingestion import dataframe_schema, load_sqlite_table, read_dataframe, save_upload, sqlite_schema
from .models import ChatRequest, ChatResponse, IngestResponse, SchemaPayload
from .relational import build_source_table, merge_source_tables
from .settings import get_settings


app = FastAPI(title="Talk to Data AI Engine", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_credentials=False,
    allow_headers=["*"],
    allow_methods=["GET", "POST"],
    allow_origins=["*"],
)


def _fallback_preview(row_count: int, schema: SchemaPayload, source_label: str) -> dict:
    return {
        "bar_chart": {"chart_type": "table", "data_points": [{"label": "Rows loaded", "value": row_count}], "outliers_noted": []},
        "headline_metrics": [
            {"label": "Rows", "value": row_count},
            {"label": "Columns", "value": len(schema.columns)},
            {"label": "Tables", "value": len(schema.tables)},
        ],
        "insight": f"I processed {row_count:,} rows and prepared a safe dataset summary.",
        "pie_chart": {"chart_type": "pie", "data_points": [], "outliers_noted": []},
        "transparency": {
            "data_sources": [source_label],
            "metric_definition_used": "upload-time preview",
        },
    }


def _pii_columns_from_schema(schema: SchemaPayload) -> list[str]:
    pii_columns = [column.name for column in schema.columns if column.isSensitive]
    pii_columns.extend(
        column.name
        for table in schema.tables
        for column in table.columns
        if column.isSensitive
    )
    return sorted(set(pii_columns))


@app.get("/health")
def health() -> dict[str, str]:
    return {"service": "talk-to-data-ai-engine", "status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
async def ingest(file: UploadFile = File(...)) -> IngestResponse:
    settings = get_settings()

    try:
        dataset_id, path, extension = await save_upload(file, settings)
        preview_dataframe = None

        if extension in {".sqlite", ".db"}:
            schema, row_count = sqlite_schema(path)
            storage_mode = "read_only_sqlite"
            try:
                preview_dataframe, _ = load_sqlite_table(path, settings.max_analysis_rows)
            except ValueError:
                preview_dataframe = None
        else:
            dataframe = read_dataframe(path)
            schema = SchemaPayload(columns=dataframe_schema(dataframe), tables=[])
            row_count = len(dataframe)
            storage_mode = "sandboxed_dataframe"
            preview_dataframe = dataframe

        source_label = f"Uploaded File: {file.filename}"
        dashboard_preview = (
            dashboard_preview_for_dataframe(preview_dataframe, source_label)
            if preview_dataframe is not None
            else _fallback_preview(row_count, schema, source_label)
        )

        return IngestResponse(
            dataset_id=dataset_id,
            file_type=extension.lstrip("."),
            pii_columns=_pii_columns_from_schema(schema),
            row_count=row_count,
            schema=schema,
            storage_mode=storage_mode,
            dashboard_preview=dashboard_preview,
            relationships=[],
            source_files=[
                {
                    "columns": [column.model_dump() for column in schema.columns],
                    "file_type": extension.lstrip("."),
                    "filename": file.filename,
                    "name": Path(file.filename or "dataset").stem,
                    "row_count": row_count,
                    "status": "linked",
                }
            ],
            upload_insight=dashboard_preview.get("insight", ""),
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=422, detail="The dataset could not be parsed safely.") from error


@app.post("/ingest-multiple", response_model=IngestResponse)
async def ingest_multiple(files: list[UploadFile] = File(...)) -> IngestResponse:
    settings = get_settings()
    if not files:
        raise HTTPException(status_code=400, detail="Upload at least one dataset to continue.")

    temp_paths: list[Path] = []
    try:
        source_entries = []
        for index, upload in enumerate(files):
            _, path, extension = await save_upload(upload, settings)
            temp_paths.append(path)
            if extension not in {".csv", ".json", ".xlsx"}:
                raise ValueError("Multiple upload supports CSV, JSON, or Excel files.")

            dataframe = read_dataframe(path, settings.max_analysis_rows)
            source_entries.append(build_source_table(dataframe, upload.filename or f"dataset_{index + 1}{extension}", extension.lstrip("."), index))

        merged_dataframe, relationships, source_files, unlinked = merge_source_tables(source_entries)
        dataset_id = str(uuid4())
        merged_path = settings.storage_dir / f"{dataset_id}.csv"
        merged_dataframe.to_csv(merged_path, index=False)

        schema = SchemaPayload(columns=dataframe_schema(merged_dataframe), tables=[])
        source_label = "Uploaded Workspace: " + ", ".join(source["filename"] for source in source_files[:4])
        dashboard_preview = dashboard_preview_for_dataframe(merged_dataframe, source_label)
        relationship_note = (
            f" I detected {len(relationships)} relationship{'s' if len(relationships) != 1 else ''}"
            if relationships else " I could not detect a strong join, so analytics stay limited to the uploaded union."
        )
        if unlinked:
            relationship_note += f" Unlinked files: {', '.join(unlinked[:3])}."

        return IngestResponse(
            dataset_id=dataset_id,
            file_type="multi",
            pii_columns=_pii_columns_from_schema(schema),
            row_count=len(merged_dataframe),
            schema=schema,
            storage_mode="merged_dataframe",
            dashboard_preview=dashboard_preview,
            relationships=relationships,
            source_files=source_files,
            upload_insight=(dashboard_preview.get("insight", "") + relationship_note).strip(),
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=422, detail="The datasets could not be merged safely.") from error
    finally:
        for path in temp_paths:
            path.unlink(missing_ok=True)


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    try:
        return analyze_chat(request)
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=422, detail="The question could not be answered safely.") from error
