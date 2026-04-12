# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True
    isSensitive: bool = False


class TableSchema(BaseModel):
    name: str
    columns: list[ColumnSchema] = Field(default_factory=list)
    rowCount: int = 0


class SchemaPayload(BaseModel):
    columns: list[ColumnSchema] = Field(default_factory=list)
    tables: list[TableSchema] = Field(default_factory=list)


class IngestResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dataset_id: str
    file_type: str
    row_count: int
    schema_payload: SchemaPayload = Field(alias="schema")
    pii_columns: list[str] = Field(default_factory=list)
    storage_mode: str
    dashboard_preview: dict[str, Any] = Field(default_factory=dict)
    relationships: list[dict[str, Any]] = Field(default_factory=list)
    source_files: list[dict[str, Any]] = Field(default_factory=list)
    upload_insight: str = ""


class ChatRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dataset_id: str
    prompt: str
    schema_payload: dict[str, Any] | None = Field(default=None, alias="schema")
    metric_definitions: list[dict[str, Any]] = Field(default_factory=list)
    source_label: str | None = None


class AnalyticsSidebar(BaseModel):
    chart_type: str
    data_points: list[dict[str, Any]] = Field(default_factory=list)
    outliers_noted: list[str] = Field(default_factory=list)


class Transparency(BaseModel):
    data_sources: list[str] = Field(default_factory=list)
    metric_definition_used: str


class ChatResponse(BaseModel):
    query_status: str
    insight_narrative: str
    analytics_sidebar: AnalyticsSidebar
    transparency: Transparency
