# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
from pathlib import Path
import re
import sqlite3
from uuid import uuid4

import pandas as pd
from fastapi import UploadFile

from .models import ColumnSchema, SchemaPayload, TableSchema
from .security import looks_sensitive_column
from .settings import Settings


ALLOWED_EXTENSIONS = {".csv", ".json", ".xlsx", ".sqlite", ".db"}
SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


async def save_upload(upload: UploadFile, settings: Settings) -> tuple[str, Path, str]:
    extension = Path(upload.filename or "").suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise ValueError("Unsupported dataset type. Upload CSV, JSON, Excel, SQLite, or DB files only.")

    dataset_id = str(uuid4())
    destination = settings.storage_dir / f"{dataset_id}{extension}"

    with destination.open("wb") as output:
        while chunk := await upload.read(1024 * 1024):
            output.write(chunk)

    return dataset_id, destination, extension


def resolve_dataset_file(dataset_id: str, settings: Settings) -> Path:
    matches = list(settings.storage_dir.glob(f"{dataset_id}.*"))
    for candidate in matches:
        if candidate.suffix.lower() in ALLOWED_EXTENSIONS and candidate.is_file():
            return candidate
    raise FileNotFoundError("Dataset not found. Upload the file again before asking questions.")


def read_dataframe(path: Path, max_rows: int | None = None) -> pd.DataFrame:
    extension = path.suffix.lower()

    if extension == ".csv":
        return pd.read_csv(path, nrows=max_rows)

    if extension == ".json":
        dataframe = pd.read_json(path)
        return dataframe.head(max_rows) if max_rows else dataframe

    if extension == ".xlsx":
        return pd.read_excel(path, engine="openpyxl", nrows=max_rows)

    raise ValueError("This file type is not a Pandas dataframe source.")


def dataframe_schema(dataframe: pd.DataFrame) -> list[ColumnSchema]:
    return [
        ColumnSchema(
            isSensitive=looks_sensitive_column(column),
            name=str(column),
            nullable=bool(dataframe[column].isna().any()),
            type=str(dataframe[column].dtype),
        )
        for column in dataframe.columns
    ]


def quote_identifier(identifier: str) -> str:
    if SQL_IDENTIFIER_PATTERN.fullmatch(identifier):
        return f'"{identifier}"'
    return '"' + identifier.replace('"', '""') + '"'


def open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def sqlite_schema(path: Path) -> tuple[SchemaPayload, int]:
    tables: list[TableSchema] = []
    total_rows = 0

    with open_sqlite_readonly(path) as connection:
        table_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()

        for (table_name,) in table_rows:
            quoted_table = quote_identifier(table_name)
            column_rows = connection.execute(f"PRAGMA table_info({quoted_table})").fetchall()
            columns = [
                ColumnSchema(
                    isSensitive=looks_sensitive_column(column_name),
                    name=str(column_name),
                    nullable=not bool(notnull),
                    type=str(column_type or "unknown"),
                )
                for _, column_name, column_type, notnull, _, _ in column_rows
            ]

            row_count = connection.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
            total_rows += int(row_count)
            tables.append(TableSchema(columns=columns, name=str(table_name), rowCount=int(row_count)))

    columns = tables[0].columns if tables else []
    return SchemaPayload(columns=columns, tables=tables), total_rows


def load_sqlite_table(path: Path, max_rows: int, prompt: str = "") -> tuple[pd.DataFrame, str]:
    with open_sqlite_readonly(path) as connection:
        table_rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        table_names = [str(row[0]) for row in table_rows]

        if not table_names:
            raise ValueError("The SQLite file does not contain readable user tables.")

        prompt_lower = prompt.lower()
        selected_table = next((name for name in table_names if name.lower() in prompt_lower), table_names[0])
        quoted_table = quote_identifier(selected_table)
        dataframe = pd.read_sql_query(f"SELECT * FROM {quoted_table} LIMIT ?", connection, params=(max_rows,))

    return dataframe, selected_table
