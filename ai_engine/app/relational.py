# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
from dataclasses import dataclass
import re
from typing import Any

import pandas as pd

from .ingestion import dataframe_schema


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _safe_table_name(filename: str, index: int) -> str:
    stem = filename.rsplit(".", 1)[0]
    normalized = _normalize_name(stem)
    return normalized or f"dataset_{index + 1}"


def _sample_values(dataframe: pd.DataFrame, column: str) -> set[str]:
    return {
        str(value).strip().lower()
        for value in dataframe[column].dropna().astype(str).head(2500).tolist()
        if str(value).strip()
    }


def _column_candidates(columns: list[str]) -> dict[str, str]:
    return {_normalize_name(column): column for column in columns}


def _relationship_score(left: pd.DataFrame, left_column: str, right: pd.DataFrame, right_column: str) -> float:
    left_values = _sample_values(left, left_column)
    right_values = _sample_values(right, right_column)
    if not left_values or not right_values:
        return 0.0

    overlap = left_values & right_values
    if not overlap:
        return 0.0

    denominator = max(min(len(left_values), len(right_values)), 1)
    score = len(overlap) / denominator
    key_name = _normalize_name(left_column)
    if key_name.endswith("id") or key_name.endswith("_id") or key_name == "id":
        score += 0.25
    return round(score, 4)


def _detect_pairwise_relationship(left_entry: "SourceTable", right_entry: "SourceTable") -> dict[str, Any] | None:
    left_candidates = _column_candidates(list(left_entry.dataframe.columns))
    right_candidates = _column_candidates(list(right_entry.dataframe.columns))
    shared_names = set(left_candidates) & set(right_candidates)
    if not shared_names:
        return None

    best_match: dict[str, Any] | None = None
    for shared_name in shared_names:
        left_column = left_candidates[shared_name]
        right_column = right_candidates[shared_name]
        score = _relationship_score(left_entry.dataframe, left_column, right_entry.dataframe, right_column)
        if score <= 0:
            continue

        candidate = {
            "confidence": score,
            "join_type": "left",
            "left_column": left_column,
            "left_dataset": left_entry.name,
            "match_key": shared_name,
            "right_column": right_column,
            "right_dataset": right_entry.name,
            "status": "detected",
        }
        if best_match is None or candidate["confidence"] > best_match["confidence"]:
            best_match = candidate

    return best_match


def _rename_conflicting_columns(merged: pd.DataFrame, candidate: pd.DataFrame, join_key: str, prefix: str) -> pd.DataFrame:
    renamed = candidate.copy()
    collisions = [column for column in renamed.columns if column != join_key and column in merged.columns]
    if collisions:
        renamed = renamed.rename(columns={column: f"{prefix}__{column}" for column in collisions})
    return renamed


@dataclass
class SourceTable:
    columns: list[dict[str, Any]]
    dataframe: pd.DataFrame
    filename: str
    filetype: str
    name: str
    row_count: int


def build_source_table(dataframe: pd.DataFrame, filename: str, filetype: str, index: int) -> SourceTable:
    prepared = dataframe.copy()
    prepared.columns = [str(column).strip() for column in prepared.columns]
    return SourceTable(
        columns=[column.model_dump() for column in dataframe_schema(prepared)],
        dataframe=prepared,
        filename=filename,
        filetype=filetype,
        name=_safe_table_name(filename, index),
        row_count=len(prepared),
    )


def merge_source_tables(entries: list[SourceTable]) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    if not entries:
        raise ValueError("Upload at least one dataset to continue.")

    ordered_entries = sorted(entries, key=lambda entry: (-entry.row_count, entry.name))
    merged_entry = ordered_entries[0]
    merged = merged_entry.dataframe.copy()
    linked = [merged_entry.name]
    remaining = ordered_entries[1:]
    relationships: list[dict[str, Any]] = []
    unlinked: list[str] = []

    while remaining:
        best_pair: tuple[dict[str, Any], SourceTable] | None = None
        for candidate in remaining:
            current_entry = SourceTable(
                columns=[],
                dataframe=merged,
                filename="merged.csv",
                filetype="csv",
                name="merged_workspace",
                row_count=len(merged),
            )
            detected = _detect_pairwise_relationship(current_entry, candidate)
            if detected is None:
                continue
            if best_pair is None or detected["confidence"] > best_pair[0]["confidence"]:
                best_pair = (detected, candidate)

        if best_pair is None:
            unlinked.extend(entry.name for entry in remaining)
            break

        detected, candidate = best_pair
        join_key = detected["right_column"]
        left_key = detected["left_column"]
        renamed_candidate = _rename_conflicting_columns(merged, candidate.dataframe, join_key, candidate.name)
        merged = merged.merge(renamed_candidate, how="left", left_on=left_key, right_on=join_key)
        if left_key != join_key and join_key in merged.columns:
            merged = merged.drop(columns=[join_key])

        detected["right_filename"] = candidate.filename
        relationships.append(detected)
        linked.append(candidate.name)
        remaining = [entry for entry in remaining if entry.name != candidate.name]

    source_files = [
        {
            "columns": entry.columns,
            "file_type": entry.filetype,
            "filename": entry.filename,
            "name": entry.name,
            "row_count": entry.row_count,
            "status": "linked" if entry.name in linked else "unlinked",
        }
        for entry in ordered_entries
    ]
    return merged, relationships, source_files, unlinked
