# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .ingestion import load_sqlite_table, read_dataframe, resolve_dataset_file
from .llm import maybe_rewrite_narrative
from .models import AnalyticsSidebar, ChatRequest, ChatResponse, Transparency
from .security import looks_sensitive_column, non_sensitive_columns, sanitize_label
from .settings import get_settings


@dataclass
class ColumnPlan:
    categorical: list[str]
    datetime: list[str]
    numeric: list[str]
    sensitive: set[str]


def _normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).strip().lower()).strip()


def _column_variants(column_name: str) -> set[str]:
    normalized = _normalize_column_name(column_name)
    variants = {normalized, normalized.replace(" ", "")}

    if "__" in column_name:
        left, right = column_name.split("__", 1)
        variants.add(_normalize_column_name(right))
        variants.add(_normalize_column_name(f"{left} {right}"))

    parts = [part for part in re.split(r"[_\s]+", normalized) if part]
    if parts:
        variants.add(" ".join(parts[-2:]))
        variants.add(parts[-1])
        if parts[-1] == "name" and len(parts) >= 2:
            variants.add(f"{parts[-2]} name")
        if parts[-1] == "id" and len(parts) >= 2:
            variants.add(f"{parts[-2]} id")

    singularized = set()
    for variant in list(variants):
        if variant.endswith("s") and len(variant) > 3:
            singularized.add(variant[:-1])
    variants.update(singularized)
    return {variant for variant in variants if variant}


def _format_number(value: float) -> str:
    if pd.isna(value):
        return "0"
    if abs(value) >= 1000000:
        return f"{value / 1000000:,.1f}m"
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if float(value).is_integer():
        return f"{value:,.0f}"
    return f"{value:,.2f}"


def _safe_float(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), 4)
    except (TypeError, ValueError):
        return 0.0


def _prompt_has_phrase(prompt_lower: str, phrase: str) -> bool:
    normalized_prompt = f" {prompt_lower.replace('_', ' ')} "
    normalized_phrase = phrase.lower().replace("_", " ").strip()
    compact_prompt = normalized_prompt.replace(" ", "")
    compact_phrase = normalized_phrase.replace(" ", "")
    return normalized_phrase in normalized_prompt or (len(compact_phrase) > 2 and compact_phrase in compact_prompt)


def _column_prompt_score(prompt_lower: str, column_name: str) -> int:
    normalized = _normalize_column_name(column_name)
    variants = _column_variants(column_name) | {column_name.lower()}
    if normalized.endswith(" id"):
        base_name = normalized[:-3].strip()
        if len(base_name) > 2:
            variants.add(base_name)
            variants.add(f"{base_name}s")
            if base_name.endswith("y"):
                variants.add(f"{base_name[:-1]}ies")

    matched = [variant for variant in variants if _prompt_has_phrase(prompt_lower, variant)]
    return max((len(match) for match in matched), default=0)


def _select_column_from_prompt(prompt: str, columns: list[str]) -> str | None:
    prompt_lower = prompt.lower()
    scored = [
        (_column_prompt_score(prompt_lower, column), index, column)
        for index, column in enumerate(columns)
    ]
    scored = [item for item in scored if item[0] > 0]
    if not scored:
        return None

    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[0][2]


def _build_column_plan(dataframe: pd.DataFrame) -> ColumnPlan:
    sensitive = {str(column) for column in dataframe.columns if looks_sensitive_column(column)}
    numeric: list[str] = []
    categorical: list[str] = []
    datetimes: list[str] = []

    for column in dataframe.columns:
        column_name = str(column)
        if column_name in sensitive:
            continue

        series = dataframe[column]
        if column_name.lower().endswith("_id") or column_name.lower().endswith(" id"):
            categorical.append(column_name)
            continue

        if pd.api.types.is_numeric_dtype(series):
            numeric.append(column_name)
            continue

        if pd.api.types.is_datetime64_any_dtype(series):
            datetimes.append(column_name)
            continue

        parsed_dates = pd.to_datetime(series, errors="coerce")
        if parsed_dates.notna().mean() >= 0.7:
            dataframe[column_name] = parsed_dates
            datetimes.append(column_name)
            continue

        unique_count = series.nunique(dropna=True)
        if 1 < unique_count <= 80:
            categorical.append(column_name)

    return ColumnPlan(categorical=categorical, datetime=datetimes, numeric=numeric, sensitive=sensitive)


def _select_metric(prompt: str, plan: ColumnPlan) -> str | None:
    if not plan.numeric:
        return None

    metric_from_prompt = _select_column_from_prompt(prompt, plan.numeric)
    if metric_from_prompt:
        return metric_from_prompt

    for term in ["revenue", "sales", "profit", "cost", "amount", "balance", "value", "volume", "count", "score"]:
        for metric in plan.numeric:
            if term in _normalize_column_name(metric):
                return metric

    return plan.numeric[0]


def _find_numeric_by_terms(plan: ColumnPlan, terms: list[str]) -> str | None:
    for metric in plan.numeric:
        normalized = _normalize_column_name(metric)
        if any(term in normalized for term in terms):
            return metric
    return None


def _add_derived_metrics(dataframe: pd.DataFrame, prompt: str) -> pd.DataFrame:
    working = dataframe.copy()
    normalized_prompt = _normalize_column_name(prompt)
    wants_revenue = any(term in normalized_prompt for term in ["revenue", "sales", "gmv", "income", "turnover"])
    if not wants_revenue:
        return working

    existing_revenue = [
        column for column in working.columns
        if any(term in _normalize_column_name(column) for term in ["revenue", "sales", "gmv", "turnover"])
    ]
    if existing_revenue:
        return working

    temp_plan = _build_column_plan(working)
    quantity_column = _find_numeric_by_terms(temp_plan, ["quantity", "qty", "units", "volume", "count"])
    price_column = _find_numeric_by_terms(temp_plan, ["price", "unit price", "rate", "amount", "value"])

    if quantity_column and price_column and quantity_column != price_column:
        quantity_series = pd.to_numeric(working[quantity_column], errors="coerce")
        price_series = pd.to_numeric(working[price_column], errors="coerce")
        if quantity_series.notna().any() and price_series.notna().any():
            working["derived_revenue"] = quantity_series.fillna(0) * price_series.fillna(0)

    return working


def _prioritize_metric_definitions(prompt: str, plan: ColumnPlan, metric_definitions: list[dict[str, Any]]) -> ColumnPlan:
    prompt_lower = prompt.lower()
    for definition in metric_definitions:
        metric_name = str(definition.get("metricName", ""))
        source_columns = [str(column) for column in definition.get("sourceColumns", [])]
        if metric_name and metric_name.lower() in prompt_lower:
            for source_column in source_columns:
                if source_column in plan.numeric:
                    reordered = [source_column] + [metric for metric in plan.numeric if metric != source_column]
                    return ColumnPlan(
                        categorical=plan.categorical,
                        datetime=plan.datetime,
                        numeric=reordered,
                        sensitive=plan.sensitive,
                    )
    return plan


def _select_category(prompt: str, plan: ColumnPlan) -> str | None:
    if not plan.categorical:
        return None

    category_from_prompt = _select_column_from_prompt(prompt, plan.categorical)
    if category_from_prompt:
        return category_from_prompt

    descriptive = [
        category for category in plan.categorical
        if not (category.lower().endswith("_id") or category.lower().endswith(" id") or category.lower() == "id")
    ]
    return descriptive[0] if descriptive else plan.categorical[0]


def _count_display_label(column_name: str | None) -> str:
    if not column_name:
        return "rows"

    normalized = column_name.lower().replace("_", " ")
    if normalized == "order id":
        return "orders"
    if normalized.endswith(" id"):
        base_name = normalized[:-3].strip()
        if base_name.endswith("y"):
            return f"{base_name[:-1]}ies"
        return f"{base_name}s"
    return normalized


def _select_identifier_category(prompt: str, plan: ColumnPlan) -> str | None:
    category_from_prompt = _select_column_from_prompt(prompt, plan.categorical)
    if category_from_prompt:
        return category_from_prompt

    for category in plan.categorical:
        if category.lower().endswith("_id") or category.lower().endswith(" id"):
            return category

    return _select_category(prompt, plan)


def _select_identifier_metric(prompt: str, plan: ColumnPlan) -> str | None:
    identifier_columns = [
        category for category in plan.categorical
        if category.lower().endswith("_id") or category.lower().endswith(" id") or category.lower() == "id"
    ]
    metric_from_prompt = _select_column_from_prompt(prompt, identifier_columns)
    if metric_from_prompt:
        return metric_from_prompt
    return identifier_columns[0] if identifier_columns else None


def _data_points_from_series(series: pd.Series, limit: int = 8) -> list[dict[str, Any]]:
    return [
        {"label": sanitize_label(index), "value": _safe_float(value)}
        for index, value in series.head(limit).items()
    ]


def _outliers(points: list[dict[str, Any]]) -> list[str]:
    values = [point["value"] for point in points]
    if len(values) < 3:
        return []

    mean_value = sum(values) / len(values)
    spread = pd.Series(values).std()
    total = sum(abs(value) for value in values)
    notes: list[str] = []

    for point in points:
        value = point["value"]
        if spread and abs(value - mean_value) > 1.75 * spread:
            notes.append(f"{point['label']} is far from the rest")
        elif total and abs(value) / total >= 0.5:
            notes.append(f"{point['label']} makes up more than half of the visible total")

    return notes[:3]


def _group_by_category(dataframe: pd.DataFrame, category: str, metric: str) -> pd.Series:
    working = dataframe[[category, metric]].dropna()
    working[metric] = pd.to_numeric(working[metric], errors="coerce")
    working = working.dropna(subset=[metric])
    grouped = working.groupby(category)[metric].sum().sort_values(ascending=False)
    return grouped


def dashboard_preview_for_dataframe(dataframe: pd.DataFrame, source_label: str = "Uploaded dataset") -> dict[str, Any]:
    dataframe = _add_derived_metrics(dataframe, "summarize the revenue and totals")
    dataframe.columns = [str(column) for column in dataframe.columns]
    plan = _build_column_plan(dataframe)
    metric = _select_metric("summary", plan)
    category = _select_category("summary", plan)

    chart_points: list[dict[str, Any]]
    chart_type = "bar"
    insight_columns: list[str]

    if metric and category:
        grouped = _group_by_category(dataframe, category, metric).head(6)
        chart_points = _data_points_from_series(grouped, limit=6)
        total_value = float(grouped.sum()) if not grouped.empty else 0.0
        top_label = sanitize_label(grouped.index[0]) if not grouped.empty else "the leading group"
        draft = (
            f"I processed {len(dataframe):,} rows. The strongest visible driver is {top_label}, "
            f"leading the grouped {metric} totals."
        )
        insight_columns = _source_columns(metric, category, None, dataframe)
    elif category:
        grouped = dataframe[category].dropna().astype(str).value_counts().head(6)
        chart_points = _data_points_from_series(grouped, limit=6)
        total_value = float(grouped.sum()) if not grouped.empty else 0.0
        top_label = sanitize_label(grouped.index[0]) if not grouped.empty else "the leading group"
        draft = (
            f"I processed {len(dataframe):,} rows. The largest safe segment in {category} is {top_label}."
        )
        insight_columns = _source_columns(None, category, None, dataframe)
    elif metric:
        total_metric = float(pd.to_numeric(dataframe[metric], errors="coerce").sum())
        chart_points = [{"label": metric, "value": _safe_float(total_metric)}]
        total_value = total_metric
        chart_type = "table"
        draft = f"I processed {len(dataframe):,} rows. The visible total for {metric} is {_format_number(total_metric)}."
        insight_columns = _source_columns(metric, None, None, dataframe)
    else:
        chart_points = [{"label": "Rows loaded", "value": len(dataframe)}]
        total_value = float(len(dataframe))
        chart_type = "table"
        draft = f"I processed {len(dataframe):,} rows and prepared a safe analytics view."
        insight_columns = non_sensitive_columns(dataframe.columns)[:5]

    pie_points = []
    for point in chart_points[:5]:
        value = abs(float(point.get("value", 0) or 0))
        if value > 0:
            pie_points.append({"label": point["label"], "value": _safe_float(value)})

    narrative = maybe_rewrite_narrative(
        "Summarize this uploaded dataset.",
        draft,
        {
            "chart_type": chart_type,
            "data_points": chart_points,
            "row_count": len(dataframe),
            "source_label": source_label,
            "total_value": _safe_float(total_value),
        },
    )

    return {
        "bar_chart": {
            "chart_type": chart_type,
            "data_points": chart_points,
            "outliers_noted": _outliers(chart_points),
        },
        "headline_metrics": [
            {"label": "Rows", "value": len(dataframe)},
            {"label": "Columns", "value": len(non_sensitive_columns(dataframe.columns))},
            {"label": "Segments", "value": len(chart_points)},
        ],
        "insight": narrative,
        "pie_chart": {
            "chart_type": "pie",
            "data_points": pie_points,
            "outliers_noted": _outliers(pie_points),
        },
        "transparency": _transparency(source_label, insight_columns, None, "upload-time preview").model_dump(),
    }


def _requested_category_values(dataframe: pd.DataFrame, category: str, prompt: str) -> list[str]:
    prompt_lower = prompt.lower()
    matches: list[str] = []
    for raw_value in dataframe[category].dropna().astype(str).unique()[:250]:
        if raw_value.lower() in prompt_lower:
            matches.append(raw_value)
        if len(matches) == 2:
            break
    return matches


def _source_columns(metric: str | None, category: str | None, datetime_column: str | None, dataframe: pd.DataFrame) -> list[str]:
    columns = [column for column in [datetime_column, category, metric] if column]
    return non_sensitive_columns(columns) or non_sensitive_columns(dataframe.columns)[:5]


def _transparency(source_label: str, columns: list[str], table_name: str | None, action: str) -> Transparency:
    data_sources = [source_label, f"Columns used: {', '.join(columns) if columns else 'safe aggregate columns'}"]
    if table_name:
        data_sources.append(f"SQLite table: {sanitize_label(table_name)}")

    return Transparency(
        data_sources=data_sources,
        metric_definition_used=f"Dynamic schema extraction; {action}",
    )


def _fallback_summary(dataframe: pd.DataFrame, prompt: str, source_label: str, table_name: str | None) -> ChatResponse:
    dataframe = _add_derived_metrics(dataframe, prompt)
    plan = _build_column_plan(dataframe)
    category = _select_category(prompt, plan)

    if category:
        counts = dataframe[category].dropna().astype(str).value_counts().head(8)
        points = _data_points_from_series(counts)
        top_label = points[0]["label"] if points else "the largest group"
        draft = f"I found {len(dataframe):,} rows. The largest group I can safely show in {category} is {top_label}."
        columns = _source_columns(None, category, None, dataframe)
        analytics = AnalyticsSidebar(chart_type="bar", data_points=points, outliers_noted=_outliers(points))
    else:
        draft = f"I found {len(dataframe):,} rows, but there were no safe number columns to total."
        columns = non_sensitive_columns(dataframe.columns)[:5]
        analytics = AnalyticsSidebar(chart_type="table", data_points=[{"label": "Rows scanned", "value": len(dataframe)}], outliers_noted=[])

    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "safe row-count summary"),
    )


def _change_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse | None:
    metric = _select_metric(prompt, plan)
    datetime_column = plan.datetime[0] if plan.datetime else None
    if not metric or not datetime_column:
        return None

    working = dataframe[[datetime_column, metric]].dropna().copy()
    working[datetime_column] = pd.to_datetime(working[datetime_column], errors="coerce")
    working[metric] = pd.to_numeric(working[metric], errors="coerce")
    working = working.dropna()
    if working.empty:
        return None

    working["_period"] = working[datetime_column].dt.to_period("M").astype(str)
    trend = working.groupby("_period")[metric].sum().sort_index()
    if len(trend) < 2:
        return None

    previous_label, current_label = trend.index[-2], trend.index[-1]
    previous_value, current_value = float(trend.iloc[-2]), float(trend.iloc[-1])
    delta = current_value - previous_value
    percentage = (delta / previous_value * 100) if previous_value else 0.0
    direction = "increased" if delta > 0 else "decreased" if delta < 0 else "stayed about the same"
    points = _data_points_from_series(trend.tail(8))

    category = _select_category(prompt, plan)
    contributor_sentence = ""
    if category:
        recent_rows = dataframe[pd.to_datetime(dataframe[datetime_column], errors="coerce").dt.to_period("M").astype(str) == current_label]
        grouped = _group_by_category(recent_rows, category, metric)
        if not grouped.empty:
            contributor_sentence = f" The largest group I can safely show in {current_label} was {sanitize_label(grouped.index[0])}."

    draft = (
        f"{metric} {direction} by {_format_number(abs(delta))} ({abs(percentage):.1f}%) "
        f"from {previous_label} to {current_label}.{contributor_sentence}"
    )
    analytics = AnalyticsSidebar(chart_type="line", data_points=points, outliers_noted=_outliers(points))
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    columns = _source_columns(metric, category, datetime_column, dataframe)
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "month-by-month aggregate comparison"),
    )


def _compare_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse | None:
    metric = _select_metric(prompt, plan)
    category = _select_category(prompt, plan)
    if not metric or not category:
        return None

    grouped = _group_by_category(dataframe, category, metric)
    if grouped.empty:
        return None

    requested_values = _requested_category_values(dataframe, category, prompt)
    if len(requested_values) == 2:
        selected = grouped[grouped.index.astype(str).isin(requested_values)]
    else:
        selected = grouped.head(2)

    if len(selected) < 2:
        selected = grouped.head(2)

    selected = selected.sort_values(ascending=False)
    points = _data_points_from_series(selected)
    first, second = selected.index[0], selected.index[1]
    delta = float(selected.iloc[0] - selected.iloc[1])
    draft = (
        f"{sanitize_label(first)} is higher than {sanitize_label(second)} by {_format_number(abs(delta))} "
        f"for {metric}. I used grouped {category} totals for this."
    )
    analytics = AnalyticsSidebar(chart_type="bar", data_points=points, outliers_noted=_outliers(points))
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    columns = _source_columns(metric, category, None, dataframe)
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "grouped aggregate comparison"),
    )


def _breakdown_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse | None:
    metric = _select_metric(prompt, plan)
    category = _select_category(prompt, plan)
    if not metric or not category:
        return None

    grouped = _group_by_category(dataframe, category, metric)
    if grouped.empty:
        return None

    points = _data_points_from_series(grouped)
    top_label = points[0]["label"] if points else "the largest group"
    top_value = points[0]["value"] if points else 0
    total = sum(point["value"] for point in points) or 1
    share = top_value / total * 100
    draft = f"{metric} is mostly driven by {top_label}, which makes up {share:.1f}% of the grouped total I can safely show."
    analytics = AnalyticsSidebar(chart_type="bar", data_points=points, outliers_noted=_outliers(points))
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    columns = _source_columns(metric, category, None, dataframe)
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "safe grouped breakdown"),
    )


def _count_and_top_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse | None:
    prompt_lower = prompt.lower()
    wants_count = any(phrase in prompt_lower for phrase in ["how many", "count", "total order", "orders total", "total orders"])
    wants_top = any(word in prompt_lower for word in ["maximum", "highest", "max ", "top ", "largest", "most"])

    if not wants_count and not wants_top:
        return None

    metric = _select_metric(prompt, plan)
    category = _select_identifier_category(prompt, plan)
    points: list[dict[str, Any]] = []
    columns = non_sensitive_columns(dataframe.columns)[:5]
    narrative_parts: list[str] = []

    if wants_count:
        count_column = category if category and category in dataframe.columns else None
        order_count = dataframe[count_column].nunique(dropna=True) if count_column else len(dataframe)
        count_label = _count_display_label(count_column)
        points.append({"label": f"Total {count_label}", "value": int(order_count)})
        narrative_parts.append(f"I found {int(order_count):,} total {count_label}.")
        columns = _source_columns(None, count_column, None, dataframe)

    if wants_top and metric:
        working = dataframe.copy()
        working[metric] = pd.to_numeric(working[metric], errors="coerce")
        working = working.dropna(subset=[metric])

        if category and category in working.columns:
            grouped = _group_by_category(working, category, metric)
            if not grouped.empty:
                top_label = sanitize_label(grouped.index[0])
                top_value = float(grouped.iloc[0])
                points.extend(_data_points_from_series(grouped, limit=5))
                narrative_parts.append(f"The highest {metric} is {top_label} at {_format_number(top_value)}.")
                columns = _source_columns(metric, category, None, dataframe)
        elif not working.empty:
            top_value = float(working[metric].max())
            points.append({"label": f"Highest {metric}", "value": _safe_float(top_value)})
            narrative_parts.append(f"The highest {metric} is {_format_number(top_value)}.")
            columns = _source_columns(metric, None, None, dataframe)

    if not narrative_parts:
        return None

    analytics = AnalyticsSidebar(chart_type="bar" if len(points) > 1 else "table", data_points=points[:8], outliers_noted=_outliers(points))
    draft = " ".join(narrative_parts)
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "count and highest grouped value"),
    )


def _grouped_identifier_count_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse | None:
    prompt_lower = prompt.lower()
    if not any(keyword in prompt_lower for keyword in ["highest", "most", "top", "bookings", "orders", "count", "how many"]):
        return None

    category = _select_category(prompt, plan)
    identifier_metric = _select_identifier_metric(prompt, plan)
    if not category or not identifier_metric or category == identifier_metric:
        return None

    working = dataframe[[category, identifier_metric]].dropna().copy()
    if working.empty:
        return None

    grouped = working.groupby(category)[identifier_metric].nunique().sort_values(ascending=False)
    if grouped.empty:
        return None

    points = _data_points_from_series(grouped, limit=6)
    top_label = points[0]["label"] if points else "the top group"
    top_value = points[0]["value"] if points else 0
    draft = (
        f"{top_label} has the highest number of unique {identifier_metric.replace('_', ' ')} values "
        f"at {_format_number(top_value)}."
    )
    analytics = AnalyticsSidebar(chart_type="bar", data_points=points, outliers_noted=_outliers(points))
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    columns = _source_columns(identifier_metric, category, None, dataframe)
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "grouped identifier count"),
    )


def _select_aggregate_operations(prompt: str) -> list[str]:
    prompt_lower = prompt.lower()
    operations: list[str] = []
    if any(phrase in prompt_lower for phrase in ["how many", "count", "number of", "total order", "orders total", "total orders"]):
        operations.append("count")
    if any(word in prompt_lower for word in ["average", "avg", "mean"]):
        operations.append("average")
    if any(word in prompt_lower for word in ["minimum", "lowest", "least", "smallest", "bottom"]):
        operations.append("min")
    if any(word in prompt_lower for word in ["maximum", "highest", "max ", "top ", "largest", "most"]):
        operations.append("max")
    if "sum" in prompt_lower or ("total" in prompt_lower and "count" not in operations):
        operations.append("sum")
    return list(dict.fromkeys(operations))


def _schema_response(dataframe: pd.DataFrame, prompt: str, source_label: str, table_name: str | None) -> ChatResponse | None:
    prompt_lower = prompt.lower()
    if not any(phrase in prompt_lower for phrase in ["columns", "schema", "fields", "what data", "dataset have"]):
        return None

    safe_columns = non_sensitive_columns(dataframe.columns)
    visible_columns = safe_columns[:12]
    points = [
        {"label": column, "value": int(dataframe[column].notna().sum())}
        for column in visible_columns
    ]
    draft = (
        f"This dataset has {len(dataframe):,} rows and {len(safe_columns):,} safe columns I can use: "
        f"{', '.join(visible_columns)}."
    )
    analytics = AnalyticsSidebar(chart_type="table", data_points=points, outliers_noted=[])
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, visible_columns, table_name, "dataset schema summary"),
    )


def _grouped_metric(dataframe: pd.DataFrame, category: str, metric: str, operation: str) -> pd.Series:
    working = dataframe[[category, metric]].dropna().copy()
    working[metric] = pd.to_numeric(working[metric], errors="coerce")
    working = working.dropna(subset=[metric])
    if operation == "average":
        grouped = working.groupby(category)[metric].mean()
    else:
        grouped = working.groupby(category)[metric].sum()
    return grouped.sort_values(ascending=operation == "min")


def _single_metric_value(dataframe: pd.DataFrame, metric: str, operation: str) -> float:
    series = pd.to_numeric(dataframe[metric], errors="coerce").dropna()
    if series.empty:
        return 0.0
    if operation == "average":
        return float(series.mean())
    if operation == "min":
        return float(series.min())
    if operation == "max":
        return float(series.max())
    return float(series.sum())


def _operation_label(operation: str) -> str:
    return {
        "average": "average",
        "max": "highest",
        "min": "lowest",
        "sum": "total",
    }.get(operation, operation)


def _should_group_metric(prompt: str, operation: str, category: str | None) -> bool:
    if not category:
        return False

    prompt_lower = prompt.lower()
    return (
        " by " in prompt_lower
        or "which" in prompt_lower
        or operation in {"max", "min"}
        or _column_prompt_score(prompt_lower, category) > 0
    )


def _data_chat_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse | None:
    schema_answer = _schema_response(dataframe, prompt, source_label, table_name)
    if schema_answer:
        return schema_answer

    prompt_lower = prompt.lower()
    operations = _select_aggregate_operations(prompt)
    metric = _select_metric(prompt, plan)
    category = _select_column_from_prompt(prompt, plan.categorical)

    if not category and any(operation in operations for operation in ["max", "min"]):
        category = _select_identifier_category(prompt, plan)
    if not operations and metric and " by " in prompt_lower:
        operations = ["sum"]
        category = category or _select_category(prompt, plan)
    if not operations:
        return None

    points: list[dict[str, Any]] = []
    narrative_parts: list[str] = []
    columns = non_sensitive_columns(dataframe.columns)[:5]

    if "count" in operations:
        count_column = _select_column_from_prompt(prompt, plan.categorical + plan.numeric)
        if "row" in prompt_lower or "record" in prompt_lower:
            count_column = None

        count_value = dataframe[count_column].nunique(dropna=True) if count_column else len(dataframe)
        count_label = _count_display_label(count_column)
        points.append({"label": f"Total {count_label}", "value": int(count_value)})
        narrative_parts.append(f"I found {int(count_value):,} total {count_label}.")
        columns = _source_columns(None, count_column, None, dataframe)

    numeric_operations = [operation for operation in operations if operation in {"average", "max", "min", "sum"}]
    for operation in numeric_operations:
        if not metric:
            continue

        if _should_group_metric(prompt, operation, category):
            grouped = _grouped_metric(dataframe, category, metric, operation)
            if grouped.empty:
                continue

            if operation != "min":
                grouped = grouped.sort_values(ascending=False)

            top_label = sanitize_label(grouped.index[0])
            top_value = float(grouped.iloc[0])
            points.extend(_data_points_from_series(grouped, limit=6))
            group_label = category.replace("_", " ")
            if operation in {"average", "sum"}:
                narrative_parts.append(
                    f"The highest {_operation_label(operation)} {metric} by {group_label} is {top_label} at {_format_number(top_value)}."
                )
            else:
                narrative_parts.append(
                    f"The {_operation_label(operation)} {metric} by {group_label} is {top_label} at {_format_number(top_value)}."
                )
            columns = _source_columns(metric, category, None, dataframe)
            continue

        value = _single_metric_value(dataframe, metric, operation)
        points.append({"label": f"{_operation_label(operation).title()} {metric}", "value": _safe_float(value)})
        narrative_parts.append(f"The {_operation_label(operation)} {metric} is {_format_number(value)}.")
        columns = _source_columns(metric, None, None, dataframe)

    if not narrative_parts:
        return None

    deduped_points = []
    seen_labels: set[str] = set()
    for point in points:
        if point["label"] in seen_labels:
            continue
        seen_labels.add(point["label"])
        deduped_points.append(point)

    analytics = AnalyticsSidebar(
        chart_type="bar" if len(deduped_points) > 1 else "table",
        data_points=deduped_points[:8],
        outliers_noted=_outliers(deduped_points),
    )
    draft = " ".join(narrative_parts)
    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "natural-language aggregate answer"),
    )


def _summary_response(dataframe: pd.DataFrame, prompt: str, plan: ColumnPlan, source_label: str, table_name: str | None) -> ChatResponse:
    metric = _select_metric(prompt, plan)
    category = _select_category(prompt, plan)
    if not metric:
        return _fallback_summary(dataframe, prompt, source_label, table_name)

    if category:
        grouped = _group_by_category(dataframe, category, metric)
        points = _data_points_from_series(grouped)
        total = grouped.sum()
        top_label = points[0]["label"] if points else "the largest group"
        draft = f"I found {len(dataframe):,} rows. {metric} totals {_format_number(float(total))}, with {top_label} as the largest group I can safely show."
        analytics = AnalyticsSidebar(chart_type="bar", data_points=points, outliers_noted=_outliers(points))
        columns = _source_columns(metric, category, None, dataframe)
    else:
        total = pd.to_numeric(dataframe[metric], errors="coerce").sum()
        draft = f"I found {len(dataframe):,} rows. {metric} totals {_format_number(float(total))}."
        analytics = AnalyticsSidebar(chart_type="table", data_points=[{"label": metric, "value": _safe_float(total)}], outliers_noted=[])
        columns = _source_columns(metric, None, None, dataframe)

    narrative = maybe_rewrite_narrative(prompt, draft, analytics.model_dump())
    return ChatResponse(
        analytics_sidebar=analytics,
        insight_narrative=narrative,
        query_status="success",
        transparency=_transparency(source_label, columns, table_name, "concise aggregate summary"),
    )


def analyze_chat(request: ChatRequest) -> ChatResponse:
    settings = get_settings()
    dataset_file = resolve_dataset_file(request.dataset_id, settings)
    table_name: str | None = None

    if dataset_file.suffix.lower() in {".sqlite", ".db"}:
        dataframe, table_name = load_sqlite_table(dataset_file, settings.max_analysis_rows, request.prompt)
    else:
        dataframe = read_dataframe(dataset_file, settings.max_analysis_rows)

    source_label = request.source_label or f"Uploaded File: {Path(dataset_file).name}"
    dataframe = _add_derived_metrics(dataframe, request.prompt)
    dataframe.columns = [str(column) for column in dataframe.columns]
    plan = _build_column_plan(dataframe)
    plan = _prioritize_metric_definitions(request.prompt, plan, request.metric_definitions)
    prompt_lower = request.prompt.lower()

    response: ChatResponse | None = None
    if any(word in prompt_lower for word in ["change", "changed", "decrease", "drop", "increase", "rise", "shift", "trend"]):
        response = _change_response(dataframe, request.prompt, plan, source_label, table_name)
    if response is None and any(word in prompt_lower for word in ["compare", "versus", " vs ", "difference", "between"]):
        response = _compare_response(dataframe, request.prompt, plan, source_label, table_name)
    if response is None and any(word in prompt_lower for word in ["breakdown", "break down", "split", "contribution"]):
        response = _breakdown_response(dataframe, request.prompt, plan, source_label, table_name)
    if response is None:
        response = _grouped_identifier_count_response(dataframe, request.prompt, plan, source_label, table_name)
    if response is None:
        response = _data_chat_response(dataframe, request.prompt, plan, source_label, table_name)
    if response is None:
        response = _count_and_top_response(dataframe, request.prompt, plan, source_label, table_name)
    if response is None:
        response = _summary_response(dataframe, request.prompt, plan, source_label, table_name)

    return response
