# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
import re
from typing import Iterable


SENSITIVE_COLUMN_PATTERN = re.compile(
    r"(aadhaar|account|address|card|client|customer|dob|email|employee|iban|mobile|name|national|"
    r"passport|phone|postcode|sort.?code|ssn|tax|telephone|user.?id|zip)",
    re.IGNORECASE,
)
EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"\b(?:(?:phone|mobile|tel)[:\s]*|\+)\d[\d ().-]{7,}\d\b", re.IGNORECASE)
LONG_NUMBER_PATTERN = re.compile(r"\b\d{8,}\b")


def looks_sensitive_column(column_name: object) -> bool:
    return bool(SENSITIVE_COLUMN_PATTERN.search(str(column_name)))


def redact_text(value: object) -> str:
    text = str(value)
    text = EMAIL_PATTERN.sub("[redacted email]", text)
    text = PHONE_PATTERN.sub("[redacted number]", text)
    text = LONG_NUMBER_PATTERN.sub("[redacted number]", text)
    return text


def sanitize_label(value: object, fallback: str = "Unknown") -> str:
    text = redact_text(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return fallback
    return text[:80]


def non_sensitive_columns(columns: Iterable[object]) -> list[str]:
    return [str(column) for column in columns if not looks_sensitive_column(column)]
