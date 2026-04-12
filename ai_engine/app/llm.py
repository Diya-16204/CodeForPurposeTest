# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
import json
from urllib import request
from urllib.parse import quote

from .security import redact_text
from .settings import get_settings


SYSTEM_INSTRUCTION = (
    "Rewrite the provided analytics answer in everyday language. "
    "Use only the aggregate facts supplied. Do not add names, emails, account numbers, or row-level details. "
    "Keep it under four sentences."
)


def _post_json(url: str, payload: dict, headers: dict[str, str]) -> dict:
    encoded = json.dumps(payload).encode("utf-8")
    http_request = request.Request(url, data=encoded, headers=headers, method="POST")
    with request.urlopen(http_request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _gemini_rewrite(prompt: str, draft: str, facts: dict) -> str | None:
    settings = get_settings()
    if not settings.gemini_api_key or not settings.gemini_model:
        return None

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{quote(settings.gemini_model)}:generateContent?key={quote(settings.gemini_api_key)}"
    )
    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "text": (
                            f"{SYSTEM_INSTRUCTION}\nQuestion: {prompt}\nDraft: {draft}\n"
                            f"Aggregate facts: {json.dumps(facts)}"
                        )
                    }
                ]
            }
        ]
    }
    result = _post_json(url, payload, {"Content-Type": "application/json"})
    candidates = result.get("candidates", [])
    if not candidates:
        return None
    return candidates[0].get("content", {}).get("parts", [{}])[0].get("text")


def _groq_rewrite(prompt: str, draft: str, facts: dict) -> str | None:
    settings = get_settings()
    if not settings.groq_api_key or not settings.groq_model:
        return None

    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {
                "role": "user",
                "content": f"Question: {prompt}\nDraft: {draft}\nAggregate facts: {json.dumps(facts)}",
            },
        ],
        "model": settings.groq_model,
        "temperature": 0.2,
    }
    result = _post_json(
        "https://api.groq.com/openai/v1/chat/completions",
        payload,
        {
            "Authorization": f"Bearer {settings.groq_api_key}",
            "Content-Type": "application/json",
        },
    )
    choices = result.get("choices", [])
    if not choices:
        return None
    return choices[0].get("message", {}).get("content")


def maybe_rewrite_narrative(prompt: str, draft: str, facts: dict) -> str:
    provider = get_settings().llm_provider
    if provider not in {"gemini", "groq"}:
        return draft

    try:
        rewritten = _gemini_rewrite(prompt, draft, facts) if provider == "gemini" else _groq_rewrite(prompt, draft, facts)
    except Exception:
        return draft

    if not rewritten:
        return draft
    return redact_text(rewritten).strip() or draft
