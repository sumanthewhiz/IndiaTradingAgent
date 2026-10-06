from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request

from .broker import NoRedirect
from .core import AIConfig, SafetyError

GEMINI_MODEL = "gemini-3.1-pro-preview"
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"


def provider_kind(endpoint: str) -> str:
    parsed = urllib.parse.urlparse(endpoint)
    if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.query
            or parsed.fragment or parsed.port not in (None, 443)):
        raise SafetyError("AI requests require an approved HTTPS endpoint without URL credentials.")
    if parsed.hostname == "api.openai.com" and parsed.path == "/v1/chat/completions":
        return "openai"
    if parsed.hostname == "generativelanguage.googleapis.com":
        if parsed.path == "/v1beta/openai/chat/completions":
            return "gemini-compatible"
        if re.fullmatch(r"/v1beta/models/gemini-[a-z0-9.-]+:generateContent", parsed.path):
            return "gemini-native"
    raise SafetyError("AI endpoint is not an approved OpenAI or Google Gemini endpoint.")


def request_payload(config: AIConfig, messages: list[dict]) -> bytes:
    kind = provider_kind(config.endpoint)
    if kind == "gemini-native":
        path_model = urllib.parse.urlparse(config.endpoint).path.split("/")[-1].split(":")[0]
        if path_model != config.model:
            raise SafetyError("Gemini model and native endpoint do not match.")
        payload = {
            "systemInstruction": {"parts": [{"text": messages[0]["content"]}]},
            "contents": [{"role": "user", "parts": [{"text": messages[1]["content"]}]}],
            "generationConfig": {
                "maxOutputTokens": config.max_output_tokens,
                "thinkingConfig": {"thinkingLevel": "low"},
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "OBJECT", "properties": {"pause_minutes": {"type": "INTEGER"}},
                    "required": ["pause_minutes"],
                },
            },
        }
    else:
        payload = {
            "model": config.model, "messages": messages,
            "max_completion_tokens": config.max_output_tokens,
            "response_format": {"type": "json_object"},
        }
        if kind == "gemini-compatible":
            payload.pop("max_completion_tokens")
            payload["max_tokens"] = config.max_output_tokens
            payload["reasoning_effort"] = "low"
    return json.dumps(payload).encode()


def request_headers(endpoint: str, key: str) -> dict[str, str]:
    if provider_kind(endpoint) == "gemini-native":
        return {"x-goog-api-key": key, "Content-Type": "application/json"}
    return {"Authorization": "Bearer " + key, "Content-Type": "application/json"}


def parse_pause(endpoint: str, response: bytes) -> int:
    parsed = json.loads(response)
    if not isinstance(parsed, dict):
        raise ValueError("AI response must be an object.")
    if provider_kind(endpoint) == "gemini-native":
        candidate = parsed["candidates"][0]
        if not isinstance(candidate, dict):
            raise ValueError("Invalid Gemini candidate.")
        if candidate.get("finishReason") != "STOP":
            raise ValueError("Incomplete/blocked Gemini generation.")
        parts = candidate["content"]["parts"]
        if not isinstance(parts, list) or not all(isinstance(part, dict) for part in parts):
            raise ValueError("Invalid Gemini content parts.")
        text = "".join(part.get("text", "") for part in parts if not part.get("thought"))
    else:
        choice = parsed["choices"][0]
        if not isinstance(choice, dict):
            raise ValueError("Invalid completion choice.")
        if choice.get("finish_reason") not in (None, "stop"):
            raise ValueError("Incomplete/blocked generation.")
        text = choice["message"]["content"]
    value = json.loads(text)
    if (not isinstance(value, dict) or set(value) != {"pause_minutes"}
            or type(value["pause_minutes"]) is not int or not 0 <= value["pause_minutes"] <= 60):
        raise ValueError("AI response is not a bounded pause decision.")
    return value["pause_minutes"]


def verify_gemini_key(key: str) -> None:
    """Metadata-only authentication check; no billable generation or brokerage information."""
    request = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}",
        headers={"x-goog-api-key": key},
    )
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=10) as response:
            body = response.read(65537)
        if len(body) > 65536:
            raise SafetyError("Google model metadata exceeded the response limit.")
        model = json.loads(body)
        if "generateContent" not in model.get("supportedGenerationMethods", []):
            raise SafetyError("The configured Gemini Pro model is not available for this key.")
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        raise SafetyError("Gemini key/model verification failed. Check the key, project permissions and API billing.") from None
