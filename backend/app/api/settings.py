"""Runtime credential settings API.

Lets the frontend read the current LLM/Neo4j credential configuration
(masked), write a partial override that hot-swaps into the running
process with no restart, and test-probe a candidate LLM key or Neo4j
connection before committing to it. See `services/settings_store.py` for
the persistence/propagation model this thinly wraps.

Response shape follows the rest of the API: `{success, data, error}`, plus
a top-level `warnings` array on the write endpoint for non-fatal issues
(an unknown key in the request body, a client that failed to close
cleanly during hot-swap) that don't make the request a failure.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

from flask import request, jsonify

from . import settings_bp
from ..config import Config
from ..services import settings_store
from ..utils.llm_client import LLMClient
from ..utils.logger import get_logger

logger = get_logger('mirofish.api.settings')

# Body field names for the /test probes are intentionally lowercase and
# distinct from the PUT body's uppercase Config-attribute-style keys: the
# probe body describes *candidate* values to try (falling back to whatever
# is currently saved for any field it omits), not a settings patch.
_LLM_TEST_FIELDS = ("llm_api_key", "llm_base_url", "llm_model_name")
_NEO4J_TEST_FIELDS = ("neo4j_uri", "neo4j_user", "neo4j_password")


@settings_bp.route('', methods=['GET'])
def get_settings():
    """Current effective settings: masked secret values, `set` booleans,
    and the override/env/default source of each of the seven editable
    keys. Never returns a raw secret."""

    return jsonify({
        "success": True,
        "data": settings_store.describe_settings(),
    })


@settings_bp.route('', methods=['PUT'])
def put_settings():
    """Accept a partial patch of the seven editable keys and hot-swap it
    into the running process. Never echoes a raw submitted value back."""

    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({
            "success": False,
            "error": "Request body must be a JSON object",
        }), 400

    unknown_types = [
        key for key, value in body.items()
        if value is not None and not isinstance(value, str)
    ]
    if unknown_types:
        return jsonify({
            "success": False,
            "error": (
                "Settings values must be strings or null: "
                f"{', '.join(sorted(unknown_types))}"
            ),
        }), 400

    result = settings_store.apply_and_propagate(body)
    return jsonify({
        "success": True,
        "data": result["settings"],
        "warnings": result["warnings"],
    })


def _classify_llm_test_error(error: Exception) -> Tuple[str, str]:
    """Map an LLM probe failure to a safe category + non-leaking message.

    Never includes the raw exception body/message (which some providers
    echo request content into) in the returned text -- only a generic,
    category-appropriate description. The full exception is logged
    server-side via `logger.exception` at the call site.
    """

    from openai import (
        AuthenticationError,
        PermissionDeniedError,
        RateLimitError,
        APIConnectionError,
        APITimeoutError,
        NotFoundError,
        BadRequestError,
    )

    if isinstance(error, (AuthenticationError, PermissionDeniedError)):
        return "auth", "Authentication failed: the API key was rejected by the provider"
    if isinstance(error, RateLimitError):
        return "quota", "Request rejected: rate limit or exhausted quota/credits"
    if isinstance(error, (APIConnectionError, APITimeoutError)):
        return "network", "Could not reach the LLM provider (connection or timeout)"
    if isinstance(error, NotFoundError):
        return "model", "The configured model was not found by the provider"

    # Credit exhaustion is NOT reliably a 429. The CommandCode proxy this
    # project points at returns HTTP 400 with "You have insufficient credits
    # to make this request" -- which would otherwise be reported as a model
    # /base-URL problem and send the user debugging the wrong thing, on the
    # single failure mode they hit most. Inspect the message (internally
    # only; the raw text is never returned) before falling back.
    if _looks_like_quota_exhaustion(error):
        return "quota", "Request rejected: exhausted quota/credits on the provider account"

    if isinstance(error, BadRequestError):
        return "model", "The provider rejected the request; check the model name/base URL"

    status_code = getattr(error, "status_code", None)
    if status_code == 401:
        return "auth", "Authentication failed: the API key was rejected by the provider"
    if status_code in (402, 429):
        return "quota", "Request rejected: rate limit or exhausted quota/credits"
    if status_code == 404:
        return "model", "The configured model was not found by the provider"

    return "unknown", "The connectivity test failed; check the server logs for details"


_QUOTA_MARKERS = (
    "insufficient credit",
    "insufficient_quota",
    "exceeded your current quota",
    "out of credits",
    "purchase more credits",
    "billing",
    "payment required",
    "quota exceeded",
)


def _looks_like_quota_exhaustion(error: Exception) -> bool:
    """Whether a provider error is really "you are out of money".

    Providers disagree wildly on how they signal this (402, 429, or a plain
    400 with prose), so fall back to matching the message text. Used only to
    pick a category -- the message itself is never echoed to the client.
    """

    if getattr(error, "status_code", None) == 402:
        return True
    text = str(error).lower()
    return any(marker in text for marker in _QUOTA_MARKERS)


@settings_bp.route('/test', methods=['POST'])
def test_llm():
    """Probe a candidate LLM key/base URL/model with one minimal
    completion. Any field omitted from the body falls back to the
    currently-saved value, so the user can test just a new key against
    the existing base URL/model.

    Always returns HTTP 200 with `success: true` -- a failed probe is a
    valid, informative result, not a malformed request. The failure (if
    any) is reported as `data.ok: false` with a safe `category`/`message`.
    """

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        body = {}

    candidate_key = body.get('llm_api_key')
    if candidate_key is not None and not isinstance(candidate_key, str):
        return jsonify({"success": False, "error": "llm_api_key must be a string"}), 400

    api_key = candidate_key or Config.LLM_API_KEY
    base_url = body.get('llm_base_url') or Config.LLM_BASE_URL
    model_name = body.get('llm_model_name') or Config.LLM_MODEL_NAME

    if not api_key:
        return jsonify({
            "success": True,
            "data": {
                "ok": False,
                "category": "auth",
                "message": "No API key configured or provided",
                "latency_ms": None,
            },
        })

    start = time.monotonic()
    try:
        client = LLMClient(api_key=api_key, base_url=base_url, model=model_name)
        client.chat(
            [{"role": "user", "content": "ping"}],
            temperature=0,
            max_tokens=5,
        )
    except Exception as error:
        latency_ms = int((time.monotonic() - start) * 1000)
        category, message = _classify_llm_test_error(error)
        logger.warning(
            "LLM settings test failed: type=%s category=%s",
            type(error).__name__,
            category,
        )
        logger.debug("LLM settings test failure detail", exc_info=True)
        return jsonify({
            "success": True,
            "data": {
                "ok": False,
                "category": category,
                "message": message,
                "latency_ms": latency_ms,
            },
        })

    latency_ms = int((time.monotonic() - start) * 1000)
    return jsonify({
        "success": True,
        "data": {"ok": True, "latency_ms": latency_ms},
    })


def _classify_neo4j_test_error(error: Exception) -> Tuple[str, str]:
    from neo4j.exceptions import AuthError, ServiceUnavailable, Neo4jError

    if isinstance(error, AuthError):
        return "auth", "Authentication failed: Neo4j rejected the username/password"
    if isinstance(error, ServiceUnavailable):
        return "network", "Could not reach the Neo4j server"
    if isinstance(error, Neo4jError):
        return "unknown", "Neo4j rejected the connection attempt"
    return "network", "Could not connect to Neo4j; check the server logs for details"


@settings_bp.route('/test-neo4j', methods=['POST'])
def test_neo4j():
    """Probe Neo4j connectivity with a standalone driver -- deliberately
    NOT through `app.utils.zep`'s process-wide cached client, so testing a
    candidate value never mutates the live client or its connection pool.
    """

    from neo4j import GraphDatabase

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        body = {}

    for field in _NEO4J_TEST_FIELDS:
        value = body.get(field)
        if value is not None and not isinstance(value, str):
            return jsonify({"success": False, "error": f"{field} must be a string"}), 400

    uri = body.get('neo4j_uri') or Config.NEO4J_URI
    user = body.get('neo4j_user') or Config.NEO4J_USER
    password = body.get('neo4j_password') or Config.NEO4J_PASSWORD

    if not uri:
        return jsonify({
            "success": True,
            "data": {
                "ok": False,
                "category": "unknown",
                "message": "No Neo4j URI configured or provided",
                "latency_ms": None,
            },
        })

    start = time.monotonic()
    driver = None
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
    except Exception as error:
        latency_ms = int((time.monotonic() - start) * 1000)
        category, message = _classify_neo4j_test_error(error)
        logger.warning(
            "Neo4j settings test failed: type=%s category=%s",
            type(error).__name__,
            category,
        )
        logger.debug("Neo4j settings test failure detail", exc_info=True)
        return jsonify({
            "success": True,
            "data": {
                "ok": False,
                "category": category,
                "message": message,
                "latency_ms": latency_ms,
            },
        })
    finally:
        if driver is not None:
            try:
                driver.close()
            except Exception:
                logger.exception("Failed to close standalone Neo4j test driver")

    latency_ms = int((time.monotonic() - start) * 1000)
    return jsonify({
        "success": True,
        "data": {"ok": True, "latency_ms": latency_ms},
    })
