"""Graphiti LLM client wired to MiroFish's existing OpenAI-compatible proxy.

Graphiti's `OpenAIGenericClient` (graphiti_core/llm_client/
openai_generic_client.py) is the right base class for any OpenAI-compatible
`/chat/completions` endpoint, which is exactly what MiroFish's
`LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL_NAME` point at. However, its
`_generate_response` calls `self.client.chat.completions.create(...)`
directly with a fixed kwargs set and does not expose a hook for extra
request kwargs (confirmed by reading that file in full) -- so there is no
clean way to pass `LLM_REASONING_EFFORT` (as `extra_body`, per
backend/app/utils/openai_chat_compat.py) through to Graphiti's own LLM
calls without overriding it.

This subclass overrides `_generate_response` to route the request through
MiroFish's existing `openai_chat_compat.create_chat_completion` helper
instead of calling the SDK directly, so Graphiti's calls get the same
GPT-5-family compatibility handling and `reasoning_effort` passthrough as
MiroFish's own `utils/llm_client.py` calls. Everything else (message
cleanup, response_format construction, code-fence stripping, error
handling) is copied from the upstream method to stay behaviorally
equivalent; only the actual request call changes.
"""

from __future__ import annotations

import json
import logging
import typing
from typing import Union, get_args, get_origin

import openai
from graphiti_core.llm_client.config import DEFAULT_MAX_TOKENS, LLMConfig, ModelSize
from graphiti_core.llm_client.errors import EmptyResponseError, RateLimitError
from graphiti_core.llm_client.openai_generic_client import DEFAULT_MODEL, OpenAIGenericClient
from graphiti_core.prompts.models import Message
from pydantic import BaseModel

from .openai_chat_compat import create_chat_completion

logger = logging.getLogger(__name__)


class MiroFishGraphitiLLMClient(OpenAIGenericClient):
    """`OpenAIGenericClient` with MiroFish's reasoning_effort passthrough."""

    def __init__(
        self,
        config: LLMConfig | None = None,
        reasoning_effort: str | None = None,
        **kwargs: typing.Any,
    ):
        super().__init__(config=config, **kwargs)
        self._reasoning_effort = reasoning_effort

    async def _generate_response(
        self,
        messages: list[Message],
        response_model: type[BaseModel] | None = None,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        model_size: ModelSize = ModelSize.medium,
    ) -> dict[str, typing.Any]:
        openai_messages: list[dict[str, str]] = []
        for m in messages:
            m.content = self._clean_input(m.content)
            if m.role == "user":
                openai_messages.append({"role": "user", "content": m.content})
            elif m.role == "system":
                openai_messages.append({"role": "system", "content": m.content})
        try:
            response = await create_chat_completion(
                self.client,
                model=self.model or DEFAULT_MODEL,
                messages=openai_messages,
                temperature=self.temperature,
                max_tokens=max_tokens,
                response_format=self._build_response_format(response_model),
                reasoning_effort=self._reasoning_effort,
            )
            result = response.choices[0].message.content or ""
            if not result:
                raise EmptyResponseError("LLM returned an empty response")
            parsed = json.loads(self._strip_code_fences(result))
            if response_model is not None:
                parsed = self._sanitize_against_schema(parsed, response_model)
            return parsed
        except openai.RateLimitError as e:
            raise RateLimitError from e
        except Exception as e:
            logger.error(f"Error in generating LLM response: {e}")
            raise

    @staticmethod
    def _is_scalar_annotation(annotation: typing.Any) -> bool:
        """Return True if a Pydantic field annotation is a plain scalar type
        (str/int/float/bool, optionally wrapped in Optional[...])."""
        origin = get_origin(annotation)
        if origin is Union:
            args = [a for a in get_args(annotation) if a is not type(None)]
            return len(args) == 1 and MiroFishGraphitiLLMClient._is_scalar_annotation(
                args[0]
            )
        return annotation in (str, int, float, bool)

    @classmethod
    def _sanitize_against_schema(
        cls, parsed: dict[str, typing.Any], response_model: type[BaseModel]
    ) -> dict[str, typing.Any]:
        """Defend against a non-compliant `json_object`-mode response.

        Graphiti's own attribute-extraction path (`_extract_entity_attributes`
        in `graphiti_core/utils/maintenance/node_operations.py`) validates the
        LLM's response against our entity/edge Pydantic model for shape only,
        then discards that validated instance and writes the *raw* dict
        straight through to `node.attributes` -- which later becomes Neo4j
        node properties. Neo4j property values must be primitives (or arrays
        of one primitive type), not nested maps.

        In `json_schema` mode the API constrains the model to the declared
        shape, so this never comes up. In `json_object` mode (this proxy
        doesn't reliably honor `json_schema` -- see the comment where this
        client is constructed in `utils/zep.py`) the model can invent extra
        keys or nest an object where a flat scalar field was declared, and
        because Pydantic's default `extra='ignore'` behavior means the
        shape-check silently drops unknown keys from validation *without*
        removing them from the dict actually returned upstream, that garbage
        reaches Neo4j and the write fails.

        This only touches keys declared with a scalar (str/int/float/bool)
        annotation -- fields genuinely typed as lists/nested models (e.g.
        Graphiti's own multi-entity extraction schemas) are left untouched.
        """
        fields = response_model.model_fields
        sanitized: dict[str, typing.Any] = {}
        for key, value in parsed.items():
            field = fields.get(key)
            if field is None:
                # Not part of the declared schema -- Graphiti's shape check
                # would silently ignore it too; drop it before it reaches
                # node.attributes rather than let it reach Neo4j untyped.
                continue
            if isinstance(value, (dict, list)) and cls._is_scalar_annotation(
                field.annotation
            ):
                sanitized[key] = json.dumps(value, ensure_ascii=False)
            else:
                sanitized[key] = value
        return sanitized
