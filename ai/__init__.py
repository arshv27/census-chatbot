"""AI layer for LLM interactions via Snowflake Cortex."""
from ai.cortex_llm import call_cortex, MODEL_SQL, MODEL_CONVERSATIONAL
from ai.prompts import (
    UNIFIED_AGENT_PROMPT,
    SYNTHESIS_PROMPT,
    REASONING_PROMPT,
    SELF_CORRECTION_PROMPT,
)

__all__ = [
    "call_cortex",
    "MODEL_SQL",
    "MODEL_CONVERSATIONAL",
    "UNIFIED_AGENT_PROMPT",
    "SYNTHESIS_PROMPT",
    "REASONING_PROMPT",
    "SELF_CORRECTION_PROMPT",
]
