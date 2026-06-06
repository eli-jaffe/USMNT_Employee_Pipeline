"""
agent.py — The reason → plan → execute → reflect → synthesize loop

This is the orchestration layer. It:
  1. Calls the Anthropic API for reasoning steps
  2. Parses LLM responses to extract structured plans and tool calls
  3. Dispatches tool calls via tools.py
  4. Manages retry logic with a hard cap
  5. Returns a final natural-language answer

Design principle: the LLM reasons and plans; Python executes and validates.
Nothing that requires deterministic correctness (SQL execution, math) is left to the LLM.
"""
from __future__ import annotations

import json
import re
import anthropic

import db
# import tools
import agents.data_agent as DataAgent
import memory as mem
import prompts.prompts as prompts

from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────────────────────

MODEL          = "claude-opus-4-5"
MAX_TOKENS     = 2048
MAX_RETRIES    = 5   # full loop retries before giving up

# ── Anthropic client ───────────────────────────────────────────────────────────
load_dotenv()
client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from environment


# ── LLM call wrapper ───────────────────────────────────────────────────────────

def _llm(system: str, user: str) -> str:
    """
    Single-turn LLM call. Returns the text content of the response.
    All agent phases use this — keeping it simple makes debugging easy.
    """
    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return response.content[0].text.strip()


# ── Phase parsers ──────────────────────────────────────────────────────────────

def _parse_reason(text: str) -> dict:
    """Extract structured fields from the Reason phase response."""
    result = {}
    for field in ["INTENT", "COMPLEXITY", "TABLES_NEEDED", "AMBIGUITIES", "DATA_AVAILABLE"]:
        match = re.search(rf"^{field}:\s*(.+)$", text, re.MULTILINE)
        result[field] = match.group(1).strip() if match else ""
    return result


def _parse_plan(text: str) -> tuple[str, list[str]]:
    """
    Extract assumption and ordered steps from the Plan phase response.
    Returns (assumption_str, [step1, step2, ...])
    """
    assumption_match = re.search(r"^ASSUMPTION:\s*(.+)$", text, re.MULTILINE)
    assumption = assumption_match.group(1).strip() if assumption_match else "None"

    steps_block = re.search(r"STEPS:\s*\n(.+)", text, re.DOTALL)
    if not steps_block:
        return assumption, []

    raw_steps = steps_block.group(1).strip()
    # Split on lines that start a new numbered step
    step_pattern = re.split(r"\n(?=\d+\.)", raw_steps)
    steps = [s.strip() for s in step_pattern if s.strip()]
    return assumption, steps


def _parse_tool_call(text: str) -> dict | None:
    """
    Parse the JSON tool call from the Execute phase.
    Returns None if parsing fails (triggers a reflect/retry).
    """
    # Strip markdown code fences if the LLM added them
    text = re.sub(r"```(?:json)?\s*|\s*```", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _parse_reflect(text: str) -> dict:
    """Extract STATUS, REASON, and RETRY_PLAN from Reflect phase response."""
    result = {}
    for field in ["STATUS", "REASON", "RETRY_PLAN"]:
        match = re.search(rf"^{field}:\s*(.+?)(?=\n[A-Z_]+:|$)", text, re.MULTILINE | re.DOTALL)
        result[field] = match.group(1).strip() if match else ""
    return result


# ── Main agent entry point ─────────────────────────────────────────────────────

def run(question: str) -> str:
    """
    Run the full agent loop for a single user question.
    Returns the final natural-language answer string.
    """
    session = mem.AgentMemory(question=question)
    schema_str = _get_schema_str()

    for attempt in range(MAX_RETRIES + 1):
        session.retry_count = attempt

        if attempt > 0:
            _print_phase(f"Retry attempt {attempt}/{MAX_RETRIES}")

        # ── 1. Reason ──────────────────────────────────────────────────────────
        _print_phase("Reasoning about question...")
        reason_prompt_text = prompts.reason_prompt(question, schema_str)
        reason_output_raw = _llm(
            system="You are a precise data analyst. Follow the output format exactly.",
            user=reason_prompt_text,
        )
        reason_output = _parse_reason(reason_output_raw)
        _print_debug("Reason", reason_output_raw)

        # Hard stop if data clearly doesn't exist
        if reason_output.get("DATA_AVAILABLE") == "NO":
            return prompts.cannot_answer_message(
                question,
                reason=f"Data not available. {reason_output.get('AMBIGUITIES', '')}",
            )

        # ── 2. Plan ────────────────────────────────────────────────────────────
        _print_phase("Building execution plan...")
        plan_prompt_text = prompts.plan_prompt(question, schema_str, reason_output_raw)
        plan_output_raw = _llm(
            system="You are a precise SQL planner. Follow the output format exactly.",
            user=plan_prompt_text,
        )
        assumption, step_list = _parse_plan(plan_output_raw)
        session.plan = step_list
        _print_debug("Plan", plan_output_raw)

        if not step_list:
            if attempt == MAX_RETRIES:
                return prompts.cannot_answer_message(question, "Could not generate a valid execution plan.")
            continue

        # ── 3. Execute ─────────────────────────────────────────────────────────
        _print_phase(f"Executing {len(step_list)} step(s)...")
        execution_succeeded = True

        for step_number, step_description in enumerate(step_list, start=1):
            _print_phase(f"  Step {step_number}: {step_description[:80]}...")

            # Ask LLM to produce the concrete tool call for this step
            execute_prompt_text = prompts.execute_step_prompt(
                question=question,
                plan_step=step_description,
                memory_summary=session.summary_for_prompt(),
                schema_str=schema_str,
            )
            tool_call_raw = _llm(
                system="You are a precise tool-call generator. Respond with valid JSON only.",
                user=execute_prompt_text,
            )
            _print_debug(f"Step {step_number} tool call", tool_call_raw)

            tool_call = _parse_tool_call(tool_call_raw)
            if not tool_call:
                session.add_step(
                    step_number=step_number,
                    description=step_description,
                    tool="(parse failed)",
                    tool_input={},
                    tool_output=None,
                    status=mem.StepStatus.ERROR,
                    error_detail=f"Could not parse tool call JSON: {tool_call_raw[:200]}",
                )
                execution_succeeded = False
                break

            tool_name  = tool_call.get("tool", "")
            tool_input = tool_call.get("input", {})

            # Handle clarify tool — updates question context, continues
            if tool_name == "clarify":
                result = DataAgent.dispatch("clarify", tool_input)
                if result["success"]:
                    clarification = result["data"]["clarification"]
                    # Append clarification to question for subsequent phases
                    question = f"{question} (Clarification: {clarification})"
                    session.add_step(
                        step_number=step_number,
                        description=step_description,
                        tool="clarify",
                        tool_input=tool_input,
                        tool_output=clarification,
                        status=mem.StepStatus.SUCCESS,
                    )
                    continue

            # Dispatch all other DataAgent
            result = DataAgent.dispatch(tool_name, tool_input)

            if result["success"]:
                session.add_step(
                    step_number=step_number,
                    description=step_description,
                    tool=tool_name,
                    tool_input=tool_input,
                    tool_output=result["data"],
                    status=mem.StepStatus.SUCCESS,
                )
                _print_phase(f"  ✓ Step {step_number} succeeded")
            else:
                session.add_step(
                    step_number=step_number,
                    description=step_description,
                    tool=tool_name,
                    tool_input=tool_input,
                    tool_output=None,
                    status=mem.StepStatus.ERROR,
                    error_detail=result["error"],
                )
                _print_phase(f"  ✗ Step {step_number} failed: {result['error']}")
                execution_succeeded = False
                break  # stop execution, enter reflect

        # ── 4. Reflect ─────────────────────────────────────────────────────────
        _print_phase("Reflecting on results...")
        reflect_prompt_text = prompts.reflect_prompt(
            question=question,
            memory_summary=session.summary_for_prompt(),
            failed_queries=session.failed_queries(),
        )
        reflect_output_raw = _llm(
            system="You are a critical quality reviewer. Follow the output format exactly.",
            user=reflect_prompt_text,
        )
        reflect_output = _parse_reflect(reflect_output_raw)
        _print_debug("Reflect", reflect_output_raw)

        status = reflect_output.get("STATUS", "").upper()

        if status == "SUCCESS":
            # ── 5. Synthesize ──────────────────────────────────────────────────
            _print_phase("Synthesizing answer...")
            synthesize_prompt_text = prompts.synthesize_prompt(
                question=question,
                memory_summary=session.summary_for_prompt(),
                assumption=assumption,
            )
            answer = _llm(
                system="You are a knowledgeable soccer analyst. Be accurate, clear, and concise.",
                user=synthesize_prompt_text,
            )
            session.final_answer = answer
            return answer

        elif status == "CANNOT_ANSWER":
            return prompts.cannot_answer_message(
                question,
                reason=reflect_output.get("REASON", "Insufficient data to answer confidently."),
            )

        elif status == "RETRY":
            if attempt == MAX_RETRIES:
                return prompts.cannot_answer_message(
                    question,
                    reason=f"Reached maximum retry limit ({MAX_RETRIES}). Last issue: {reflect_output.get('REASON', '')}",
                )
            # Retry plan is embedded in reflect output — next loop iteration
            # will re-reason with the full memory trace visible
            _print_phase(f"  Retrying: {reflect_output.get('RETRY_PLAN', '')[:120]}")
            continue

        else:
            # Malformed reflect response — treat as retry
            if attempt == MAX_RETRIES:
                return prompts.cannot_answer_message(question, "Unexpected error in reflection phase.")
            continue

    return prompts.cannot_answer_message(question, "Maximum retries exceeded.")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_schema_str() -> str:
    """Fetch and format schema once per question."""
    result = DataAgent.schema_lookup()
    if result["success"]:
        return result["data"]["formatted"]
    return "(Schema unavailable)"


# Flip this to True to see raw LLM outputs at each phase — helpful during development
DEBUG = False

def _print_phase(msg: str) -> None:
    print(f"  {msg}")

def _print_debug(label: str, content: str) -> None:
    if DEBUG:
        print(f"\n  --- {label} ---\n{content}\n  ---\n")