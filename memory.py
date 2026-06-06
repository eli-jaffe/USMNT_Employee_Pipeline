"""
memory.py — Session state for one agent run

Keeps a structured log of every step so the agent can:
  - See what it has already tried (prevents repeating failed queries)
  - Pass full context to the reflect phase
  - Surface a clean trace for debugging

One AgentMemory instance is created per user question and discarded afterward.
Persistent cross-session memory is out of scope for v1.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from enum import Enum


class StepStatus(str, Enum):
    SUCCESS = "success"
    ERROR   = "error"
    SKIPPED = "skipped"


@dataclass
class StepRecord:
    """
    One completed (or failed) step in the execution plan.
    The agent reads these back verbatim when constructing retry prompts,
    so keep values serializable and human-readable.
    """
    step_number:  int
    description:  str               # what the plan said this step would do
    tool:         str               # tool name that was called
    tool_input:   dict              # arguments passed to the tool
    tool_output:  Any               # raw return value from the tool
    status:       StepStatus
    error_detail: str = ""          # populated only on StepStatus.ERROR


@dataclass
class AgentMemory:
    """
    Full state for one question → answer cycle.

    Attributes:
        question        The original user question, unchanged.
        plan            Ordered list of step descriptions from the Plan phase.
        steps           Completed StepRecords, in execution order.
        retry_count     How many times the loop has re-entered Plan after a failure.
        final_answer    Set by the Synthesize phase; None until then.
    """
    question:     str
    plan:         list[str]         = field(default_factory=list)
    steps:        list[StepRecord]  = field(default_factory=list)
    retry_count:  int               = 0
    final_answer: str | None        = None

    # ── Convenience accessors used by tools.py and agent.py ──────────────────

    def add_step(
        self,
        step_number: int,
        description: str,
        tool: str,
        tool_input: dict,
        tool_output: Any,
        status: StepStatus,
        error_detail: str = "",
    ) -> StepRecord:
        record = StepRecord(
            step_number=step_number,
            description=description,
            tool=tool,
            tool_input=tool_input,
            tool_output=tool_output,
            status=status,
            error_detail=error_detail,
        )
        self.steps.append(record)
        return record

    def failed_steps(self) -> list[StepRecord]:
        return [s for s in self.steps if s.status == StepStatus.ERROR]

    def successful_steps(self) -> list[StepRecord]:
        return [s for s in self.steps if s.status == StepStatus.SUCCESS]

    def last_error(self) -> StepRecord | None:
        failed = self.failed_steps()
        return failed[-1] if failed else None

    def failed_queries(self) -> list[str]:
        """Return all SQL strings that have already failed — used in retry prompts."""
        queries = []
        for step in self.failed_steps():
            if "sql" in step.tool_input:
                queries.append(step.tool_input["sql"])
        return queries

    def summary_for_prompt(self) -> str:
        """
        Compact text representation of completed steps.
        Injected into the Reflect and Retry prompts so the LLM
        has full context without re-running anything.
        """
        if not self.steps:
            return "No steps executed yet."

        lines = []
        for s in self.steps:
            status_tag = f"[{s.status.upper()}]"
            lines.append(f"Step {s.step_number} {status_tag} — {s.description}")
            lines.append(f"  Tool: {s.tool}")
            lines.append(f"  Input: {s.tool_input}")
            if s.status == StepStatus.ERROR:
                lines.append(f"  Error: {s.error_detail}")
            else:
                # Truncate large outputs so we don't blow the prompt
                output_str = str(s.tool_output)
                if len(output_str) > 500:
                    output_str = output_str[:500] + "... [truncated]"
                lines.append(f"  Output: {output_str}")
        return "\n".join(lines)