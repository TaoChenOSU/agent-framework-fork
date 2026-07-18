# Copyright (c) Microsoft. All rights reserved.

"""Workflow extraction — decompose a monolithic agent trace into a pipeline of sub-agents.

This script reads an OpenTelemetry trace (``spans.jsonl``) produced by a single,
powerful agent (see ``travel_agent.py``) and turns it into a *workflow* of smaller,
specialized sub-agents.

How it works:

* The trace has a root span, under which sits a single ``invoke_agent`` span.
* That ``invoke_agent`` span is the parent of every ``chat`` span (one per model
  turn) and every ``execute_tool`` span (one per tool the model invoked).
* Each ``chat`` span emits a batch of tool calls; the ``execute_tool`` spans that
  follow it — up to the next ``chat`` span — are exactly the tools that turn used.

So every ``chat`` call from the master agent becomes one sub-agent, and each
sub-agent is given **only** the tools that chat call actually used. The tool
implementations are the :class:`FunctionTool` objects defined in ``tools.py``.

The result is N sub-agents (N = number of chat calls) with a minimal tool
surface each, instead of the master agent's full toolkit. When Foundry
credentials are configured they are also wired into a sequential workflow.

Run:
    python workflow_extraction.py [path/to/spans.jsonl]
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import tools as tools_module
from agent_framework import FunctionTool
from agent_framework.observability import configure_otel_providers, get_tracer
from dotenv import load_dotenv
from opentelemetry.trace import SpanKind
from travel_agent import PROMPT

load_dotenv()


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------


def build_tool_registry() -> dict[str, FunctionTool]:
    """Return a ``{tool_name: FunctionTool}`` map for every tool defined in ``tools.py``.

    The tools are discovered by introspection so this stays in sync automatically
    when tools are added to or removed from ``tools.py``.
    """
    registry: dict[str, FunctionTool] = {}
    for obj in vars(tools_module).values():
        if isinstance(obj, FunctionTool):
            registry[obj.name] = obj
    return registry


# ---------------------------------------------------------------------------
# Span parsing
# ---------------------------------------------------------------------------


def load_spans(path: Path) -> list[dict[str, Any]]:
    """Load spans from a trace file.

    The file is written by ``ConsoleSpanExporter`` as a sequence of pretty-printed
    (multi-line) JSON objects, so it is not strictly line-delimited. This decodes
    the concatenated objects one after another.
    """
    text = path.read_text(encoding="utf-8")
    decoder = json.JSONDecoder()
    spans: list[dict[str, Any]] = []
    index = 0
    length = len(text)
    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break
        span, index = decoder.raw_decode(text, index)
        spans.append(span)
    return spans


def _span_id(span: dict[str, Any]) -> str | None:
    return span.get("context", {}).get("span_id")


def _parent_id(span: dict[str, Any]) -> str | None:
    return span.get("parent_id")


def _operation(span: dict[str, Any]) -> str | None:
    return span.get("attributes", {}).get("gen_ai.operation.name")


def find_invoke_agent_span(spans: list[dict[str, Any]]) -> dict[str, Any]:
    """Return the single ``invoke_agent`` span (the master agent's invocation)."""
    for span in spans:
        if str(span.get("name", "")).startswith("invoke_agent"):
            return span
    raise ValueError("No 'invoke_agent' span found in the trace.")


# ---------------------------------------------------------------------------
# Iteration grouping
# ---------------------------------------------------------------------------


@dataclass
class Iteration:
    """One master-agent turn: a ``chat`` call plus the tools it invoked."""

    index: int
    model: str | None
    tool_calls: list[str]
    reasoning: str | None


def _extract_reasoning(attributes: dict[str, Any]) -> str | None:
    """Return the model's reasoning text for a ``chat`` span, if it emitted any.

    The reasoning is carried in the ``gen_ai.output.messages`` attribute as a
    JSON string of assistant messages, each with ``parts`` that may include a
    ``reasoning`` part. Empty reasoning parts (common for pure tool-call turns)
    are ignored.
    """
    raw = attributes.get("gen_ai.output.messages")
    if not raw:
        return None
    try:
        messages = json.loads(raw)
    except json.JSONDecodeError:
        return None
    texts: list[str] = []
    for message in messages:
        for part in message.get("parts", []):
            if part.get("type") == "reasoning":
                content = part.get("content", "")
                if content:
                    texts.append(content)
    return "\n".join(texts) or None


def group_iterations(spans: list[dict[str, Any]], invoke_span: dict[str, Any]) -> list[Iteration]:
    """Group the master agent's child spans into per-``chat`` iterations.

    Child ``chat`` and ``execute_tool`` spans are walked in chronological order.
    Each ``chat`` span opens a new iteration; the ``execute_tool`` spans that
    follow it (until the next ``chat`` span) are the tools that turn used.
    """
    parent = _span_id(invoke_span)
    children = [span for span in spans if _parent_id(span) == parent and _operation(span) in {"chat", "execute_tool"}]
    children.sort(key=lambda span: span.get("start_time", ""))

    iterations: list[Iteration] = []
    current: Iteration | None = None
    for span in children:
        operation = _operation(span)
        attributes = span.get("attributes", {})
        if operation == "chat":
            current = Iteration(
                index=len(iterations) + 1,
                model=attributes.get("gen_ai.request.model"),
                tool_calls=[],
                reasoning=_extract_reasoning(attributes),
            )
            iterations.append(current)
        elif operation == "execute_tool" and current is not None:
            tool_name = attributes.get("gen_ai.tool.name")
            if tool_name:
                current.tool_calls.append(tool_name)
    return iterations


# ---------------------------------------------------------------------------
# Sub-agent specs
# ---------------------------------------------------------------------------


@dataclass
class SubAgentSpec:
    """A sub-agent extracted from one master-agent chat call."""

    name: str
    tools: list[FunctionTool]
    tool_names: list[str]
    external_tool_names: list[str]
    model: str | None
    reasoning: str | None


def _distinct(names: list[str]) -> list[str]:
    """Return names with duplicates removed, preserving first-seen order."""
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def build_instructions(spec: SubAgentSpec) -> str:
    """Build instructions for a sub-agent from its master-agent chat turn.

    The master agent's reasoning for that turn already describes exactly what the
    step is trying to do, so we reuse it verbatim as the sub-agent's instructions.
    Turns that emitted no reasoning (for example pure tool-call turns) fall back
    to a generic instruction to just use the tools it was given.
    """
    if spec.reasoning:
        return spec.reasoning
    return "Load the skill"


def build_sub_agent_specs(iterations: list[Iteration], registry: dict[str, FunctionTool]) -> list[SubAgentSpec]:
    """Turn iterations into sub-agent specs, resolving tool names against ``tools.py``.

    Tool names not found in the registry (for example the built-in ``load_skill``
    tool provided by a context provider rather than ``tools.py``) are reported
    separately as external tools.
    """
    specs: list[SubAgentSpec] = []
    for iteration in iterations:
        distinct = _distinct(iteration.tool_calls)
        resolved = [name for name in distinct if name in registry]
        external = [name for name in distinct if name not in registry]
        specs.append(
            SubAgentSpec(
                name=f"SubAgent{iteration.index:02d}",
                tools=[registry[name] for name in resolved],
                tool_names=resolved,
                external_tool_names=external,
                model=iteration.model,
                reasoning=iteration.reasoning,
            )
        )
    return specs


def extract_sub_agent_specs(trace_path: Path) -> list[SubAgentSpec]:
    """Parse a trace file and return the extracted sub-agent specs."""
    spans = load_spans(trace_path)
    invoke_span = find_invoke_agent_span(spans)
    iterations = group_iterations(spans, invoke_span)
    registry = build_tool_registry()
    return build_sub_agent_specs(iterations, registry)


# ---------------------------------------------------------------------------
# Reporting & (optional) workflow construction
# ---------------------------------------------------------------------------


def print_report(specs: list[SubAgentSpec], registry_size: int) -> None:
    """Print a human-readable summary of the extracted sub-agents."""
    print(f"Extracted {len(specs)} sub-agent(s) from the invoke_agent trace.")
    print(f"Master agent tool surface: {registry_size} tool(s).\n")
    for spec in specs:
        tools = ", ".join(spec.tool_names) if spec.tool_names else "(none)"
        line = f"  {spec.name}  [{len(spec.tool_names)} tool(s)]  {tools}"
        print(line)
        if spec.external_tool_names:
            print(f"    external (not in tools.py): {', '.join(spec.external_tool_names)}")


async def build_and_run_workflow(specs: list[SubAgentSpec]) -> None:
    """Chain the extracted sub-agents into a sequential workflow and run the travel prompt.

    Requires Foundry credentials (set ``FOUNDRY_PROJECT_ENDPOINT`` and run ``az login``).
    The shared conversation flows through each sub-agent in order; every sub-agent only has
    the tools its corresponding master-agent chat call used. Sub-agents that used the
    context-provider ``load_skill`` tool are given the skills provider so those turns keep
    working outside the master agent.
    """
    endpoint = os.environ.get("FOUNDRY_PROJECT_ENDPOINT")
    if not endpoint:
        print("\n(Set FOUNDRY_PROJECT_ENDPOINT and run 'az login' to run the workflow.)")
        return

    try:
        from agent_framework import Agent, AgentResponse, Message, SkillsProvider
        from agent_framework.foundry import FoundryChatClient
        from agent_framework.orchestrations import SequentialBuilder
        from azure.identity import AzureCliCredential
    except ImportError as exc:  # pragma: no cover - optional dependency path
        print(f"\n(Skipping workflow run; missing dependency: {exc})")
        return

    configure_otel_providers(enable_sensitive_data=True)

    client = FoundryChatClient(
        project_endpoint=endpoint,
        model="gpt-5.4-mini",  # Use a less powerful model for the sub-agents; the master agent used gpt-5.6-sol.
        credential=AzureCliCredential(),
    )

    skills_provider = SkillsProvider.from_paths(
        skill_paths=str(Path(__file__).parent / "skills"),
        disable_load_skill_approval=True,
    )

    agents = [
        Agent(
            client=client,
            name=spec.name,
            instructions=build_instructions(spec),
            tools=spec.tools,
            context_providers=[skills_provider] if "load_skill" in spec.external_tool_names else None,
        )
        for spec in specs
    ]

    with get_tracer().start_as_current_span("Travel Planning - Workflow", kind=SpanKind.CLIENT):
        workflow = SequentialBuilder(participants=agents, output_from="all").build()

        print(f"\nRunning the travel_agent.py prompt through {len(agents)} chained sub-agent(s)...\n")
        result = await workflow.run(PROMPT)

    conversation: list[Message] = [Message(role="user", contents=[PROMPT])]
    for output in result.get_outputs():
        response = cast(AgentResponse, output)
        conversation.extend(response.messages)

    print("===== Final Conversation =====")
    for index, message in enumerate(conversation, start=1):
        author = message.author_name or ("assistant" if message.role == "assistant" else "user")
        print(f"{'-' * 60}\n{index:02d} [{author}]\n{message.text}")


async def main(argv: list[str]) -> None:
    trace_path = Path(argv[1]) if len(argv) > 1 else Path(__file__).parent / "spans.jsonl"
    if not trace_path.exists():
        raise SystemExit(f"Trace file not found: {trace_path}")

    specs = extract_sub_agent_specs(trace_path)
    print_report(specs, registry_size=len(build_tool_registry()))
    await build_and_run_workflow(specs)


if __name__ == "__main__":
    asyncio.run(main(sys.argv))
