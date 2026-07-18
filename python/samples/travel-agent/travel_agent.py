# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os
from pathlib import Path

from agent_framework import Agent, Content, SkillsProvider
from agent_framework.foundry import FoundryChatClient
from agent_framework.observability import configure_otel_providers, get_tracer
from azure.identity import AzureCliCredential
from dotenv import load_dotenv
from opentelemetry import trace
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.trace import SpanKind
from tools import (
    book_attraction,
    book_flight,
    book_hotel,
    get_flight_price,
    get_hotel_price,
    get_weather,
    list_booking_vendors,
    make_restaurant_reservation,
    search_attractions,
    search_flights,
    search_hotels,
    suggest_destinations,
)

"""
Travel Agent — Plan trips end to end

This sample builds a single agent that can suggest vacation ideas, check the
weather, search live availability and pricing for flights, hotels, and
attractions, cross-compare candidate packages to find the best value, and
make (fake) reservations. The tools live in ``tools.py`` and return canned —
but internally consistent — data so the sample runs without any real backend.

Itinerary creation is handled by a file-based Agent Skill
(``skills/itinerary-planner/SKILL.md``) rather than a hard-coded tool: the
agent loads the skill on demand and uses its formatting rules to produce a
polished, well-structured Markdown itinerary.

After each turn it prints *everything* the agent did — reasoning, each tool
call with its arguments, each tool result, and the final answer — so you can
see the agent's decision process turn by turn.

Prerequisites:
    export FOUNDRY_PROJECT_ENDPOINT="https://your-project-endpoint"
    export FOUNDRY_MODEL="gpt-4o"   # optional, defaults to gpt-4o
"""

PROMPT = (
    "My wife and I want a trip to Kyoto. "
    "We'll fly from Seattle, departing 2026-09-14. We'll return on 2026-09-19 local time. "
    "Our budget is around $5,000. "
    "Book the round trip flight tickets, hotel, and attractions through the cheapest vendor, plus "
    "dinner reservations for 2 each evening. Use your best judgement to pick the best options for us. "
    "Finally, put together a well-formatted 5-day itinerary that weaves in everything we booked."
)


def render_content(content: Content) -> str | None:
    """Turn a single content item into a human-readable trace line.

    Returns None for content that carries no useful detail to print.
    """
    if content.type == "text_reasoning":
        return f"  [reasoning] {content.text}" if content.text else None
    if content.type == "function_call":
        return f"  [tool call] {content.name}({content.arguments})"
    if content.type == "function_result":
        return f"  [tool result] {content.result}"
    if content.type == "usage" and content.usage_details is not None:
        return f"  [usage] {content.usage_details}"
    if content.type == "error":
        return f"  [error] {content.message}"
    return None


load_dotenv()


async def main() -> None:
    # Export every span to a local JSONL file (one JSON object per line) so the
    # full trace can be inspected after the run. This is registered in addition
    # to any exporters configured via standard OTEL_* environment variables.
    span_file_path = Path(__file__).parent / "spans.jsonl"
    span_file = span_file_path.open("w", encoding="utf-8")
    configure_otel_providers(
        enable_sensitive_data=True,
        exporters=[ConsoleSpanExporter(out=span_file)],
    )

    client = FoundryChatClient(
        project_endpoint=os.environ.get("FOUNDRY_PROJECT_ENDPOINT"),
        model=os.environ.get("FOUNDRY_MODEL"),
        credential=AzureCliCredential(),
    )

    # Discover the file-based itinerary-planner skill. The agent advertises it
    # in the system prompt and loads its full instructions on demand via the
    # load_skill tool when the user asks for an itinerary.
    skills_provider = SkillsProvider.from_paths(
        skill_paths=str(Path(__file__).parent / "skills"),
        disable_load_skill_approval=True,
    )

    async with Agent(
        client=client,
        name="TravelAgent",
        instructions=(
            "You are a helpful travel agent that plans trips end to end. "
            "Ask brief clarifying questions when dates, party size, or budget are missing. "
            "Recommend destinations and time outdoor activities around clear weather. "
            "Compare vendors for each flight and hotel and choose the cheapest suitable offer. "
            "Schedule attractions only when they are open on the visit date. "
            "Compare a few candidate packages to find the best value. "
            "Automatically book the flights, hotel, attractions, and dinner reservations. "
            "Produce a polished, well-formatted itinerary at the end."
        ),
        tools=[
            suggest_destinations,
            get_weather,
            list_booking_vendors,
            search_flights,
            search_hotels,
            search_attractions,
            get_flight_price,
            get_hotel_price,
            # compare_trip_packages,
            book_flight,
            book_hotel,
            book_attraction,
            make_restaurant_reservation,
        ],
        context_providers=[skills_provider],
        # Ask the (reasoning-capable) model to return a summary of its reasoning
        # so the trace can show [reasoning] lines. These options are provider
        # specific and passed through as-is to the Responses API.
        default_options={"reasoning": {"effort": "medium", "summary": "detailed"}},
    ) as agent:
        session = agent.create_session()

        print(f"\n{'=' * 70}\nUser: {PROMPT}\n{'-' * 70}")
        with get_tracer().start_as_current_span("Travel Planning - Agent", kind=SpanKind.CLIENT):
            response = await agent.run(PROMPT, session=session)

        # Print the full trace: reasoning, tool calls, tool results, then the answer.
        for message in response.messages:
            for content in message.contents:
                line = render_content(content)
                if line is not None:
                    print(line)
        print(f"TravelAgent: {response}")

        # In non-streaming mode, token usage is aggregated on the response itself.
        if response.usage_details is not None:
            print(f"  [usage] {response.usage_details}")

    # Force the batch span processor to flush all buffered spans to the file.
    # We intentionally do NOT close the file here: the SDK exports spans on a
    # background thread and also flushes again via an atexit shutdown hook, so
    # closing it now would cause "write to closed file" errors. The process will
    # close the handle on exit.
    trace.get_tracer_provider().force_flush()  # type: ignore[attr-defined]
    print(f"\nSpans written to {span_file_path}")


if __name__ == "__main__":
    asyncio.run(main())
