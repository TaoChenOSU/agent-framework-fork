---
name: itinerary-planner
description: Create a well-formatted, day-by-day travel itinerary. Use whenever the user asks to build, plan, or draft an itinerary for a destination.
license: MIT
compatibility: Works with any model that supports tool use.
metadata:
  author: agent-framework-samples
  version: "1.0"
---

## Purpose

Produce a polished, easy-to-scan travel itinerary in **Markdown**. The output
should read like something a professional travel agent would hand a client.

## Required structure

Always format the itinerary using this exact skeleton:

```markdown
# {Destination} — {N}-Day Itinerary

**Trip dates:** {start_date} – {end_date} · **Travelers:** {party_size} · **Weather:** {short weather note}

> A one- to two-sentence overview that sets the tone for the trip.

## Day 1 — {theme, e.g. "Arrival & Old Town"}

| Time      | Activity | Notes |
| --------- | -------- | ----- |
| Morning   | ...      | ...   |
| Afternoon | ...      | ...   |
| Evening   | ...      | ...   |

## Day 2 — {theme}

| Time      | Activity | Notes |
| --------- | -------- | ----- |
| Morning   | ...      | ...   |
| Afternoon | ...      | ...   |
| Evening   | ...      | ...   |

<!-- ...one "## Day N" section per day... -->

## Practical tips

- Getting around: ...
- What to pack: ...
- Budget note: ...
```

## Rules

1. Emit **one `## Day N` section per day** requested, each with a short theme in the heading.
2. Use the three-row **time table** (Morning / Afternoon / Evening) for every day.
3. Give each day a distinct focus — avoid repeating the same activity type on consecutive days.
4. Tailor activities to the traveler's stated interests (beaches, food, history, etc.).
5. If weather information is available, reflect it (e.g. indoor plans on rainy days) and fill in the **Weather** field in the header.
6. If any attractions have already been searched or booked, weave those specific names into the relevant days.
7. Keep each cell concise — a phrase, not a paragraph.
8. Close with a **Practical tips** section (getting around, what to pack, a budget note).
9. Return only the finished Markdown itinerary — no preamble or apologies.
