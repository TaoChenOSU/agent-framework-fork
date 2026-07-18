# Copyright (c) Microsoft. All rights reserved.

"""Tools for the Travel Agent sample.

These tools return canned — but internally consistent — data so the sample
runs end to end without any real booking backend. Search results are seeded
by their inputs so repeated calls for the same destination return stable
prices and options, which lets the agent cross-compare and reason about the
best combination of flights, hotels, and attractions within a budget.

Booking tools that would spend money in the real world use
``approval_mode="never_require"`` here purely to keep the sample
non-interactive; in production those should use ``"always_require"``.
"""

import hashlib
import json
import math
from collections.abc import Sequence
from datetime import datetime
from random import Random
from typing import Annotated, Any, cast
from uuid import uuid4

from agent_framework import tool
from pydantic import Field


def _seed(*parts: str) -> Random:
    """Return a deterministic RNG seeded from the given strings.

    Using a stable seed means the same destination/date always yields the same
    prices and options, so the agent can compare results across tool calls.
    """
    digest = hashlib.sha256("|".join(parts).lower().encode()).hexdigest()
    return Random(int(digest[:16], 16))


# Booking marketplaces the sample can quote against, with their public domains.
_VENDOR_DOMAINS: dict[str, str] = {
    "Expedia": "expedia.com",
    "Booking.com": "booking.com",
    "Hotels.com": "hotels.com",
    "Agoda": "agoda.com",
    "Kayak": "kayak.com",
    "Priceline": "priceline.com",
}
_FLIGHT_MARKETPLACES = ["Expedia", "Kayak", "Priceline"]
_HOTEL_MARKETPLACES = ["Booking.com", "Expedia", "Hotels.com", "Agoda"]


def _slug(name: str) -> str:
    """Reduce a display name to a lowercase alphanumeric slug for fake URLs."""
    return "".join(c for c in name.lower() if c.isalnum())


# The special vendor name meaning "the provider's own website" (airline or hotel site).
_DIRECT = "Direct"


def _resolve_vendor(requested: str | None, rng: Random, marketplaces: Sequence[str]) -> str:
    """Resolve the single vendor a search is priced against.

    ``requested`` is one vendor name — a marketplace (e.g. ``Expedia``) or ``Direct`` for
    the provider's own website. When it is empty or unrecognized, a vendor is chosen at
    random from the marketplaces plus the direct option, so the provider's own site is
    only quoted some of the time.
    """
    options = [*marketplaces, _DIRECT]
    if requested:
        wanted = requested.strip().lower()
        for vendor in options:
            if vendor.lower() == wanted:
                return vendor
        if wanted in {"airline", "hotel", "airline site", "hotel site", "own", "provider"}:
            return _DIRECT
    return rng.choice(options)


def _quote(rng: Random, unit_price: int, quantity: int, is_direct: bool) -> dict[str, Any]:
    """Compute one vendor's price breakdown (unit price, taxes/fees, total, cancellation).

    The base price varies a little per vendor; direct provider sites are slightly more
    likely to offer free cancellation.
    """
    unit = max(1, round(unit_price * rng.uniform(0.93, 1.10)))
    subtotal = unit * quantity
    taxes = round(subtotal * rng.uniform(0.08, 0.22))
    return {
        "unit_price_usd": unit,
        "taxes_fees_usd": taxes,
        "total_usd": subtotal + taxes,
        "free_cancellation": rng.random() < (0.7 if is_direct else 0.45),
    }


def _base_price(identity: str, low: int, high: int) -> int:
    """Return a stable base price for an item, independent of the vendor.

    Seeding on the item's identity alone means every vendor prices the *same*
    underlying flight or hotel, differing only by each vendor's markup, so the
    agent can meaningfully compare the same item across vendors.
    """
    return _seed("base-price", identity).randint(low, high)


def _vendor_carries(identity: str, vendor: str, is_direct: bool) -> bool:
    """Decide whether a vendor offers a given item.

    The provider's own site (``Direct``) always carries it; marketplaces stock
    roughly 70% of items, so not every vendor offers every flight or hotel.
    The choice is seeded on (item, vendor) so it stays consistent across calls.
    """
    if is_direct:
        return True
    return _seed("carries", identity, vendor).random() < 0.7


_WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Real, well-known attractions for the destinations suggest_destinations recommends.
# Keyed by a lowercase city token matched against the destination string.
_ATTRACTIONS: dict[str, list[tuple[str, str]]] = {
    "lisbon": [
        ("Belém Tower", "history"),
        ("Jerónimos Monastery", "history"),
        ("São Jorge Castle", "history"),
        ("Time Out Market Lisboa", "food"),
        ("Tram 28 Sightseeing Ride", "scenic"),
        ("Oceanário de Lisboa", "nature"),
        ("LX Factory", "culture"),
    ],
    "kyoto": [
        ("Fushimi Inari Shrine", "history"),
        ("Kinkaku-ji (Golden Pavilion)", "history"),
        ("Kiyomizu-dera Temple", "history"),
        ("Arashiyama Bamboo Grove", "nature"),
        ("Nishiki Market Food Tour", "food"),
        ("Gion District Evening Walk", "culture"),
        ("Traditional Tea Ceremony", "culture"),
    ],
    "barcelona": [
        ("Sagrada Família", "culture"),
        ("Park Güell", "scenic"),
        ("Casa Batlló", "culture"),
        ("Gothic Quarter Walking Tour", "history"),
        ("La Boqueria Market Tour", "food"),
        ("Montjuïc Cable Car", "scenic"),
        ("Spotify Camp Nou Tour", "adventure"),
    ],
    "queenstown": [
        ("Skyline Gondola & Luge", "scenic"),
        ("Shotover Jet", "adventure"),
        ("Milford Sound Day Trip", "nature"),
        ("Kawarau Bridge Bungy", "adventure"),
        ("TSS Earnslaw Steamship Cruise", "scenic"),
        ("Gibbston Valley Wine Tour", "food"),
        ("Ben Lomond Track Hike", "adventure"),
    ],
    "marrakech": [
        ("Jemaa el-Fnaa Square", "culture"),
        ("Bahia Palace", "history"),
        ("Majorelle Garden", "nature"),
        ("Medina Souks Guided Tour", "food"),
        ("Koutoubia Mosque Gardens", "history"),
        ("Agafay Desert Excursion", "adventure"),
        ("Moroccan Cooking Class", "food"),
    ],
}

# Fallback catalog for destinations without a curated list.
_GENERIC_ATTRACTIONS: list[tuple[str, str]] = [
    ("Historic Old Town Walking Tour", "history"),
    ("Local Food & Market Tour", "food"),
    ("Landmark Museum Skip-the-Line", "culture"),
    ("Sunset Boat Cruise", "scenic"),
    ("Guided Cycling Adventure", "adventure"),
    ("Cooking Class with a Local Chef", "food"),
    ("Panoramic Viewpoint Cable Car", "scenic"),
    ("Day Trip to Nearby Highlights", "nature"),
]


def _match_city(destination: str) -> str | None:
    """Return the curated-catalog key whose city name appears in the destination string."""
    lowered = destination.lower()
    for key in _ATTRACTIONS:
        if key in lowered:
            return key
    return None


def _attraction_schedule(destination: str, name: str) -> tuple[str, str, list[str]]:
    """Return an attraction's (opening_time, closing_time, closed_days) deterministically.

    Times and any weekly closure are seeded from the attraction so repeated calls stay
    consistent. Roughly half of attractions close one weekday (often Monday, like many
    museums); the rest open daily.
    """
    rng = _seed("attraction-schedule", destination, name)
    open_h = rng.choice([8, 9, 9, 10, 10, 11])
    open_m = rng.choice([0, 0, 0, 30])
    close_h = rng.choice([16, 17, 18, 18, 19, 20, 21, 22])
    opening = f"{open_h:02d}:{open_m:02d}"
    closing = f"{close_h:02d}:00"
    if rng.random() < 0.5:
        closed_day = "Monday" if rng.random() < 0.5 else rng.choice(_WEEKDAYS)
        closed_days = [closed_day]
    else:
        closed_days = []
    return opening, closing, closed_days


# ---------------------------------------------------------------------------
# Discovery & planning
# ---------------------------------------------------------------------------


@tool(
    approval_mode="never_require",
    description="Suggest vacation destinations matching the traveler's interests and travel month.",
)
def suggest_destinations(
    interests: Annotated[str, Field(description="Traveler interests, e.g. 'beaches, food, history'.")],
    month: Annotated[str, Field(description="The month of travel, e.g. 'June'.")],
) -> str:
    """Suggest vacation destinations that match the traveler's interests and travel month."""
    ideas = [
        ("Lisbon, Portugal", "coastal city with historic neighborhoods and great seafood"),
        ("Kyoto, Japan", "temples, gardens, and traditional cuisine"),
        ("Barcelona, Spain", "beaches, Gaudí architecture, and tapas"),
        ("Queenstown, New Zealand", "adventure sports and dramatic landscapes"),
        ("Marrakech, Morocco", "vibrant markets, history, and desert excursions"),
    ]
    rng = _seed(interests, month)
    picks = rng.sample(ideas, k=3)
    lines = [f"- {name}: {why}" for name, why in picks]
    return f"Based on interests '{interests}' in {month}, consider:\n" + "\n".join(lines)


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------


@tool(
    approval_mode="never_require",
    description="Get a weather forecast for a destination on an exact date (whole-day summary or a specific hour).",
)
def get_weather(
    destination: Annotated[str, Field(description="City or region to check, e.g. 'Barcelona, Spain'.")],
    date: Annotated[str, Field(description="Exact date in ISO format, e.g. '2026-06-14'.")],
    hour: Annotated[
        int | None,
        Field(description="Optional exact hour in 24-hour format (0-23). Omit for a whole-day summary.", ge=0, le=23),
    ] = None,
) -> str:
    """Get a weather forecast for a destination on an exact date.

    Provide ``hour`` for an hour-specific forecast (e.g. to place indoor activities
    during rain or schedule a sunset cruise); call it once per hour you want to plan
    in detail so the trip can be scheduled hour by hour. Omit ``hour`` to get a
    whole-day summary (highs/lows, overall conditions, chance of rain, sun times).
    """
    # A stable per-day seed drives the daily temperature envelope; the hour seed
    # adds hour-specific variation so repeated calls stay consistent.
    day_rng = _seed("weather-day", destination, date)
    high_c = day_rng.randint(18, 34)
    low_c = high_c - day_rng.randint(6, 12)

    if hour is None:
        # Whole-day summary.
        day_precip_prob = day_rng.randint(0, 90)
        if day_precip_prob > 65:
            condition = day_rng.choice(["rainy", "showers", "thunderstorms"])
            precip_mm = round(day_rng.uniform(2.0, 30.0), 1)
        elif day_precip_prob > 35:
            condition = day_rng.choice(["partly cloudy", "cloudy", "overcast"])
            precip_mm = round(day_rng.uniform(0.0, 2.0), 1)
        else:
            condition = "mostly sunny"
            precip_mm = 0.0
        summary = {
            "destination": destination,
            "date": date,
            "scope": "day",
            "high_c": high_c,
            "low_c": low_c,
            "high_f": round(high_c * 9 / 5 + 32),
            "low_f": round(low_c * 9 / 5 + 32),
            "conditions": condition,
            "precip_probability_pct": day_precip_prob,
            "precip_mm": precip_mm,
            "humidity_pct": day_rng.randint(40, 85),
            "max_wind_kph": day_rng.randint(8, 40),
            "uv_index_max": day_rng.randint(1, 11),
            "sunrise": f"{day_rng.randint(5, 7):02d}:{day_rng.choice(['00', '15', '30', '45'])}",
            "sunset": f"{day_rng.randint(19, 21):02d}:{day_rng.choice(['00', '15', '30', '45'])}",
        }
        return json.dumps(summary)

    # Hour-specific forecast.
    hour_rng = _seed("weather-hour", destination, date, str(hour))
    mid = (high_c + low_c) / 2
    amplitude = (high_c - low_c) / 2
    # Diurnal curve: coldest around 03:00, warmest around 15:00.
    temp_c = mid - amplitude * math.cos((hour - 15) / 24 * 2 * math.pi)

    humidity = hour_rng.randint(35, 95)
    wind_kph = hour_rng.randint(3, 38)
    feels_like_c = temp_c - (wind_kph / 25) + (humidity - 50) / 25
    is_daylight = 7 <= hour <= 20
    precip_prob = hour_rng.randint(0, 90)
    if precip_prob > 65:
        condition = hour_rng.choice(["light rain", "rain showers", "thunderstorm"])
        precip_mm = round(hour_rng.uniform(0.5, 12.0), 1)
    elif precip_prob > 35:
        condition = hour_rng.choice(["partly cloudy", "cloudy", "overcast"])
        precip_mm = round(hour_rng.uniform(0.0, 0.6), 1)
    else:
        condition = "clear" if is_daylight else "clear night"
        precip_mm = 0.0

    forecast = {
        "destination": destination,
        "date": date,
        "scope": "hour",
        "hour": hour,
        "local_time": f"{hour:02d}:00",
        "temp_c": round(temp_c, 1),
        "temp_f": round(temp_c * 9 / 5 + 32),
        "feels_like_c": round(feels_like_c, 1),
        "conditions": condition,
        "precip_probability_pct": precip_prob,
        "precip_mm": precip_mm,
        "humidity_pct": humidity,
        "wind_kph": wind_kph,
        "wind_direction": hour_rng.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW"]),
        "uv_index": max(0, round((amplitude / 8) * math.sin((hour - 6) / 12 * math.pi) * 11)) if is_daylight else 0,
        "visibility_km": round(hour_rng.uniform(2.0, 20.0), 1),
        "is_daylight": is_daylight,
    }
    return json.dumps(forecast)


# ---------------------------------------------------------------------------
# Availability & pricing search
# ---------------------------------------------------------------------------


@tool(
    approval_mode="never_require",
    description="List the booking vendors available for flights and hotels.",
)
def list_booking_vendors() -> str:
    """List the booking vendors available for flights and hotels."""
    catalog = {
        "flights": [
            *({"vendor": name, "domain": _VENDOR_DOMAINS[name]} for name in _FLIGHT_MARKETPLACES),
        ],
        "hotels": [
            *({"vendor": name, "domain": _VENDOR_DOMAINS[name]} for name in _HOTEL_MARKETPLACES),
        ],
    }
    return json.dumps(catalog)


@tool(
    approval_mode="never_require",
    description="Search flights priced against a single booking vendor, sorted cheapest-first.",
)
def search_flights(
    origin: Annotated[str, Field(description="Departure city or airport.")],
    destination: Annotated[str, Field(description="Arrival city or airport.")],
    depart_date: Annotated[str, Field(description="Departure date, e.g. '2026-06-14'.")],
    vendor: Annotated[str, Field(description="Single vendor to price against: Expedia, Kayak, Priceline.")],
) -> str:
    """Search flights priced against a single booking vendor.

    Each search is quoted against one vendor — a marketplace (Expedia, Kayak, Priceline)
    or ``Direct`` for the airline's own website. To compare vendors, call this again with
    a different ``vendor``. Every option includes the per-ticket price, taxes/fees, total,
    cancellation policy, and a booking URL; results are sorted cheapest-first.
    """
    rng = _seed("flight", origin, destination, depart_date, vendor or "")
    airlines = ["Skyward", "AeroLink", "BlueJet", "Vista Air", "NorthStar"]
    vendor_name = _resolve_vendor(vendor, rng, _FLIGHT_MARKETPLACES)
    is_direct = vendor_name == _DIRECT
    options: list[dict[str, Any]] = []
    for _ in range(rng.randint(3, 4)):
        stops = rng.choice([0, 0, 1, 2])
        depart_hour = rng.randint(5, 21)
        duration_h = rng.randint(2, 14) + stops * 2
        airline = rng.choice(airlines)
        flight_id = f"FL-{uuid4().hex[:6].upper()}"
        display_vendor = f"{airline} (airline site)" if is_direct else vendor_name
        domain = f"{_slug(airline)}.com" if is_direct else _VENDOR_DOMAINS[vendor_name]
        price = _quote(rng, rng.randint(180, 1200), 1, is_direct)
        options.append({
            "flight_id": flight_id,
            "airline": airline,
            "vendor": display_vendor,
            "channel": "direct" if is_direct else "marketplace",
            "origin": origin,
            "destination": destination,
            "depart_date": depart_date,
            "depart_time": f"{depart_hour:02d}:{rng.choice(['00', '15', '30', '45'])}",
            "duration_hours": duration_h,
            "stops": stops,
            "cabin": rng.choice(["economy", "economy", "premium economy", "business"]),
            "seats_left": rng.randint(1, 30),
            "price_usd": price["unit_price_usd"],
            "taxes_fees_usd": price["taxes_fees_usd"],
            "total_usd": price["total_usd"],
            "free_cancellation": price["free_cancellation"],
            "booking_url": f"https://www.{domain}/checkout?ref={flight_id}",
        })
    options.sort(key=lambda o: o["total_usd"])
    return json.dumps(options)


@tool(
    approval_mode="never_require",
    description="Search hotels priced against a single booking vendor, sorted cheapest-first.",
)
def search_hotels(
    destination: Annotated[str, Field(description="City where the hotel is located.")],
    check_in: Annotated[str, Field(description="Check-in date, e.g. '2026-06-14'.")],
    nights: Annotated[int, Field(description="Number of nights.")],
    vendor: Annotated[
        str, Field(description=("Single vendor to price against: Booking.com, Expedia, Hotels.com, Agoda."))
    ],
) -> str:
    """Search hotels priced against a single booking vendor.

    Each search is quoted against one vendor — a marketplace (Booking.com, Expedia,
    Hotels.com, Agoda) or ``Direct`` for the hotel's own website. To compare vendors,
    call this again with a different ``vendor``. Every option includes the nightly rate,
    taxes/fees, total, cancellation policy, and a booking URL; results are sorted
    cheapest-first.
    """
    rng = _seed("hotel", destination, check_in, str(nights), vendor or "")
    names = ["The Grand Coast", "Old Town Inn", "Riverside Suites", "Skyline Hotel", "Casa Bella", "Harbor View Lodge"]
    areas = ["city center", "old town", "beachfront", "business district", "arts quarter"]
    vendor_name = _resolve_vendor(vendor, rng, _HOTEL_MARKETPLACES)
    is_direct = vendor_name == _DIRECT
    options: list[dict[str, Any]] = []
    for name in rng.sample(names, k=rng.randint(3, 5)):
        hotel_id = f"HT-{uuid4().hex[:6].upper()}"
        display_vendor = f"{name} (hotel site)" if is_direct else vendor_name
        domain = f"{_slug(name)}.com" if is_direct else _VENDOR_DOMAINS[vendor_name]
        price = _quote(rng, rng.randint(70, 480), nights, is_direct)
        options.append({
            "hotel_id": hotel_id,
            "name": name,
            "vendor": display_vendor,
            "channel": "direct" if is_direct else "marketplace",
            "destination": destination,
            "area": rng.choice(areas),
            "check_in": check_in,
            "nights": nights,
            "rating": round(rng.uniform(3.4, 4.9), 1),
            "board": rng.choice(["room only", "breakfast included", "half board"]),
            "rooms_left": rng.randint(1, 12),
            "price_per_night_usd": price["unit_price_usd"],
            "taxes_fees_usd": price["taxes_fees_usd"],
            "total_usd": price["total_usd"],
            "free_cancellation": price["free_cancellation"],
            "booking_url": f"https://www.{domain}/checkout?ref={hotel_id}",
        })
    options.sort(key=lambda o: o["total_usd"])
    return json.dumps(options)


@tool(
    approval_mode="never_require",
    description="Search attractions and tours with price, rating, category, hours, and weekly closures.",
)
def search_attractions(
    destination: Annotated[str, Field(description="City or region to find attractions in.")],
    date: Annotated[
        str | None,
        Field(description="Optional visit date in ISO format, e.g. '2026-06-15', to flag attractions closed that day."),
    ] = None,
) -> str:
    """Search attractions and tours with price, rating, category, duration, and opening hours.

    Returns a JSON list of attraction options so the agent can compare and pick ones to book.
    For the supported destinations (Lisbon, Kyoto, Barcelona, Queenstown, Marrakech) these are
    real, well-known attractions. Every option includes ``opening_time``, ``closing_time``, and
    ``closed_days`` (weekdays the attraction is shut). Pass ``date`` to also get ``weekday`` and an
    ``open_on_date`` flag so closed-day visits can be avoided.
    """
    rng = _seed("attraction", destination)
    city_key = _match_city(destination)
    catalog = _ATTRACTIONS[city_key] if city_key else rng.sample(_GENERIC_ATTRACTIONS, k=rng.randint(4, 6))

    weekday_name: str | None = None
    if date:
        try:
            weekday_name = _WEEKDAYS[datetime.strptime(date, "%Y-%m-%d").weekday()]
        except ValueError:
            weekday_name = None

    options: list[dict[str, Any]] = []
    for name, category in catalog:
        opening, closing, closed_days = _attraction_schedule(destination, name)
        entry: dict[str, Any] = {
            "attraction_id": f"AT-{uuid4().hex[:6].upper()}",
            "name": name,
            "destination": destination,
            "category": category,
            "rating": round(rng.uniform(4.0, 5.0), 1),
            "duration_hours": rng.randint(1, 8),
            "price_usd": rng.randint(15, 180),
            "spots_left": rng.randint(2, 40),
            "opening_time": opening,
            "closing_time": closing,
            "closed_days": closed_days,
        }
        if weekday_name is not None:
            entry["weekday"] = weekday_name
            entry["open_on_date"] = weekday_name not in closed_days
        options.append(entry)
    options.sort(key=lambda o: o["rating"], reverse=True)
    return json.dumps(options)


@tool(
    approval_mode="never_require",
    description="Check one flight's price on a single vendor to compare vendors for the same flight.",
)
def get_flight_price(
    flight_id: Annotated[str, Field(description="flight_id of a flight returned by search_flights.")],
    depart_date: Annotated[str, Field(description="The flight's departure date, e.g. '2026-06-14'.")],
    vendor: Annotated[
        str,
        Field(description="Single vendor to check: Expedia, Kayak, Priceline, or Direct for the airline's own site."),
    ],
    airline: Annotated[
        str | None, Field(description="Optional airline name from search_flights for a clearer response.")
    ] = None,
) -> str:
    """Check one flight's price on a single vendor so you can compare vendors.

    Search once with ``search_flights`` to get a ``flight_id``, then call this with a
    different ``vendor`` to price the *same* flight elsewhere. Not every vendor carries
    every flight: if the vendor does not offer it the response has ``available: false`` —
    try another vendor or the airline's own site (``Direct``). Otherwise it returns the
    per-ticket price, taxes/fees, total, cancellation policy, and a booking URL.
    """
    identity = f"{flight_id}|{depart_date}"
    vendor_name = _resolve_vendor(vendor, _seed("flight-vendor", identity, vendor or ""), _FLIGHT_MARKETPLACES)
    is_direct = vendor_name == _DIRECT
    if not _vendor_carries(identity, vendor_name, is_direct):
        return json.dumps({
            "flight_id": flight_id,
            "depart_date": depart_date,
            "vendor": vendor_name,
            "available": False,
            "message": f"{vendor_name} does not currently offer flight {flight_id}.",
        })
    base = _base_price(identity, 180, 1200)
    price = _quote(_seed("flight-quote", identity, vendor_name), base, 1, is_direct)
    if is_direct:
        display_vendor = f"{airline} (airline site)" if airline else "airline site"
        domain = f"{_slug(airline)}.com" if airline else "airline.com"
    else:
        display_vendor = vendor_name
        domain = _VENDOR_DOMAINS[vendor_name]
    return json.dumps({
        "flight_id": flight_id,
        "airline": airline,
        "depart_date": depart_date,
        "vendor": display_vendor,
        "channel": "direct" if is_direct else "marketplace",
        "available": True,
        "price_usd": price["unit_price_usd"],
        "taxes_fees_usd": price["taxes_fees_usd"],
        "total_usd": price["total_usd"],
        "free_cancellation": price["free_cancellation"],
        "booking_url": f"https://www.{domain}/checkout?ref={flight_id}",
    })


@tool(
    approval_mode="never_require",
    description="Check one hotel's price on a single vendor to compare vendors for the same stay.",
)
def get_hotel_price(
    name: Annotated[str, Field(description="Hotel name returned by search_hotels.")],
    destination: Annotated[str, Field(description="City where the hotel is located.")],
    check_in: Annotated[str, Field(description="Check-in date, e.g. '2026-06-14'.")],
    nights: Annotated[int, Field(description="Number of nights.")],
    vendor: Annotated[
        str,
        Field(
            description=(
                "Single vendor to check: Booking.com, Expedia, Hotels.com, Agoda, or Direct for the hotel's own site."
            )
        ),
    ],
) -> str:
    """Check one hotel's price on a single vendor so you can compare vendors.

    Search once with ``search_hotels`` to get a hotel name, then call this with a
    different ``vendor`` to price the *same* stay elsewhere. Not every vendor carries
    every hotel: if the vendor does not offer it the response has ``available: false`` —
    try another vendor or the hotel's own site (``Direct``). Otherwise it returns the
    nightly rate, taxes/fees, total, cancellation policy, and a booking URL.
    """
    identity = f"{_slug(name)}|{_slug(destination)}|{check_in}|{nights}"
    vendor_name = _resolve_vendor(vendor, _seed("hotel-vendor", identity, vendor or ""), _HOTEL_MARKETPLACES)
    is_direct = vendor_name == _DIRECT
    if not _vendor_carries(identity, vendor_name, is_direct):
        return json.dumps({
            "name": name,
            "destination": destination,
            "check_in": check_in,
            "nights": nights,
            "vendor": vendor_name,
            "available": False,
            "message": f"{vendor_name} does not currently offer {name}.",
        })
    base = _base_price(identity, 70, 480)
    price = _quote(_seed("hotel-quote", identity, vendor_name), base, nights, is_direct)
    if is_direct:
        display_vendor = f"{name} (hotel site)"
        domain = f"{_slug(name)}.com"
    else:
        display_vendor = vendor_name
        domain = _VENDOR_DOMAINS[vendor_name]
    return json.dumps({
        "name": name,
        "destination": destination,
        "check_in": check_in,
        "nights": nights,
        "vendor": display_vendor,
        "channel": "direct" if is_direct else "marketplace",
        "available": True,
        "price_per_night_usd": price["unit_price_usd"],
        "taxes_fees_usd": price["taxes_fees_usd"],
        "total_usd": price["total_usd"],
        "free_cancellation": price["free_cancellation"],
        "booking_url": f"https://www.{domain}/checkout?ref={_slug(name)}",
    })


# ---------------------------------------------------------------------------
# Cross-comparison
# ---------------------------------------------------------------------------


@tool(
    approval_mode="never_require",
    description="Score and rank candidate trip packages by total cost, flagging any that exceed the budget.",
)
def compare_trip_packages(
    packages_json: Annotated[
        str,
        Field(
            description=(
                "A JSON array of candidate trip packages to compare. Each package is an object with "
                "'label' (str), and any of 'flight_price_usd', 'hotel_total_usd', "
                "'attractions_total_usd' (numbers). Build these from search_flights, search_hotels, "
                "and search_attractions results."
            )
        ),
    ],
    budget_usd: Annotated[
        float | None,
        Field(description="Optional total budget in USD. Packages over budget are flagged."),
    ] = None,
) -> str:
    """Score and rank candidate trip packages by total cost, flagging any that exceed the budget.

    Use this after searching flights, hotels, and attractions to cross-compare combinations and
    recommend the best-value option. Returns a JSON summary ranked from cheapest to most expensive.
    """
    try:
        packages = json.loads(packages_json)
    except json.JSONDecodeError as exc:
        return json.dumps({"error": f"Invalid packages_json: {exc}"})
    if not isinstance(packages, list) or not packages:
        return json.dumps({"error": "packages_json must be a non-empty JSON array."})

    ranked: list[dict[str, Any]] = []
    for pkg in cast(list[dict[str, Any]], packages):
        flight = float(pkg.get("flight_price_usd", 0) or 0)
        hotel = float(pkg.get("hotel_total_usd", 0) or 0)
        attractions = float(pkg.get("attractions_total_usd", 0) or 0)
        total = flight + hotel + attractions
        entry: dict[str, Any] = {
            "label": pkg.get("label", "unnamed package"),
            "flight_price_usd": flight,
            "hotel_total_usd": hotel,
            "attractions_total_usd": attractions,
            "total_usd": round(total, 2),
        }
        if budget_usd is not None:
            entry["within_budget"] = total <= budget_usd
            entry["over_budget_by_usd"] = round(max(0.0, total - budget_usd), 2)
        ranked.append(entry)

    ranked.sort(key=lambda p: p["total_usd"])
    result: dict[str, Any] = {
        "budget_usd": budget_usd,
        "ranked_packages": ranked,
        "recommended": ranked[0]["label"] if ranked else None,
    }
    return json.dumps(result)


# ---------------------------------------------------------------------------
# Booking
# ---------------------------------------------------------------------------


@tool(
    approval_mode="never_require",
    description="Book a flight through the chosen vendor and return a confirmation.",
)
def book_flight(
    origin: Annotated[str, Field(description="Departure city or airport.")],
    destination: Annotated[str, Field(description="Arrival city or airport.")],
    depart_date: Annotated[str, Field(description="Departure date, e.g. '2026-06-14'.")],
    flight_id: Annotated[
        str | None, Field(description="Optional flight_id from search_flights to book a specific option.")
    ] = None,
    vendor: Annotated[
        str | None,
        Field(description="Vendor to book through, e.g. 'Expedia' or the airline site. Use the best_vendor offer."),
    ] = None,
) -> str:
    """Book a flight through the chosen vendor and return a confirmation. (Fake — no real booking is made.)"""
    rng = _seed("book-flight", origin, destination, depart_date, flight_id or "", vendor or "")
    airline = rng.choice(["Skyward", "AeroLink", "BlueJet", "Vista Air"])
    price = rng.randint(220, 980)
    confirmation = f"FL-{uuid4().hex[:8].upper()}"
    ref = f" (option {flight_id})" if flight_id else ""
    via = f" via {vendor}" if vendor else ""
    return (
        f"Flight booked{ref}{via}: {airline} {origin} → {destination} on {depart_date}. "
        f"Price: ${price}. Confirmation: {confirmation}."
    )


@tool(
    approval_mode="never_require",
    description="Book a hotel through the chosen vendor and return a confirmation.",
)
def book_hotel(
    destination: Annotated[str, Field(description="City where the hotel is located.")],
    check_in: Annotated[str, Field(description="Check-in date, e.g. '2026-06-14'.")],
    nights: Annotated[int, Field(description="Number of nights.")],
    hotel_id: Annotated[
        str | None, Field(description="Optional hotel_id from search_hotels to book a specific option.")
    ] = None,
    vendor: Annotated[
        str | None,
        Field(description="Vendor to book through, e.g. 'Booking.com' or the hotel site. Use the best_vendor offer."),
    ] = None,
) -> str:
    """Book a hotel through the chosen vendor and return a confirmation. (Fake — no real booking is made.)"""
    rng = _seed("book-hotel", destination, check_in, str(nights), hotel_id or "", vendor or "")
    hotel = rng.choice(["The Grand Coast", "Old Town Inn", "Riverside Suites", "Skyline Hotel"])
    nightly = rng.randint(90, 400)
    confirmation = f"HT-{uuid4().hex[:8].upper()}"
    ref = f" (option {hotel_id})" if hotel_id else ""
    via = f" via {vendor}" if vendor else ""
    return (
        f"Hotel booked{ref}{via}: {hotel} in {destination}, check-in {check_in} for {nights} night(s). "
        f"Rate: ${nightly}/night (${nightly * nights} total). Confirmation: {confirmation}."
    )


@tool(
    approval_mode="never_require",
    description="Book tickets for an attraction or tour and return a confirmation.",
)
def book_attraction(
    destination: Annotated[str, Field(description="City where the attraction is located.")],
    attraction_name: Annotated[str, Field(description="Name of the attraction or tour to book.")],
    date: Annotated[str, Field(description="Date of the activity, e.g. '2026-06-15'.")],
    party_size: Annotated[int, Field(description="Number of guests.")],
    attraction_id: Annotated[
        str | None, Field(description="Optional attraction_id from search_attractions to book a specific option.")
    ] = None,
) -> str:
    """Book tickets for an attraction or tour and return a confirmation. (Fake — no real booking is made.)"""
    rng = _seed("book-attraction", destination, attraction_name, date, attraction_id or "")
    per_person = rng.randint(15, 180)
    confirmation = f"AT-{uuid4().hex[:8].upper()}"
    return (
        f"Attraction booked: {attraction_name} in {destination} on {date} for {party_size} "
        f"guest(s). Price: ${per_person}/person (${per_person * party_size} total). "
        f"Confirmation: {confirmation}."
    )


@tool(
    approval_mode="never_require",
    description="Reserve a table at a restaurant and return a confirmation.",
)
def make_restaurant_reservation(
    destination: Annotated[str, Field(description="City where the restaurant is located.")],
    party_size: Annotated[int, Field(description="Number of guests.")],
    reservation_time: Annotated[str, Field(description="Date and time, e.g. '2026-06-15 19:30'.")],
) -> str:
    """Reserve a table at a restaurant. (Fake — no real reservation is made.)"""
    rng = _seed("reservation", destination, str(party_size), reservation_time)
    restaurant = rng.choice(["Casa Verde", "The Blue Table", "Sakura House", "Harbor Grill"])
    confirmation = f"RS-{uuid4().hex[:8].upper()}"
    return (
        f"Reservation confirmed at {restaurant} in {destination} for {party_size} "
        f"guest(s) at {reservation_time}. Confirmation: {confirmation}."
    )
