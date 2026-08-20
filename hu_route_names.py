import re

def extract_train_number(trip_short_name):
    """Pulls the train's real public number out of tripShortName's leading
    token (e.g. "5827" from "5827 személyvonat", "3047" from "H3047 HÉV").
    Used by poller.py's resolve_trip_id() to join a live vehicle to a
    specific static trips.id row via routes.name, since MAV's own static
    feed has trip_short_name 0/34,349 filled - the number has to come from
    the live feed's tripShortName instead. See NOTES.md "Live per-train
    identity" for why route.longName/gtfsId can't be used for this.
    """
    if not trip_short_name:
        return None
    m = re.search(r"\d+", trip_short_name.split(" ", 1)[0])
    return m.group(0) if m else None

def clean_route_name(long_name, trip_short_name):
    """MAV's OTP route.longName groups every train number that has ever run
    on a line pattern into one slash-separated string (e.g. "Sz 36410/36412/
    .../36443") instead of identifying the single physical train a
    vehiclePositions entry actually is - confirmed directly against the live
    endpoint 2026-08-02: 50/145 concurrent vehicles hit this, across every
    category (Sz/EC/IC/ER/IR), not just local "Sz" trains, and the static
    GTFS import (routes.name) is already one-route-per-train-number, so
    longName's grouping isn't even the real route granularity - it's an
    OTP-side pattern cluster. Falls back to category + this train's own
    number (from tripShortName, which does identify a single train) instead
    of showing the whole list.
    """
    if not long_name or "/" not in long_name:
        return long_name
    category = long_name.split(" ", 1)[0]
    number = (trip_short_name or "").split(" ", 1)[0]
    return f"{category} {number}".strip() if number else category
