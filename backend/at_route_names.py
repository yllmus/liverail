import re

def extract_at_train_number(line_name):
    """Pulls the train's public number out of HAFAS's live line name (e.g.
    "639" from "RJX 639", "1130" from "D 1130"). Used by
    austria_poller.py's resolve_trip_id() to join a live vehicle to a
    specific static trips.id row via trips.trip_short_name.

    Unlike HU's tripShortName ("5827 személyvonat" - number first), AT's
    live line.name and static trip_short_name both put the category first
    ("RJ 658", "RJX 639"), so this searches the whole string for a digit run
    rather than just the leading token (see hu_route_names.py's
    extract_train_number for the HU equivalent, which can't be reused here
    for that reason).

    Requires the digit run to be preceded by whitespace - AT's S-Bahn lines
    are named as bare line codes with no separating space ("S1", "S45"),
    not a train number, and static trip_short_name never uses that format
    either (it's "S 28209", space included) - so an unspaced digit run is a
    line code, not a resolvable per-train number, and must not be treated
    as one.
    """
    if not line_name:
        return None
    m = re.search(r"\s(\d+)", line_name)
    return m.group(1) if m else None
