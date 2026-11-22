import re

# Non-rail stops that appear in the German rail feed. Kept to the patterns
# actually seen in the data, and matched as plain substrings because some of
# them have no parentheses.
_NON_RAIL_MARKERS = (
    "Bushaltestelle",
    "Parkeisenbahn",
    "Museumsbahn",
    "Fähre",
    "(virt.)",
    "(U, Tram)",
    "-Tram",
)


def clean_display_name(name, country_code):
    """Pure function: raw GTFS stop_name -> rider-facing display_name, or
    None for confirmed non-rail entries. No DB access, no side effects.

    Deliberately conservative, checked against real counts before deciding
    to skip:
    - Bahnhof/Hbf/Bahnhst/Bf abbreviation canonicalization (AT: 5 competing
      conventions across 1173 stations; DE: 4 competing conventions across
      7870) is not attempted - genuinely ambiguous without a hand-built
      per-station mapping.
    - Czech-diacritic correction on DE border stations (e.g. "Ceska
      Trebova") is not attempted - wrong at the source feed, and guessing
      correct diacritics from memory risks fabricating wrong station names.
    """
    if not isinstance(name, str) or not name.strip():
        return None
    if any(marker in name for marker in _NON_RAIL_MARKERS):
        return None

    cleaned = name.strip()

    # Germany writes "City, Stop"; show it as "City Stop". Only the first comma
    # is collapsed. Hungary's commas are a deliberate MÁV naming convention for
    # street halts and are left alone.
    if country_code == "DE":
        cleaned = re.sub(r",\s+", " ", cleaned, count=1)

    # Normalise spacing around parentheses: "Halle(Saale)Hbf" becomes "Halle
    # (Saale) Hbf".
    cleaned = re.sub(r"\s*\(", " (", cleaned)
    cleaned = re.sub(r"\(\s+", "(", cleaned)
    cleaned = re.sub(r"\s+\)", ")", cleaned)
    cleaned = re.sub(r"\)(?=\S)", ") ", cleaned)

    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None
