import re

# Confirmed non-rail rows in the DE static feed (41 total, verified against
# the live DB - a plain substring check, not a stricter "must be
# parenthesized" match, since some Bushaltestelle rows use
# ", Bushaltestelle" with no parens at all. Checked query-wide: none of
# these substrings appear anywhere in HU/AT/PL's station names today, so
# this list is applied country-agnostically rather than DE-gated. Kept
# short and literal on purpose - these are the only patterns actually
# confirmed in real data. Do NOT extend this list from guesses.
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

    # DE only: "City, Stop" -> "City Stop" (1446/7870 = 18.4% of DE
    # stations). Only the first comma is collapsed so a name with a second,
    # unrelated comma later isn't mangled. HU also has a comma pattern
    # (17/1197) but it's a legitimate MÁV on-street-halt naming convention,
    # not broken - left untouched.
    if country_code == "DE":
        cleaned = re.sub(r",\s+", " ", cleaned, count=1)

    # All countries: normalize parenthetical spacing - exactly one space
    # before "(", none just inside "(" or ")", one after ")" if glued to
    # the next word. Handles "TP Summerau(Gr)" -> "TP Summerau (Gr)",
    # "Halle(Saale)Hbf" -> "Halle (Saale) Hbf".
    cleaned = re.sub(r"\s*\(", " (", cleaned)
    cleaned = re.sub(r"\(\s+", "(", cleaned)
    cleaned = re.sub(r"\s+\)", ")", cleaned)
    cleaned = re.sub(r"\)(?=\S)", ") ", cleaned)

    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None
