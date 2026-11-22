"""Fold a station or route name into the ASCII form a rider actually types.

Pure, dependency-free, and unit-tested (`tests/test_search_text.py`) - same
convention as `station_names.clean_display_name()` and
`austria_poller.station_name_candidates()`.

This exists because trigram similarity is computed byte-for-byte: "Krakow"
and "Krakow Glowny" share trigrams, "Krakow" and "Krakow Glowny"'s real
spelling `Kraków Główny` do not. Measured on the real index before this was
added, searching PL for `Krakow` returned **zero** results - the single
busiest station in southern Poland, invisible to anyone without a Polish
keyboard. The same applies to `Munchen`/`München` (DE), `Wien`/`Wien` (AT is
fine, but `Salzburg-Süd` is not) and `Szekesfehervar`/`Székesfehérvár` (HU).

Folding happens in Python at index time *and* at query time, rather than via
Postgres' `unaccent` extension, for one concrete reason: `unaccent()` is
STABLE, not IMMUTABLE (it reads a dictionary file), so it cannot be used in a
generated column or an expression index without wrapping it in a hand-rolled
IMMUTABLE function that lies about that. Storing a plain folded column keeps
the trigram index a plain column index and keeps this logic testable without
a database.
"""

import unicodedata

# Letters NFKD does not decompose, because the stroke is part of the letter
# rather than a combining mark. Polish ł is the one that matters here (Łódź,
# Wrocław).
_EXTRA_FOLDS = {
    "ł": "l", "Ł": "L",      # ł Ł  (pl)
    "ß": "ss",                     # ß    (de)
    "ø": "o", "Ø": "O",      # ø Ø
    "æ": "ae", "Æ": "AE",    # æ Æ
    "œ": "oe", "Œ": "OE",    # œ Œ
    "đ": "d", "Đ": "D",      # đ Đ
    "þ": "th", "Þ": "TH",    # þ Þ
    "ı": "i",                      # ı
}


def normalize_search_text(value):
    """Lowercase, strip diacritics, and collapse whitespace.

    German umlauts fold to the bare vowel (`ö` -> `o`, giving `munchen`)
    rather than the `oe` transliteration (`muenchen`). Both are things people
    type; only one can be the exact match, and dropping the diacritic is the
    commoner keyboard-limited spelling. `muenchen` still matches by trigram
    similarity, just not exactly.
    """
    if not value:
        return ""
    out = []
    for ch in value:
        if ch in _EXTRA_FOLDS:
            out.append(_EXTRA_FOLDS[ch])
        else:
            out.append(ch)
    decomposed = unicodedata.normalize("NFKD", "".join(out))
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.lower().split())
