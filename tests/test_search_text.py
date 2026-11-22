"""Diacritic folding for search. Pure, no DB, no network.

Every expectation here is a real station name from one of the four imported
feeds, paired with what a rider on an English/ASCII keyboard actually types.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend"))

from search_text import normalize_search_text


def test_lowercases_and_trims():
    assert normalize_search_text("  Budapest-Keleti  ") == "budapest-keleti"


def test_collapses_inner_whitespace():
    assert normalize_search_text("Wien   Hauptbahnhof") == "wien hauptbahnhof"


def test_empty_and_none():
    assert normalize_search_text("") == ""
    assert normalize_search_text(None) == ""


def test_hungarian_accents():
    # Székesfehérvár is one of HU's busiest stations; "Szekesfehervar" is
    # what anyone without a Hungarian keyboard types.
    assert normalize_search_text("Székesfehérvár") == "szekesfehervar"
    assert normalize_search_text("Nyíregyháza") == "nyiregyhaza"


def test_german_umlauts_fold_to_bare_vowel():
    # NOT "muenchen": dropping the diacritic is the commoner keyboard-limited
    # spelling, and only one form can be the exact match. See search_text.py.
    assert normalize_search_text("München Hbf") == "munchen hbf"
    assert normalize_search_text("Osnabrück") == "osnabruck"


def test_eszett_expands():
    assert normalize_search_text("Weißenfels") == "weissenfels"


def test_polish_stroked_l():
    # The case that returned literally zero results before this existed: `ł`
    # has no combining-mark decomposition, so NFKD alone leaves it untouched.
    assert normalize_search_text("Kraków Główny") == "krakow glowny"
    assert normalize_search_text("Łódź Fabryczna") == "lodz fabryczna"
    assert normalize_search_text("Wrocław Główny") == "wroclaw glowny"


def test_czech_and_slovak_marks_on_border_stations():
    assert normalize_search_text("Děčín hl.n.") == "decin hl.n."
    assert normalize_search_text("Žilina") == "zilina"


def test_punctuation_and_case_preserved_otherwise():
    # Folding must not strip the characters route codes are made of.
    assert normalize_search_text("RE54") == "re54"
    assert normalize_search_text("Halle (Saale) Hbf") == "halle (saale) hbf"


def test_is_idempotent():
    once = normalize_search_text("Kraków Główny")
    assert normalize_search_text(once) == once
