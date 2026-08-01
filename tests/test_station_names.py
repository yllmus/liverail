from station_names import clean_display_name


def test_leaves_clean_names_unchanged():
    assert clean_display_name("Wien Meidling", "AT") == "Wien Meidling"


def test_de_strips_city_comma_stop_pattern():
    assert clean_display_name("Aachen, Hbf", "DE") == "Aachen Hbf"


def test_de_comma_fix_only_touches_first_comma():
    assert clean_display_name("Neudorf (Erzgeb), Vierenstr", "DE") == "Neudorf (Erzgeb) Vierenstr"


def test_hu_comma_pattern_left_alone():
    assert clean_display_name("Szeged, Pulz utca", "HU") == "Szeged, Pulz utca"


def test_normalizes_missing_space_before_paren():
    assert clean_display_name("TP Summerau(Gr)", "AT") == "TP Summerau (Gr)"


def test_normalizes_missing_spaces_both_sides_of_paren():
    assert clean_display_name("Halle(Saale)Hbf", "DE") == "Halle (Saale) Hbf"


def test_handles_two_adjacent_parenthetical_groups():
    assert clean_display_name("Frankfurt(Oder)(Gr)", "DE") == "Frankfurt (Oder) (Gr)"


def test_filters_bushaltestelle_with_parens():
    assert clean_display_name("Ritschenhausen (Bushaltestelle)", "DE") is None


def test_filters_bushaltestelle_without_parens():
    assert clean_display_name("Holzdorf (b. Weimar), Bushaltestelle", "DE") is None


def test_filters_parkeisenbahn():
    assert clean_display_name("Bernburg (Saale) Tiergarten (Parkeisenbahn)", "DE") is None


def test_filters_museumsbahn():
    assert clean_display_name("Scheggerott Museumsbahn", "DE") is None


def test_filters_faehre():
    assert clean_display_name("Wolgast Wolgaster Fähre", "DE") is None


def test_filters_virt_marker():
    assert clean_display_name("Thondorf (Siersleben), Schenkplatz (virt.)", "DE") is None


def test_filters_u_tram_marker():
    assert clean_display_name("Hauptbahnhof (U, Tram)", "DE") is None


def test_filters_hoenheim_tram():
    assert clean_display_name("Hoenheim-Tram", "DE") is None


def test_does_not_touch_czech_diacritics():
    # Documented, permanent limitation - the source feed's diacritics are
    # already wrong; this must not attempt to "fix" them.
    assert clean_display_name("Ceska Trebova", "DE") == "Ceska Trebova"


def test_none_input_returns_none():
    assert clean_display_name(None, "DE") is None


def test_blank_string_returns_none():
    assert clean_display_name("   ", "HU") is None
