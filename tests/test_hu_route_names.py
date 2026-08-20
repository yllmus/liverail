from hu_route_names import clean_route_name, extract_train_number

def test_passes_through_names_without_slash():
    assert clean_route_name("IC NAPFÉNY", "728 NAPFÉNY InterCity") == "IC NAPFÉNY"
    assert clean_route_name("H8", "H1061 HÉV") == "H8"

def test_collapses_grouped_local_train_list():
    long_name = "Sz 36410/36412/36413/36415/36423/36424/36432/36437/36438/36441/36443"
    assert clean_route_name(long_name, "36441 személyvonat") == "Sz 36441"

def test_collapses_grouped_named_train_list():
    long_name = "EC BARTÓK BÉLA/CSÁRDÁS/LEHÁR FERENC/LISZT FERENC/SEMMELWEIS IGNÁC"
    assert clean_route_name(long_name, "343 BARTÓK BÉLA EuroCity") == "EC 343"

def test_falls_back_to_category_when_trip_short_name_missing():
    assert clean_route_name("Sz 1/2/3", None) == "Sz"
    assert clean_route_name("Sz 1/2/3", "") == "Sz"

def test_passes_through_falsy_input():
    assert clean_route_name(None, "36441 személyvonat") is None
    assert clean_route_name("", "36441 személyvonat") == ""

def test_extract_train_number_plain():
    assert extract_train_number("5827 személyvonat") == "5827"

def test_extract_train_number_hev_prefix():
    assert extract_train_number("H3047 HÉV") == "3047"

def test_extract_train_number_named_train():
    assert extract_train_number("9609 HELIKON InterRégió") == "9609"

def test_extract_train_number_missing():
    assert extract_train_number(None) is None
    assert extract_train_number("") is None

def test_extract_train_number_no_digits_in_leading_token():
    assert extract_train_number("személyvonat 123") is None
