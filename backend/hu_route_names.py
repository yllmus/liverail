import re

def extract_train_number(trip_short_name):
    """Pulls the train's real public number out of tripShortName's leading
    token (e.g. "5827" from "5827 személyvonat", "3047" from "H3047 HÉV").
    Used by poller.py's resolve_trip_id() to join a live vehicle to a
    specific static trips.id row via routes.name, since MAV's own static
    feed has trip_short_name 0/34,349 filled - the number has to come from
    the live feed's tripShortName instead. route.longName and gtfsId can't be
    used for this: longName groups many trains together, and gtfsId is
    internal to MÁV's OTP instance and joins to nothing in the static feed.
    """
    if not trip_short_name:
        return None
    m = re.search(r"\d+", trip_short_name.split(" ", 1)[0])
    return m.group(0) if m else None

def clean_route_name(long_name, trip_short_name):
    """A stable route name for one live Hungarian train.

    MÁV's route.longName often lists every train number that has run on a
    line pattern ("Sz 36410/36412/.../36443"), and the list changes from day
    to day. In that case return the category plus this train's own number,
    taken from tripShortName ("Sz 36441"). Names without a slash are already
    stable and pass through unchanged.
    """
    if not long_name or "/" not in long_name:
        return long_name
    category = long_name.split(" ", 1)[0]
    number = (trip_short_name or "").split(" ", 1)[0]
    return f"{category} {number}".strip() if number else category
