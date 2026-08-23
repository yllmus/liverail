"""Proactive delay alerts over Web Push.

Runs on its own timer a minute after the four pollers, and reads the DATABASE
rather than the live feeds - the pollers have just written the current state,
so re-fetching four national feeds to learn what was already stored would
triple this box's outbound traffic for nothing.

THE HARD PART IS NOT SENDING, IT IS DECIDING WHEN. A watched train is checked
every five minutes for up to 24 hours - 288 opportunities to send the same
fact. Anyone notified "your train is 12 minutes late" 288 times turns
notifications off and never turns them back on, so the useful signal has to be
the CHANGE, not the state:

    delayed    first time it crosses the threshold at all
    worsened   only once it is materially worse than what was last sent
    cancelled  once, and it outranks everything - a cancelled train has no
               delay figure at all (Phase 1 writes NULL, never 0)
    recovered  once, when it drops back under the threshold, so the last
               thing anyone was told is not a delay that no longer exists

decide() holds that logic and touches neither the database nor the network, so
every branch is unit-testable against fixtures. Everything DB-shaped is around
it, exactly as parse_vehicles/resolve_delays are split in the pollers.

WHAT THIS DELIBERATELY DOES NOT DO. It does not alert on a train it cannot
find. A watch whose train has left the feed goes quiet rather than sending
"we lost your train", because the overwhelmingly common cause is the journey
simply having finished.
"""
import argparse
import json
import time

import psycopg2

from config import (DB_PASSWORD, PUSH_ENABLED, VAPID_PRIVATE_KEY,
                    VAPID_SUBJECT)

# Below this, a train is on time in every sense the rest of this project uses -
# it is the same 2-minute boundary as the app-wide severity tiers, plus enough
# margin that normal timetable noise does not fire a notification.
DELAY_THRESHOLD = 5.0
# A delay has to grow by this much before anyone is told again. Set from what
# actually changes a decision: 3 more minutes does not, missing a connection
# does. Smaller values reproduce the every-five-minutes spam this exists to
# avoid.
WORSEN_STEP = 10.0
# Ignore readings older than this. delay_observations only writes a row when a
# figure CHANGES (the PL/DE dedup work), so a stale row is normal and means
# "unchanged", not "no data" - but a reading from this morning should not
# produce an alert this evening.
MAX_READING_AGE_HOURS = 3
# A push service answering 404/410 means that browser is gone permanently.
# Anything else (a 500, a timeout) might be transient, so it takes several
# before a subscription is retired.
MAX_FAILURES = 5


def connect():
    return psycopg2.connect(dbname="train_delay_db", user="trainuser",
                            password=DB_PASSWORD, host="localhost")


def decide(current, last):
    """Should we notify, and about what?

    current: dict(cancelled=bool, delay=float|None, found=bool)
    last:    dict(kind=str, delay=float|None) or None

    Returns (kind, delay) or None. Pure - no DB, no network.
    """
    if not current.get("found"):
        return None                      # journey over, or not in the feed
    last_kind = (last or {}).get("kind")

    if current.get("cancelled"):
        # Once only. Repeating a cancellation adds nothing and the train is
        # not going to un-cancel in a way this would catch.
        return None if last_kind == "cancelled" else ("cancelled", None)

    delay = current.get("delay")
    if delay is None:
        # The feed declined to predict. MÁV does this at a trip's final stop
        # even for an hour-late train; reporting it as on time would be a lie
        # and reporting it as delayed would be a guess.
        return None

    if delay >= DELAY_THRESHOLD:
        if last_kind in (None, "recovered"):
            return ("delayed", delay)
        last_delay = (last or {}).get("delay")
        if last_delay is None or delay >= last_delay + WORSEN_STEP:
            return ("worsened", delay)
        return None

    # Under the threshold. Worth saying only if the last thing we said was that
    # it was late - otherwise this is an unprompted "your train is fine".
    if last_kind in ("delayed", "worsened"):
        return ("recovered", delay)
    return None


def compose(kind, delay, label, station, country):
    """The words a person actually reads. Kept next to decide() so the two
    cannot drift apart, and deliberately plain: a notification is read in two
    seconds on a platform, not studied."""
    name = label or "Your train"
    where = f" at {station}" if station else ""
    if kind == "cancelled":
        return (f"{name} is cancelled",
                f"This service has been cancelled{where}. You may be entitled "
                f"to compensation - tap to check.")
    mins = int(round(delay))
    if kind == "recovered":
        return (f"{name} is back on time", f"Now {mins} min behind{where}.")
    verb = "is running late" if kind == "delayed" else "is running later"
    return (f"{name} {verb}", f"{mins} min behind schedule{where}.")


def latest_state(cur, country, train_id, station=None):
    """The most recent reading for one watched train.

    The identity column differs per country and that is deliberate - see
    schema/017. HU/AT carry the opaque feed id on the observation itself;
    PL/DE carry a real trips FK, so they join through gtfs_trip_id.
    """
    params = [country, train_id, MAX_READING_AGE_HOURS]
    if country in ("HU", "AT"):
        where_id = "d.live_trip_id = %s"
        join = ""
    else:
        where_id = "t.gtfs_trip_id = %s"
        join = "JOIN trips t ON t.id = d.trip_id"
    station_clause = ""
    if station:
        station_clause = " AND s.name = %s"
    sql = f"""
        SELECT d.is_cancelled, d.delay_minutes, s.name, d.observed_date
        FROM delay_observations d
        {join}
        LEFT JOIN stations s ON s.id = d.station_id
        WHERE d.country_code = %s AND {where_id}
          AND d.observed_date >= (now() - make_interval(hours => %s))::date
          {station_clause}
        ORDER BY d.id DESC
        LIMIT 1
    """
    if station:
        params.append(station)
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return {"found": False}
    cancelled, delay, stop_name, _ = row
    return {"found": True, "cancelled": bool(cancelled),
            "delay": float(delay) if delay is not None else None,
            "stop": stop_name}


def send_push(sub, payload):
    """Returns (ok, status_code_or_None). Import is local so the rest of this
    module - decide(), compose() - stays importable for tests on a box with no
    pywebpush installed."""
    from pywebpush import webpush, WebPushException
    try:
        webpush(
            subscription_info={"endpoint": sub["endpoint"],
                               "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]}},
            data=json.dumps(payload),
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_SUBJECT},
            timeout=10,
        )
        return True, 201
    except WebPushException as exc:
        code = getattr(getattr(exc, "response", None), "status_code", None)
        return False, code
    except Exception as exc:
        # Deliberately broad. A subscription whose stored p256dh is not a valid
        # EC point raises ValueError out of the encryption layer, well before
        # any HTTP request - and an uncaught one here would abandon the whole
        # run, so ONE corrupt row would silence alerts for every other watcher.
        # Treated as a normal failure so the retry counter retires it.
        print(f"  send error ({type(exc).__name__}: {exc})")
        return False, None


def run(dry_run=False, verbose=False):
    if not PUSH_ENABLED:
        print("push disabled: no VAPID keys configured (see .env.example)")
        return 0
    conn = connect()
    cur = conn.cursor()
    sent = checked = 0
    try:
        cur.execute("""
            SELECT w.id, w.country_code, w.train_id, w.label, w.station,
                   s.id, s.endpoint, s.p256dh, s.auth
            FROM push_watch w
            JOIN push_subscription s ON s.id = w.subscription_id
            WHERE w.expires_at > now() AND s.disabled_at IS NULL
        """)
        watches = cur.fetchall()
        for (wid, country, train_id, label, station,
             sub_id, endpoint, p256dh, auth) in watches:
            checked += 1
            current = latest_state(cur, country, train_id, station)
            cur.execute("""SELECT kind, delay_minutes FROM push_sent
                           WHERE watch_id = %s ORDER BY sent_at DESC LIMIT 1""", (wid,))
            r = cur.fetchone()
            last = {"kind": r[0], "delay": r[1]} if r else None
            verdict = decide(current, last)
            if verbose:
                print(f"  watch {wid} {country} {train_id[:24]}: "
                      f"{current} last={last} -> {verdict}")
            if not verdict:
                continue
            kind, delay = verdict
            title, body = compose(kind, delay, label, station or current.get("stop"), country)
            payload = {"title": title, "body": body,
                       "url": f"/?country={country}&train={train_id}",
                       "tag": f"watch-{wid}", "kind": kind}
            if dry_run:
                print(f"  WOULD SEND [{kind}] {title} - {body}")
                sent += 1
                continue
            ok, code = send_push({"endpoint": endpoint, "p256dh": p256dh, "auth": auth},
                                 payload)
            if ok:
                cur.execute("""INSERT INTO push_sent (watch_id, kind, delay_minutes)
                               VALUES (%s,%s,%s)""", (wid, kind, delay))
                cur.execute("UPDATE push_subscription SET last_ok_at=now(), failures=0 "
                            "WHERE id=%s", (sub_id,))
                sent += 1
            elif code in (404, 410):
                # Gone for good - the browser unsubscribed or was wiped.
                cur.execute("UPDATE push_subscription SET disabled_at=now() WHERE id=%s",
                            (sub_id,))
                print(f"  subscription {sub_id} retired (HTTP {code})")
            else:
                cur.execute("""UPDATE push_subscription SET failures = failures + 1,
                               disabled_at = CASE WHEN failures + 1 >= %s THEN now()
                                                  ELSE disabled_at END
                               WHERE id=%s""", (MAX_FAILURES, sub_id))
                print(f"  send failed for subscription {sub_id} (HTTP {code})")
            conn.commit()
        # Housekeeping: expired watches and the subscriptions left holding none.
        cur.execute("DELETE FROM push_watch WHERE expires_at < now() - interval '7 days'")
        cur.execute("""DELETE FROM push_subscription
                       WHERE disabled_at < now() - interval '30 days'""")
        conn.commit()
        print(f"Checked {checked} watch(es), sent {sent} notification(s)"
              f"{' [DRY RUN]' if dry_run else ''}.")
        return sent
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    t0 = time.time()
    run(dry_run=a.dry_run, verbose=a.verbose)
    print(f"took {time.time()-t0:.1f}s")
