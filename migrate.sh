#!/bin/bash
# Move this project to a new machine with the smallest possible data gap.
#
# RESTORE.md builds a box from nothing. This is the other half: cutting a
# LIVE box over to a new one without losing observations or writing to two
# databases at once. Run `freeze` on the old machine and `restore` on the new.
#
# WHY THERE IS A GAP AT ALL, and why that is the right trade. The four pollers
# write every five minutes and their history is the one asset no feed can give
# back. If both machines poll at once they write divergent histories into two
# databases and there is no clean way to merge them afterwards. So the old box
# stops first, and the gap is a known handful of poll cycles rather than a
# silent inconsistency. Expect 30-45 minutes, nearly all of it index rebuilds.
#
#   old$  ./migrate.sh freeze
#   (copy the dump across)
#   new$  ./migrate.sh restore <dump-file>
#   new$  ./migrate.sh verify <counts-file>
set -euo pipefail
cd "$(dirname "$0")"

TIMERS="poller@hu.timer poller@at.timer poller@pl.timer poller@de.timer pushalerts.timer retrain.timer"
STAMP=$(date +%Y%m%d_%H%M%S)

counts() {
    python3 - <<'PY'
from config import DB_PASSWORD
import psycopg2, json, sys
c = psycopg2.connect(dbname="train_delay_db", user="trainuser",
                     password=DB_PASSWORD, host="localhost")
cur = c.cursor()
out = {}
cur.execute("SELECT country_code, count(*) FROM delay_observations GROUP BY 1 ORDER BY 1")
out["delay_observations"] = dict(cur.fetchall())
for t in ("stations", "routes", "trips", "stop_times", "delay_distribution",
          "push_subscription", "push_watch", "model_eval"):
    try:
        cur.execute(f"SELECT count(*) FROM {t}")
        out[t] = cur.fetchone()[0]
    except Exception:
        c.rollback()
        out[t] = None
print(json.dumps(out, indent=1, sort_keys=True))
PY
}

case "${1:-}" in
freeze)
    echo "==> stopping all scheduled writers (the data gap starts now)"
    # Timers first, then any service mid-run, so nothing restarts underneath us.
    sudo systemctl stop $TIMERS
    for u in poller@hu poller@at poller@pl poller@de pushalerts retrain; do
        sudo systemctl stop "$u.service" 2>/dev/null || true
    done
    sudo systemctl disable $TIMERS >/dev/null 2>&1 || true
    echo "==> recording row counts, so the restore can be checked rather than assumed"
    counts > "counts_$STAMP.json"
    cat "counts_$STAMP.json"
    echo "==> dumping"
    DUMP="train_delay_db_migrate_$STAMP.dump"
    sudo -u postgres pg_dump -Fc -d train_delay_db -f "/tmp/$DUMP"
    sudo mv "/tmp/$DUMP" .
    sudo chown "$USER" "$DUMP"
    sha256sum "$DUMP" | tee "$DUMP.sha256"
    ls -lh "$DUMP"
    cat <<MSG

Frozen. Nothing on this machine is writing to the database any more.

Copy BOTH of these to the new box, plus .env (which is gitignored and holds
the database password AND the VAPID keys - regenerating those would invalidate
every existing push subscription):

    scp $DUMP $DUMP.sha256 counts_$STAMP.json .env user@NEW:~/

then on the new box:  ./migrate.sh restore $DUMP
MSG
    ;;

restore)
    DUMP="${2:?usage: ./migrate.sh restore <dump-file>}"
    [ -f "$DUMP.sha256" ] && sha256sum -c "$DUMP.sha256"
    [ -f .env ] || { echo "REFUSING: .env is missing - copy it from the old box first"; exit 1; }
    echo "==> creating role and database if absent"
    sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='trainuser'" \
        | grep -q 1 || sudo -u postgres createuser trainuser
    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='train_delay_db'" \
        | grep -q 1 || sudo -u postgres createdb -O trainuser train_delay_db
    PW=$(grep -oP '(?<=^TRAIN_DB_PASSWORD=).*' .env)
    sudo -u postgres psql -q -c "ALTER ROLE trainuser WITH PASSWORD '$PW';"
    echo "==> restoring (parallel; index rebuilds are the slow part)"
    JOBS=$(( $(nproc) > 4 ? 4 : $(nproc) ))
    sudo -u postgres pg_restore -d train_delay_db -j "$JOBS" --no-owner --role=trainuser "$DUMP" \
        || echo "NOTE: pg_restore reported errors; check them before trusting this."
    sudo -u postgres psql -q -d train_delay_db -c "ANALYZE;"
    echo "==> restored counts"
    counts
    ;;

verify)
    OLD="${2:?usage: ./migrate.sh verify <counts-file-from-old-box>}"
    counts > /tmp/counts_new.json
    python3 - "$OLD" /tmp/counts_new.json <<'PY'
import json, sys
old = json.load(open(sys.argv[1])); new = json.load(open(sys.argv[2]))
bad = False
for k in sorted(set(old) | set(new)):
    o, n = old.get(k), new.get(k)
    if isinstance(o, dict):
        for cc in sorted(set(o) | set(n or {})):
            a, b = o.get(cc), (n or {}).get(cc)
            flag = "" if a == b else "   <-- MISMATCH"
            bad |= a != b
            print(f"  delay_observations[{cc}]  old={a}  new={b}{flag}")
    else:
        flag = "" if o == n else "   <-- MISMATCH"
        bad |= o != n
        print(f"  {k:22} old={o}  new={n}{flag}")
print("\nMISMATCHES FOUND - do not decommission the old box" if bad
      else "\nAll counts match.")
sys.exit(1 if bad else 0)
PY
    ;;

start)
    echo "==> starting the pollers on this machine (the data gap ends now)"
    sudo systemctl enable --now $TIMERS
    systemctl list-timers $TIMERS --no-pager
    ;;

*)
    sed -n '2,20p' "$0"
    exit 1
    ;;
esac
