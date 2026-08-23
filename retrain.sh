#!/bin/bash
# Weekly champion/challenger cycle for every country, one at a time.
#
# SEQUENTIAL ON PURPOSE. This box has a single vCPU shared with four pollers,
# the API and the push checker; two LightGBM trainings at once would starve all
# of them. A country that is too thin to judge prints its reason and costs
# about a second, so running all four is cheap.
#
# Weekly, not nightly: the underlying patterns move on the scale of timetable
# periods, and a gate that is asked to judge seven times a week on nearly
# identical data will eventually promote noise.
cd /home/ubuntu || exit 1
echo "===== retrain $(date -Is) ====="
for C in DE PL HU AT; do
    timeout 3600 python3 -u model_loop.py --country "$C" 2>&1
    echo
done

# The delay forecast re-measures itself rather than being promoted. It is not
# wired into any endpoint: on the three days of trajectory data that existed
# when it was written it beat persistence by only ~2.5% (DE) and ~1.6% (HU),
# and only beyond three and six stops respectively. Those margins are too thin
# to ship, but they should GROW as history deepens - so this runs weekly and
# writes the numbers to the same log, and the decision to ship becomes a
# reading rather than a guess. See DELAY_STRUCTURE.md section 6.
for C in DE HU; do
    timeout 3600 python3 -u delay_forecast.py --country "$C" 2>&1
    echo
done
