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
