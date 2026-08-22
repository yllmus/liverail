// Oneshot script: plan an ÖBB journey between two stations and print the
// itineraries as JSON. Third of the three HAFAS scripts, and deliberately the
// same shape as radar.js and trip.js — oneshot subprocess, never a persistent
// server, because this box is memory-constrained and already runs four
// pollers alongside the API.
//
// Why this exists: /plan was Hungary-only, because MÁV's OTP is the one
// planner of the four countries that we can call. Austria is the only other
// one with a usable planner — hafas-client's oebb profile implements
// journeys(). Germany's HAFAS is shut down and Poland's feed is a data mirror
// with no planning API, so both of those need a connection-scan search over
// the stop_times already imported; neither can be borrowed like this.
//
// Usage: node journeys.js '<fromName>' '<toName>' '<ISO departure>' <results>
//
// Stations are resolved by NAME through HAFAS's own locations() lookup rather
// than by passing coordinates, because ÖBB's planner keys on its own station
// ids and a nearby-coordinate match silently plans from the wrong platform
// cluster at large stations. The caller passes the name it holds; this script
// does the resolution and reports what it matched, so a wrong match is
// visible rather than silent.
import { createClient } from 'hafas-client';
import { profile as oebbProfile } from 'hafas-client/p/oebb/index.js';

const client = createClient(oebbProfile, 'train-delay-tracker');

const fromName = process.argv[2];
const toName = process.argv[3];
const departure = process.argv[4] ? new Date(process.argv[4]) : new Date();
const results = Math.max(1, Math.min(parseInt(process.argv[5] || '4', 10), 6));

if (!fromName || !toName) {
  process.stderr.write('journeys.js: need <from> <to>\n');
  process.exit(2);
}

async function resolveStop(name) {
  const found = await client.locations(name, { results: 5, addresses: false, poi: false });
  const stop = (found || []).find((f) => f.type === 'stop' || f.type === 'station');
  return stop || null;
}

try {
  const [from, to] = await Promise.all([resolveStop(fromName), resolveStop(toName)]);
  if (!from || !to) {
    process.stdout.write(JSON.stringify({
      error: 'station-not-found',
      // Named individually so the caller can say which end failed rather than
      // reporting a generic failure for a journey that is half-resolvable.
      fromResolved: from ? from.name : null,
      toResolved: to ? to.name : null,
    }));
    process.exit(0);
  }
  const res = await client.journeys(from.id, to.id, {
    departure, results, transfers: -1, stopovers: false, polylines: false,
    // Rail only, matching every other country's scope in this project. ÖBB's
    // planner will otherwise route via regional buses, which we neither track
    // nor hold delay history for - a risk figure could not be computed for
    // such a leg and the itinerary would look broken.
    products: { nationalExpress: true, national: true, interregional: true,
                regional: true, suburban: true, bus: false, ferry: false,
                subway: false, tram: false, onCall: false },
  });
  process.stdout.write(JSON.stringify({
    fromResolved: from.name,
    toResolved: to.name,
    journeys: (res.journeys || []).map((j) => ({
      legs: (j.legs || []).map((l) => ({
        walking: !!l.walking,
        from: l.origin ? l.origin.name : null,
        to: l.destination ? l.destination.name : null,
        departure: l.departure || l.plannedDeparture || null,
        arrival: l.arrival || l.plannedArrival || null,
        departureDelay: l.departureDelay === undefined ? null : l.departureDelay,
        arrivalDelay: l.arrivalDelay === undefined ? null : l.arrivalDelay,
        line: l.line ? l.line.name : null,
        // ÖBB fills trip_short_name in its STATIC feed ("RJ 658") and the live
        // line name in the same CATEGORY NUMBER order, which is what
        // at_route_names.extract_at_train_number() already keys on. Passed
        // through so a leg arrives joinable to our own history.
        fahrtNr: l.line && l.line.fahrtNr ? l.line.fahrtNr : null,
        tripId: l.tripId || null,
        cancelled: !!l.cancelled,
      })),
    })),
  }));
} catch (err) {
  process.stderr.write(`journeys.js failed: ${err.message}\n`);
  process.exit(1);
}
