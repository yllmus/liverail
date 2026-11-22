// Oneshot script: fetch ONE ÖBB trip's full journey by its HAFAS trip id and
// print it as JSON. Same design as radar.js (oneshot subprocess, never a
// persistent server — the box is memory-constrained), and invoked the same
// way, from api.py's /train/AT/{id} branch.
//
// Why this exists separately from radar.js: radar() returns only
// `nextStopovers`, which in practice is the next ~4 stops (it is bounded by
// the radar call's own `duration` window), not the whole journey. trip()
// returns every stopover from origin to destination with both planned and
// live times — the actual per-stop timeline a train tracker needs.
//
// Usage: node trip.js '<tripId>' '<lineName>'
// lineName is required by hafas-client's oebb profile for trip(); it comes
// straight from the same radar movement the tripId did (movement.line.name).
import { createClient } from 'hafas-client';
import { profile as oebbProfile } from 'hafas-client/p/oebb/index.js';

const client = createClient(oebbProfile, 'train-delay-tracker');

const tripId = process.argv[2];
const lineName = process.argv[3] || undefined;

if (!tripId) {
  process.stderr.write('trip.js: missing tripId argument\n');
  process.exit(2);
}

try {
  const res = await client.trip(tripId, { lineName, stopovers: true, polyline: false });
  const t = res.trip || res;
  // Reshaped to just what api.py consumes, rather than dumping HAFAS's full
  // object: the raw trip carries a 2400-point polyline and a products map per
  // stop, which is megabytes of JSON crossing a subprocess pipe for no gain.
  process.stdout.write(JSON.stringify({
    id: t.id,
    line: t.line ? t.line.name : null,
    direction: t.direction || (t.destination ? t.destination.name : null),
    origin: t.origin ? t.origin.name : null,
    destination: t.destination ? t.destination.name : null,
    cancelled: !!t.cancelled,
    currentLocation: t.currentLocation
      ? { latitude: t.currentLocation.latitude, longitude: t.currentLocation.longitude }
      : null,
    stopovers: (t.stopovers || []).map((s) => ({
      name: s.stop ? s.stop.name : null,
      latitude: s.stop && s.stop.location ? s.stop.location.latitude : null,
      longitude: s.stop && s.stop.location ? s.stop.location.longitude : null,
      plannedArrival: s.plannedArrival || null,
      arrival: s.arrival || null,
      arrivalDelay: s.arrivalDelay === undefined ? null : s.arrivalDelay,
      plannedDeparture: s.plannedDeparture || null,
      departure: s.departure || null,
      departureDelay: s.departureDelay === undefined ? null : s.departureDelay,
      platform: s.departurePlatform || s.arrivalPlatform || null,
      cancelled: !!s.cancelled,
    })),
  }));
} catch (err) {
  process.stderr.write(`trip.js failed: ${err.message}\n`);
  process.exit(1);
}
