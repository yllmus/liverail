// Oneshot script: fetch current ÖBB vehicle positions and print them as JSON.
// Invoked fresh per call from austria_poller.py and from api.py's /live?country=AT
// branch — deliberately not a persistent server (box is memory-constrained).
// hafas-client is ESM-only, hence this file (and its package.json "type") is ESM too.
import { createClient } from 'hafas-client';
import { profile as oebbProfile } from 'hafas-client/p/oebb/index.js';

const client = createClient(oebbProfile, 'train-delay-tracker');

// Austria's bounding box.
const bbox = {
  north: 49.02,
  west: 9.53,
  south: 46.37,
  east: 17.16,
};

try {
  // client.radar() resolves to {movements: [...], realtimeDataUpdatedAt}.
  // Only the array is written out — austria_poller.py's parse_at_vehicles()
  // (and its test fixture) expect a bare list of vehicle movements.
  //
  // results was left at hafas-client's own default (256) - confirmed by
  // direct testing this was silently truncating the real feed hard: Austria
  // nationally has ~1000-1700+ concurrent train+bus movements (a raw
  // results:3000 call returned 1689, well under that cap, i.e. the true
  // count), of which ~900-1000 are mode:"train" - vs. the ~110-115 that
  // made it through the old 256-item cap after bus-filtering. 2000 gives
  // headroom above the observed high-water mark without asking HAFAS for
  // an unbounded/unnecessarily large response.
  const { movements } = await client.radar(bbox, { results: 2000, duration: 30 });
  process.stdout.write(JSON.stringify(movements));
} catch (err) {
  process.stderr.write(`radar.js failed: ${err.message}\n`);
  process.exit(1);
}
