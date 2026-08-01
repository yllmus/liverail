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
  const { movements } = await client.radar(bbox, { results: 256, duration: 30 });
  process.stdout.write(JSON.stringify(movements));
} catch (err) {
  process.stderr.write(`radar.js failed: ${err.message}\n`);
  process.exit(1);
}
