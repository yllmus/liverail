// One-shot script: fetch current ÖBB vehicle positions and print them as JSON.
// Run fresh for each call by austria_poller.py and by /live?country=AT rather
// than as a persistent server, because the server had little memory to spare.
// hafas-client is ESM-only, so this file is ESM too.
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
  // radar() resolves to {movements, realtimeDataUpdatedAt}; only the array is
  // printed, which is what parse_at_vehicles() expects.
  //
  // `results` defaults to 256 in hafas-client, far below the number of
  // vehicles moving in Austria at once, so the default silently dropped most
  // of the national feed. 2000 later proved too low at evening peak too.
  // monitor.py flags a response that comes back exactly at the cap, because
  // this limit will need raising again as traffic grows.
  const { movements } = await client.radar(bbox, { results: 4000, duration: 30 });
  process.stdout.write(JSON.stringify(movements));
} catch (err) {
  process.stderr.write(`radar.js failed: ${err.message}\n`);
  process.exit(1);
}
