# Testing

```bash
pytest           # everything that can run in this environment; the rest skips with a reason
pytest -m slow   # only the slow whole-country tests
pytest -m ""     # everything, slow tests included
```

## What runs where

`tests/conftest.py` probes the database once and skips what cannot run, so a fresh clone needs no setup:

| Environment | Runs | Skips |
|---|---|---|
| No database | 183 unit and mocked API tests | 40 tests marked `db` or `real_data` |
| Empty schema (as in CI) | 213 tests | 19 tests marked `real_data` |
| Production database | all 232 | none |

Unit tests also run without a `.env`: `conftest.py` sets a placeholder `TRAIN_DB_PASSWORD` when no real one is configured.

| | What it covers | Needs |
|---|---|---|
| Unit tests | Every poller's `parse_*` function against fixtures, station and route name cleaning, trip resolution, the alert decision logic, the model promotion gate, the transfer-model feature row | Nothing. Resolve steps run against a fake cursor. |
| API tests (`tests/test_api.py`) | Endpoints through FastAPI's `TestClient`, with outbound HTTP and Node subprocess calls mocked | Some need nothing, `db` tests need the schema, `real_data` tests need real observations |

This split follows from the adapter design in [data-sources.md](data-sources.md): because each `parse_*` function is pure, most of the logic can be tested without a database or a network. A few tests guard mistakes that are easy to repeat:

- **Route order.** `/train/{country}/{train_id:path}` is greedy and swallowed `/train/DE/x/history` until the more specific route was registered first. A test fails if the order changes.
- **Monotonic transfer risk.** The probability of a successful change never decreases as the buffer grows.
- **No train/serve skew.** The served feature row is compared with the training feature row on identical inputs.
- **Gate refusals.** Marginal gains, tight-change regressions, losing to the base rate, bad calibration, and a tight win bought with an overall loss are each rejected.

## CI

`.github/workflows/tests.yml` starts PostgreSQL, builds the database exactly as the README's "Running it" section does (schema, migrations, reference data, countries), and runs `pytest -m ""`. Every endpoint test that does not depend on real observations therefore runs against the real schema on every push.

## Why some API tests are slow

Five tests (`/sitemap.xml`, `/operators/summary` for three countries, `/routes/summary`) aggregate a whole country. Against the production database of about 16 million rows that takes minutes, because the API's summary cache lives in the process and every test run starts cold; `/sitemap.xml` alone warms the route list for all four countries. They are excluded by default and still worth running before a change to `api.py`: the sitemap endpoint once shipped a 504 that only they would have caught. Against the empty CI schema they take a fraction of a second.

One finding worth keeping: the suite went from over five minutes to 8.9 seconds after the slow tests were marked, and then to 1.5 seconds after an index was added for the Polish and German pollers' duplicate check. The tests had been competing with that query for disk.

## What is still missing

The 19 `real_data` tests only run against the production database. A small fixture dataset loaded on top of the schema (a few stations, routes and a few thousand observations) would let them run in CI too, and would make their assertions exact instead of loose, since today they run against live data that changes every five minutes.
