# Testing

```bash
pytest tests/             # fast suite: pure functions and fake cursors, about a second
pytest tests/ -m slow     # the whole-country API tests, need a populated database
pytest tests/ -m ""       # everything
```

`pytest.ini` sets `addopts = -m "not slow"`, so a bare `pytest tests/` skips the slow tests.

## Two kinds of test

| | What it covers | Needs |
|---|---|---|
| Unit tests (about 140) | Every poller's `parse_*` function against fixtures, station and route name cleaning, trip resolution, the alert decision logic, the model promotion gate, the transfer-model feature row | Nothing. Resolve steps run against a fake cursor. |
| API tests (`tests/test_api.py`) | Endpoints through FastAPI's `TestClient`, with outbound HTTP and Node subprocess calls mocked | A real PostgreSQL with the schema and data |

This split follows from the adapter design in [data-sources.md](data-sources.md): because each `parse_*` function is pure, most of the logic can be tested without a database or a network. A few tests guard mistakes that are easy to repeat:

- **Route order.** `/train/{country}/{train_id:path}` is greedy and swallowed `/train/DE/x/history` until the more specific route was registered first. A test fails if the order changes.
- **Monotonic transfer risk.** The probability of a successful change never decreases as the buffer grows.
- **No train/serve skew.** The served feature row is compared with the training feature row on identical inputs.
- **Gate refusals.** Marginal gains, tight-change regressions, losing to the base rate, bad calibration, and a tight win bought with an overall loss are each rejected.

## Why some API tests are slow

Five tests (`/sitemap.xml`, `/operators/summary` for three countries, `/routes/summary`) aggregate a whole country out of about 16 million rows. The API's summary cache lives in the process, so every fresh pytest run starts cold and pays full price. `/sitemap.xml` alone warms the route list for all four countries, which takes 60 to 90 seconds cold.

This was diagnosed by measurement. The suspects that turned out innocent were the cache-warming thread (it does not run under a plain `TestClient`), the search-index rebuild (it has a 20-hour staleness threshold), and import cost.

One finding worth keeping: the suite went from over five minutes to 8.9 seconds after the slow tests were marked, and then to 1.5 seconds after an index was added for the Polish and German pollers' duplicate check. The tests had been competing with that query for disk. Most of the remaining runtime had been contention, not the tests.

The slow tests are still worth running before a change to `api.py`. The sitemap endpoint once shipped a 504 that only they would have caught.

## What was not done

The right fix is a seeded test database: apply `schema/schema.sql` to an empty database, load a few thousand fixture rows, and point the API at it. Every aggregate would be instant, and assertions could be exact instead of loose, because today's tests run against live data that changes every five minutes. It was not built before the project stopped.
