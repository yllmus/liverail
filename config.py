import os

# Absolute, derived from this file's own location rather than the process's
# working directory. The systemd units all set WorkingDirectory=/home/ubuntu
# so a relative ".env" happened to work, but anything run from elsewhere (a
# cron entry with a different cwd, a restore on a freshly rebuilt box, a
# one-off script) would silently fall through to no .env at all.
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def _load_env_file(path=_ENV_PATH):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

_load_env_file()

# No default. This used to fall back to a hardcoded literal, which meant a
# missing or unreadable .env did not fail - it silently connected with a
# known password instead, and that literal is in this repo's history back to
# the initial commit (see NOTES.md, "Rotating the DB password"). A restore
# that forgets to write .env should stop immediately and say so, not come up
# half-working against a guessable credential.
DB_PASSWORD = os.environ.get("TRAIN_DB_PASSWORD")
if not DB_PASSWORD:
    raise RuntimeError(
        f"TRAIN_DB_PASSWORD is not set. Expected it in {_ENV_PATH} or the "
        "environment. See .env.example; on a fresh restore, copy it to .env "
        "and set a real password matching the trainuser role in Postgres."
    )

# Web Push (VAPID). Optional, unlike the DB password: alerts are a feature, and
# a box without keys should serve the rest of the site normally rather than
# refuse to start. push_alerts and /push/* check PUSH_ENABLED and report the
# feature as unavailable instead of raising.
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY")
VAPID_SUBJECT = os.environ.get("VAPID_SUBJECT", "mailto:admin@example.com")
PUSH_ENABLED = bool(VAPID_PRIVATE_KEY and VAPID_PUBLIC_KEY)
