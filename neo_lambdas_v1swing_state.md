# Neo Lambda Deployment State — V1 Swing

## psycopg2 Layer Issue (2026-04-24)

**Root cause:** Layer `psycopg2-py312:1` (arn:aws:lambda:eu-west-2:956177812472:layer:psycopg2-py312:1)
contains a CPython 3.10 binary despite its name suggesting py3.12.
Lambdas on Python 3.12 runtime fail with `No module named '\''psycopg2._psycopg'\''`.

**Fix:** Bundle `psycopg2-binary==2.9.12` (manylinux2014_x86_64, cp312) directly
into the deployment zip. Remove the broken layer.

**Lambdas fixed:**
- `neo-admin-signals-dev` — fixed during V1 Swing alignment (2026-04-24), layer still attached (layer is shadowed by bundled copy)
- `neo-admin-proposals-dev` — fixed 2026-04-24; layer cleared + new zip deployed (CodeSize: 4284745)

**Lambdas still using broken layer psycopg2-py312:1 on py3.12 — AT RISK:**
- neo-dash-account-dev
- neo-admin-trades-dev
- neo-admin-audit-download-dev
- neo-dash-calendar-dev
- neo-dash-drawdown-dev
- neo-admin-learning-dev
- neo-admin-health-dev
- neo-admin-audit-dev
- neo-admin-signals-dev (layer attached; bundled copy in zip should shadow it)
- neo-admin-capital-events-dev
- neo-admin-trades-download-dev
- neo-admin-agents-dev
- neo-admin-alerts-dev
- neo-admin-stats-dev
- neo-dash-trades-download-dev
- neo-dash-trades-dev
- neo-dash-positions-dev

**Build command (reusable for all at-risk Lambdas):**
```bash
pip install \
    --platform manylinux2014_x86_64 \
    --target build/ \
    --implementation cp \
    --python-version 3.12 \
    --only-binary=:all: \
    psycopg2-binary==2.9.12
```

**Verified .so:** `_psycopg.cpython-312-x86_64-linux-gnu.so`

**Test invocation result (2026-04-24):**
- StatusCode: 200, Body keys: [proposals, total, pending_count], No error in body
- No psycopg2 errors in CloudWatch (last 5 min)

**CloudFront invalidation:** I7Q3MSJCI2XHUHSLQL2U4P2QQD (InProgress, dist E16A8HVTFGN2V0)
