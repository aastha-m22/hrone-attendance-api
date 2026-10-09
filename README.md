# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo). All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_db   # or copy .env.example to .env
uvicorn app.main:app --port 8000
```
Requires MongoDB 6.0+ (`$densify`, `$setWindowFields`, `$documents`, `$lookup` with `localField` + `pipeline`).
Indexes are created at startup (idempotent). If MongoDB is not reachable at startup, the app still starts, and `/health` returns 503 and retries.

## Notes
- Instants are aware UTC `datetime`s internally (`tz_aware=True`), epoch milliseconds at the API boundary, truncated to whole seconds (R1).
- Analytics are aggregation pipelines. Each endpoint and `/admin/explain/{endpoint}` share one pipeline builder, so the explained pipeline is the one that runs.
- Rounding is half-up through `Decimal` (R4/R8). Inside MongoDB, the trend rates use `Decimal128` with `floor(x*10^n + 0.5)`, because `$round` rounds half to even.
- `working_days` (R7) is calendar arithmetic and is done in Python; it does not read any documents.
- PATCH uses optimistic concurrency: the update only applies if the record is unchanged since it was read (status, punch times and history length). Otherwise it re-reads and recomputes, up to 5 times, then returns 409.

## Tests
`tests/check_api.py` seeds an awkward dataset into a **scratch** database (it drops both collections). It starts the app, compares every analytics endpoint with an independent Python reference, fires parallel punch-ins, punch-outs, creates and PATCHes, and checks `/admin/explain` for IXSCAN and no COLLSCAN.
```bash
MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_test python tests/check_api.py         # ~4k records
MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_test python tests/check_api.py --big   # ~100k records
```
