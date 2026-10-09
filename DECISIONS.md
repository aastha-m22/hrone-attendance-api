# DECISIONS.md

1. **Indexes.** On `attendance_logs`: unique `(emp_code, date)` (one record per day; also serves per-employee lists, the monthly summary, PATCH and every `$lookup`). `(date -1, emp_code 1)` matches the list sort and the month scans of the leaderboard. `(status, date -1, emp_code 1)` covers the status filter. `(emp_code, punch_in -1)` finds the latest open punch for punch-out. On `employees`: unique `emp_code`, `(department, emp_code)`, and `joined_on` for the summary's headcount match. I rejected an index on `late_minutes`: it is a weak filter, and the date range already narrows the scan.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one`. MongoDB enforces the unique index atomically, so one insert succeeds (201) and the other raises `DuplicateKeyError`, which I return as 409. There is no read-before-write.

3. **Ties.** I use `$rank` (competition ranking), then filter `rank <= limit`. Everyone tied at the cutoff is returned, so `limit=2` with totals 50/40/40 gives three rows ranked 1, 2, 2.

4. **Headcount.** The pipeline starts from `employees` (joined on or before month end), not from logs. `$lookup` pulls each person's logs and `$unwind` keeps empty matches, so someone with zero logs still adds 1.

5. **At 100x.** Pre-aggregate daily per-department counters on write (or in a nightly job) instead of scanning raw logs per request, and shard `attendance_logs` on `emp_code`.
