"""
End-to-end checks against a real MongoDB. DROPS the two collections in MONGO_DB, so use a scratch database.

    MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_test python tests/check_api.py          # ~3k records
    MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_test python tests/check_api.py --big    # ~100k records

What it does
  1. Seeds an awkward dataset straight into MongoDB (like the grader): ties, employees with no logs, weekend records,
     mid-month joiners, a joiner after the month, overnight shifts, legacy docs without half_day/history,
     open records, logs whose employee does not exist, a case-variant department name.
  2. Starts the app with uvicorn (so indexes are created on a pre-seeded database) and waits for /health.
  3. Compares every analytics endpoint with an independent pure-Python reference computed from the same documents.
  4. Fires parallel requests at punch-in, punch-out, employee create and PATCH.
  5. Checks /admin/explain for IXSCAN and no COLLSCAN, and times the endpoints.
"""
import json
import os
import random
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from fractions import Fraction

import httpx
from pymongo import MongoClient

BIG = "--big" in sys.argv
BASE = "http://127.0.0.1:8011"
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
PRESENCE = {"PRESENT", "WFH", "ON_DUTY"}
rng = random.Random(42)
failures = []


def check(cond, label, detail=""):
    if not cond:
        failures.append(f"{label}: {detail}")
        print(f"  FAIL {label}: {str(detail)[:400]}")


def half_up(x, places):
    if x is None:
        return None
    d = Decimal(x.numerator) / Decimal(x.denominator) if isinstance(x, Fraction) else Decimal(str(x))
    return float(d.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def days(a, b):
    a, b = date.fromisoformat(a), date.fromisoformat(b)
    return [(a + timedelta(i)).isoformat() for i in range((b - a).days + 1)]


def weekday(d):
    return date.fromisoformat(d).weekday() < 5


# --------------------------------------------------------------------------- dataset
def make_dataset():
    emps, logs = [], []

    def emp(code, dept, joined, shift=("09:30", "18:30")):
        emps.append({"emp_code": code, "name": f"Name {code}", "email": f"{code.lower()}@x.io", "department": dept,
                     "shift_start": shift[0], "shift_end": shift[1], "joined_on": joined,
                     "created_at": datetime(2026, 1, 1, tzinfo=UTC)})

    def log(code, d, status, late=0, ot=0, wh=None, half=None, legacy=False, open_=False):
        doc = {"emp_code": code, "date": d, "status": status}
        if status in PRESENCE:
            pin = datetime.fromisoformat(d + "T09:30:00").replace(tzinfo=IST) + timedelta(minutes=late)
            doc["punch_in"] = pin.astimezone(UTC)
            if open_:
                doc.update(punch_out=None, work_hours=None, late_minutes=late, overtime_minutes=0, half_day=False)
            else:
                wh = wh if wh is not None else round(rng.uniform(3, 10), 2)
                doc.update(punch_out=(pin + timedelta(hours=wh)).astimezone(UTC), work_hours=wh, late_minutes=late,
                           overtime_minutes=ot, half_day=wh < 4.5 if half is None else half)
        else:
            doc.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False)
        doc["history"] = []
        if legacy:
            doc.pop("history"); doc.pop("half_day", None)
        logs.append(doc)

    # Hand-made cases
    emp("EMP0001", "Engineering", "2026-01-05")
    emp("EMP0002", "Engineering", "2026-07-15")                      # mid-month joiner (Wed)
    emp("EMP0003", "Engineering", "2026-01-05")                      # tie with EMP0004
    emp("EMP0004", "Engineering", "2026-01-05")
    emp("EMP0005", "Operations", "2026-01-05", ("22:00", "06:00"))  # overnight
    emp("EMP0006", "Operations", "2026-01-05")                      # no logs at all
    emp("EMP0007", "Operations", "2026-08-10")                      # joins after July
    emp("EMP0008", "engineering", "2026-01-05")                     # case-variant department
    emp("EMP0009", "Future", "2026-08-03")                          # dept with headcount 0 in July
    emp("EMP0010", "Sales", "2026-07-31")                           # joins last day (Fri)
    log("EMP0001", "2026-07-01", "PRESENT", late=15, wh=9.12)
    log("EMP0001", "2026-07-02", "WFH", wh=3.5)                      # half day
    log("EMP0001", "2026-07-04", "PRESENT", late=20, ot=45, wh=6)    # Saturday: late/OT count, presence doesn't
    log("EMP0001", "2026-07-06", "LEAVE")
    log("EMP0001", "2026-07-07", "ABSENT")
    log("EMP0001", "2026-07-08", "ON_DUTY", legacy=True, wh=8.98)
    log("EMP0001", "2026-07-09", "PRESENT", open_=True, late=12)      # open record
    log("EMP0001", "2026-06-30", "PRESENT", late=99)                 # previous month
    log("EMP0001", "2026-08-01", "PRESENT", late=99)                 # next month
    log("EMP0002", "2026-07-15", "PRESENT", late=11)
    log("EMP0002", "2026-07-16", "PRESENT", wh=4.49)
    log("EMP0003", "2026-07-01", "PRESENT", late=30)
    log("EMP0003", "2026-07-02", "PRESENT", late=10)
    log("EMP0004", "2026-07-03", "PRESENT", late=40)
    log("EMP0005", "2026-07-06", "PRESENT", ot=40, wh=8.75)
    log("EMP0005", "2026-07-11", "PRESENT", late=50, wh=8)          # Saturday
    log("EMP0008", "2026-07-01", "PRESENT", late=40)
    log("EMP0010", "2026-07-31", "PRESENT", late=40)
    log("EMP9999", "2026-07-01", "PRESENT", late=500)               # orphan log: employee does not exist
    # Department "Tie": totals 50, 40, 40, 30, 30 -> ranks 1, 2, 2, 4, 4
    for code, lates in [("EMP0020", [50]), ("EMP0021", [25, 15]), ("EMP0022", [40]), ("EMP0023", [30]), ("EMP0024", [12, 18])]:
        emp(code, "Tie", "2026-01-01")
        for i, late in enumerate(lates):
            log(code, f"2026-07-{6 + i:02d}", "PRESENT", late=late)

    # Bulk employees and logs
    n_emp, (d0, d1) = (420, ("2025-08-01", "2026-07-31")) if BIG else (60, ("2026-06-01", "2026-08-31"))
    depts = ["Engineering", "Operations", "Sales", "Finance", "HR"]
    for i in range(n_emp):
        code = f"EMP{100 + i:04d}"
        joined = rng.choice(["2025-06-01", "2026-03-01", f"2026-07-{rng.randint(1, 31):02d}", "2026-09-01"])
        shift = ("22:00", "06:00") if i % 9 == 0 else ("09:30", "18:30")
        emp(code, rng.choice(depts), joined, shift)
        for d in days(d0, d1):
            r = rng.random()
            if r < 0.12 or (not weekday(d) and r < 0.85):
                continue                                              # missing day
            status = rng.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [60, 15, 8, 9, 8])[0]
            late = rng.choice([0, 0, 0, 0, 11, 15, 20, 30, 45]) if status in PRESENCE else 0
            ot = rng.choice([0, 0, 0, 30, 45, 60]) if status in PRESENCE else 0
            log(code, d, status, late=late, ot=ot, legacy=rng.random() < 0.05,
                open_=status in PRESENCE and rng.random() < 0.02)
    return emps, logs


# --------------------------------------------------------------------------- reference implementation
def month_range(month):
    y, m = map(int, month.split("-"))
    end = (date(y + m // 12, m % 12 + 1, 1) - timedelta(1)).isoformat()
    return f"{month}-01", end


def ref_monthly(emp, logs, month):
    start, end = month_range(month)
    ls = [l for l in logs if l["emp_code"] == emp["emp_code"] and start <= l["date"] <= end]
    wd = sum(1 for d in days(max(start, emp["joined_on"]), end) if weekday(d)) if emp["joined_on"] <= end else 0
    present = sum(Fraction(1, 2) if l.get("half_day") else 1 for l in ls if l["status"] in PRESENCE and weekday(l["date"]))
    return {"emp_code": emp["emp_code"], "month": month, "working_days": wd, "present_days": half_up(Fraction(present), 2),
            "leave_days": sum(l["status"] == "LEAVE" for l in ls),
            "late_count": sum((l.get("late_minutes") or 0) > 0 for l in ls),
            "total_late_minutes": sum(l.get("late_minutes") or 0 for l in ls if (l.get("late_minutes") or 0) > 0),
            "total_overtime_minutes": sum(l.get("overtime_minutes") or 0 for l in ls),
            "attendance_pct": half_up(Fraction(present) / wd * 100, 2) if wd else None}


def ref_summary(emps, logs, month, department=None):
    start, end = month_range(month)
    by_dept = defaultdict(list)
    for e in emps:
        if e["joined_on"] <= end and (department is None or e["department"] == department):
            by_dept[e["department"]].append(e["emp_code"])
    items = []
    for dept in sorted(by_dept):
        codes = set(by_dept[dept])
        ls = [l for l in logs if l["emp_code"] in codes and start <= l["date"] <= end]
        wh = [Fraction(Decimal(str(l["work_hours"]))) for l in ls if l["status"] in PRESENCE and l.get("work_hours") is not None]
        present = sum(Fraction(1, 2) if l.get("half_day") else 1 for l in ls if l["status"] in PRESENCE and weekday(l["date"]))
        items.append({"department": dept, "headcount": len(codes), "present_days": half_up(Fraction(present), 2),
                      "avg_work_hours": half_up(sum(wh) / len(wh), 2) if wh else None,
                      "late_count": sum((l.get("late_minutes") or 0) > 0 for l in ls),
                      "total_late_minutes": sum(l["late_minutes"] for l in ls if (l.get("late_minutes") or 0) > 0),
                      "leave_count": sum(l["status"] == "LEAVE" for l in ls),
                      "on_duty_count": sum(l["status"] == "ON_DUTY" for l in ls)})
    return {"month": month, "items": items}


def ref_leaderboard(emps, logs, month, limit=10, department=None):
    start, end = month_range(month)
    emap = {e["emp_code"]: e for e in emps}
    tot = defaultdict(lambda: [0, 0])
    for l in logs:
        if start <= l["date"] <= end and (l.get("late_minutes") or 0) > 0 and l["emp_code"] in emap:
            if department is None or emap[l["emp_code"]]["department"] == department:
                tot[l["emp_code"]][0] += l["late_minutes"]; tot[l["emp_code"]][1] += 1
    rows = sorted(tot.items(), key=lambda kv: (-kv[1][0], kv[0]))
    out = []
    for code, (t, c) in rows:
        rank = 1 + sum(1 for _, (t2, _) in rows if t2 > t)
        if rank <= limit:
            out.append({"rank": rank, "emp_code": code, "name": emap[code]["name"],
                        "department": emap[code]["department"], "total_late_minutes": t, "late_count": c})
    return {"month": month, "items": out}


def ref_trend(emps, logs, dept, frm, to):
    members = [e for e in emps if e["department"] == dept]
    codes = {e["emp_code"] for e in members}
    rows, rates = [], []
    for d in days(frm, to):
        hc = sum(e["joined_on"] <= d for e in members)
        ls = [l for l in logs if l["emp_code"] in codes and l["date"] == d]
        present = sum(Fraction(1, 2) if l.get("half_day") else 1 for l in ls if l["status"] in PRESENCE)
        rate = half_up(Fraction(present) / hc, 4) if weekday(d) and hc > 0 else None
        rates.append(rate)
        window = [Decimal(str(r)) for r in rates[-7:] if r is not None]
        mav = half_up(sum(window) / len(window), 4) if window else None
        rows.append({"date": d, "is_working_day": weekday(d), "headcount": hc, "present_count": float(present),
                     "late_count": sum((l.get("late_minutes") or 0) > 0 for l in ls), "attendance_rate": rate,
                     "moving_avg_7d": mav})
    return {"department": dept, "items": rows}


# --------------------------------------------------------------------------- run
def main():
    uri, dbname = os.environ["MONGO_URI"], os.environ["MONGO_DB"]
    db = MongoClient(uri, tz_aware=True)[dbname]
    emps, logs = make_dataset()
    db.employees.drop(); db.attendance_logs.drop()
    db.employees.insert_many([dict(e) for e in emps])
    for i in range(0, len(logs), 10000):
        db.attendance_logs.insert_many([dict(l) for l in logs[i:i + 10000]])
    print(f"seeded {len(emps)} employees, {len(logs)} logs")

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "app.main:app", "--port", "8011"], cwd=root,
                            env={**os.environ}, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:
        t0 = time.time()
        while time.time() - t0 < 20:
            try:
                if httpx.get(BASE + "/health").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.3)
        print(f"ready in {time.time() - t0:.1f}s")
        run_checks(db, emps, logs)
    finally:
        proc.terminate()
    print("\n" + ("ALL CHECKS PASSED" if not failures else f"{len(failures)} FAILURES"))
    sys.exit(1 if failures else 0)


def run_checks(db, emps, logs):
    c = httpx.Client(base_url=BASE, timeout=60)
    month = "2026-07"

    print("analytics vs reference")
    for e in emps[:80]:
        got = c.get(f"/analytics/employees/{e['emp_code']}/monthly", params={"month": month}).json()
        check(got == ref_monthly(e, logs, month), f"monthly {e['emp_code']}", (got, ref_monthly(e, logs, month)))
    check(c.get("/analytics/employees/EMP9999/monthly", params={"month": month}).status_code == 404, "monthly 404")
    check(c.get("/analytics/employees/EMP0001/monthly", params={"month": "2026-13"}).status_code == 422, "monthly 422")
    for dept in [None, "Engineering", "engineering", "Future", "Nope"]:
        p = {"month": month} | ({"department": dept} if dept else {})
        got, exp = c.get("/analytics/departments/summary", params=p).json(), ref_summary(emps, logs, month, dept)
        check(got == exp, f"summary {dept}", (got, exp))
    for dept in [None, "Engineering", "Operations", "Tie"]:
        for limit in [1, 2, 3, 4, 5, 10, 50]:
            p = {"month": month, "limit": limit} | ({"department": dept} if dept else {})
            got, exp = c.get("/analytics/leaderboard/late", params=p).json(), ref_leaderboard(emps, logs, month, limit, dept)
            check(got == exp, f"leaderboard {dept} {limit}", (got, exp))
    tie = c.get("/analytics/leaderboard/late", params={"month": month, "department": "Tie", "limit": 2}).json()["items"]
    check([(r["rank"], r["emp_code"]) for r in tie] == [(1, "EMP0020"), (2, "EMP0021"), (2, "EMP0022")], "tie at cutoff", tie)
    for dept, frm, to in [("Engineering", "2026-07-01", "2026-07-31"), ("Operations", "2026-07-03", "2026-07-20"),
                          ("Future", "2026-07-25", "2026-08-10"), ("Sales", "2026-07-31", "2026-07-31"),
                          ("Engineering", "2026-06-01", "2026-08-31")]:
        got = c.get(f"/analytics/departments/{dept}/trend", params={"from": frm, "to": to}).json()
        exp = ref_trend(emps, logs, dept, frm, to)
        check(got == exp, f"trend {dept} {frm}..{to}",
              next(((g, x) for g, x in zip(got.get("items", []), exp["items"]) if g != x), (len(got.get("items", [])), got)))
    check(c.get("/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-02"}).status_code == 404, "trend 404")
    check(c.get("/analytics/departments/Sales/trend", params={"from": "2026-07-02", "to": "2026-07-01"}).status_code == 422, "trend to<from")
    check(c.get("/analytics/departments/Sales/trend", params={"from": "2026-01-01", "to": "2026-04-03"}).status_code == 422, "trend 93 days")
    check(c.get("/analytics/departments/Sales/trend", params={"from": "2026-07-01", "to": "2026-09-30"}).status_code == 200, "trend 92d ok")

    print("lists")
    lst = c.get("/attendance", params={"page_size": 100}).json()
    check(lst["total"] == len(logs), "attendance total unfiltered", (lst["total"], len(logs)))
    exp = sorted(logs, key=lambda l: (l["emp_code"]))
    exp = sorted(exp, key=lambda l: l["date"], reverse=True)[:100]
    check([(x["date"], x["emp_code"]) for x in lst["items"]] == [(l["date"], l["emp_code"]) for l in exp], "attendance sort")
    for p, f in [({"status": "LEAVE"}, lambda l: l["status"] == "LEAVE"),
                 ({"emp_code": "EMP0001", "date_from": "2026-07-01", "date_to": "2026-07-31"},
                  lambda l: l["emp_code"] == "EMP0001" and "2026-07-01" <= l["date"] <= "2026-07-31")]:
        check(c.get("/attendance", params=p).json()["total"] == sum(map(f, logs)), f"attendance total {p}")
    emp_page = c.get("/employees", params={"department": "Engineering", "page": 2, "page_size": 5}).json()
    eng = sorted(e["emp_code"] for e in emps if e["department"] == "Engineering")
    check(emp_page["total"] == len(eng) and [x["emp_code"] for x in emp_page["items"]] == eng[5:10], "employees page 2")

    print("concurrency")
    c.post("/employees", json={"emp_code": "EMP5000", "name": "Race", "email": "r@x.io", "department": "Race",
                               "joined_on": "2026-01-01"})
    pin = int(datetime(2026, 9, 1, 9, 0, tzinfo=IST).timestamp() * 1000)
    with ThreadPoolExecutor(20) as ex:
        codes = list(ex.map(lambda i: c.post("/attendance/punch-in", json={"emp_code": "EMP5000", "punched_at": pin + i * 1000}).status_code, range(20)))
    check(sorted(codes) == [201] + [409] * 19, "parallel punch-in", codes)
    with ThreadPoolExecutor(20) as ex:
        codes = list(ex.map(lambda i: c.post("/attendance/punch-out", json={"emp_code": "EMP5000", "punched_at": pin + 9 * 3600000 + i}).status_code, range(20)))
    check(sorted(codes) == [200] + [409] * 19, "parallel punch-out", codes)
    with ThreadPoolExecutor(10) as ex:
        codes = list(ex.map(lambda i: c.patch("/attendance/EMP5000/2026-09-01", json={
            "punch_out": pin + 8 * 3600000 + i * 60000, "reason": f"fix number {i}", "regularized_by": "hr"}).status_code, range(10)))
    hist = c.get("/attendance", params={"emp_code": "EMP5000"}).json()["items"][0]["history"]
    check(len(hist) == codes.count(200) and set(codes) <= {200, 409, 422}, "parallel patch keeps history", (codes, len(hist)))
    with ThreadPoolExecutor(10) as ex:
        codes = list(ex.map(lambda i: c.post("/employees", json={"emp_code": "EMP5001", "name": "D", "email": "d@x.io",
                                                                  "department": "Race", "joined_on": "2026-01-01"}).status_code, range(10)))
    check(sorted(codes) == [201] + [409] * 9, "parallel employee create", codes)

    print("explain and timing")
    params = {
        "attendance_list": [{}, {"emp_code": "EMP0100"}, {"date_from": "2026-07-01", "date_to": "2026-07-31"},
                            {"status": "LEAVE"}, {"emp_code": "EMP0100", "status": "PRESENT", "page": 2}],
        "employee_monthly": [{"emp_code": "EMP0100", "month": month}],
        "department_summary": [{"month": month}, {"month": month, "department": "Engineering"}],
        "late_leaderboard": [{"month": month}, {"month": month, "department": "Sales", "limit": 3}],
        "department_trend": [{"department": "Engineering", "from": "2026-05-01", "to": "2026-07-31"}],
    }
    for ep, plist in params.items():
        for p in plist:
            r = c.get(f"/admin/explain/{ep}", params=p)
            text = json.dumps(r.json())
            check(r.status_code == 200 and "IXSCAN" in text and "COLLSCAN" not in text, f"explain {ep} {p}",
                  (r.status_code, text[:300]))
    check(c.get("/admin/explain/employee_monthly", params={"month": month}).status_code == 422, "explain missing param")
    for path, p in [("/attendance", {}), ("/attendance", {"status": "LEAVE", "page": 50}),
                    ("/analytics/departments/summary", {"month": month}),
                    ("/analytics/leaderboard/late", {"month": month}),
                    ("/analytics/departments/Engineering/trend", {"from": "2026-05-01", "to": "2026-07-31"})]:
        t0 = time.time(); r = c.get(path, params=p); ms = (time.time() - t0) * 1000
        print(f"  {r.status_code} {ms:7.1f} ms  {path} {p}")
        check(r.status_code == 200, f"status {path}")


if __name__ == "__main__":
    main()
