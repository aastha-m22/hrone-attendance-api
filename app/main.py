"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is loaded for convenience; real environment variables win)

Layout of this file
  1. Configuration and database client
  2. Time helpers (epoch ms <-> datetime, IST, R1 attendance date)
  3. Business rules R2-R5 (pure functions, no database)
  4. Request models and input validation
  5. Serialisation (Mongo document -> API JSON)
  6. Index creation and app startup
  7. Employees and attendance endpoints
  8. Analytics: pipeline builders (shared by the endpoints and by /admin/explain)
  9. Analytics endpoints
 10. /admin/explain
"""
import json
import logging
import os
import re
from calendar import monthrange
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, BaseModel, Field, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration and database client
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # never overrides variables that are already set in the environment

MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB")
if not MONGO_URI or not MONGO_DB:
    raise RuntimeError("MONGO_URI and MONGO_DB must be set (environment or .env)")

# tz_aware=True: PyMongo returns timezone-aware UTC datetimes instead of naive ones, so date
# arithmetic with IST values is always correct and never depends on the server's local zone.
client = MongoClient(MONGO_URI, tz_aware=True, tzinfo=timezone.utc, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]
employees_col = db["employees"]
logs_col = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

EPOCH_MS_MIN = 100_000_000_000  # below this the value looks like seconds
EPOCH_MS_MAX = 4_102_444_800_000  # 2100-01-01

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
NON_PRESENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_BELOW_HOURS = 4.5
MAX_SHIFT = timedelta(hours=24)
TREND_MAX_DAYS = 92

Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """Aware (or naive-UTC) datetime -> epoch milliseconds. Integer maths only, no float rounding."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def truncate_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def now_utc() -> datetime:
    return datetime.now(UTC)


def parse_hhmm(value: str) -> time:
    h, m = value.split(":")
    return time(int(h), int(m))


def is_overnight(emp: dict) -> bool:
    # "HH:MM" strings compare correctly as text
    return emp["shift_end"] <= emp["shift_start"]


def attendance_date(punch_in: datetime, emp: dict) -> str:
    """R1: the IST calendar date of the punch-in, except that on an overnight shift a punch-in
    earlier than shift_end belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp) and local.time() < parse_hhmm(emp["shift_end"]):
        day -= timedelta(days=1)
    return day.isoformat()


def shift_start_dt(date_str: str, emp: dict) -> datetime:
    return datetime.combine(date.fromisoformat(date_str), parse_hhmm(emp["shift_start"]), tzinfo=IST)


def shift_end_dt(date_str: str, emp: dict) -> datetime:
    end = datetime.combine(date.fromisoformat(date_str), parse_hhmm(emp["shift_end"]), tzinfo=IST)
    if is_overnight(emp):
        end += timedelta(days=1)  # R1: an overnight shift ends on the next calendar day
    return end


def to_decimal(value: Any) -> Decimal:
    """int / float / Decimal128 -> Decimal. Floats go through their shortest repr, so 2.675 stays 2.675."""
    if isinstance(value, Decimal128):
        return value.to_decimal()
    return value if isinstance(value, Decimal) else Decimal(str(value))


def round_half_up(value: Any, places: int) -> Optional[float]:
    """R8 rounding: half-up, unlike Python's round() (half-even on an inexact float): 2.675 -> 2.68."""
    if value is None:
        return None
    return float(to_decimal(value).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


# --------------------------------------------------------------------------- #
# 3. Business rules R2-R5 (inputs are already truncated to whole seconds)
# --------------------------------------------------------------------------- #
def compute_late_minutes(punch_in: datetime, date_str: str, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor minutes since shift start."""
    seconds = int((punch_in - shift_start_dt(date_str, emp)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_overtime_minutes(punch_out: datetime, date_str: str, emp: dict) -> int:
    """R3: floor minutes from shift end to punch-out, only if >= 30."""
    minutes = int((punch_out - shift_end_dt(date_str, emp)).total_seconds()) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up (Decimal, not float round())."""
    seconds = int((punch_out - punch_in).total_seconds())
    hours = (Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours)


def derive_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime],
                  date_str: str, emp: dict) -> dict:
    """All four derived fields for a record, following the table in DATA_MODEL.md."""
    if status in NON_PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, date_str, emp)
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    work_hours = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": work_hours,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(punch_out, date_str, emp),
        "half_day": work_hours < HALF_DAY_BELOW_HOURS,  # R5 uses the rounded value
    }


# --------------------------------------------------------------------------- #
# 4. Request models and input validation
# --------------------------------------------------------------------------- #
# strict=True: rejects floats (1.7e12), strings ("1783312500000") and booleans with 422.
EpochMs = Annotated[int, Field(strict=True, ge=EPOCH_MS_MIN, le=EPOCH_MS_MAX)]

DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


def _valid_date(value: str) -> str:
    if not DATE_RE.match(value):
        raise ValueError("must be a date in YYYY-MM-DD format")
    date.fromisoformat(value)  # raises ValueError for 2026-02-30
    return value


DateStr = Annotated[str, AfterValidator(_valid_date)]


def validation_error(loc: tuple, msg: str) -> RequestValidationError:
    """A 422 with FastAPI's default body shape ({"detail": [ {...} ]})."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


class EmployeeIn(BaseModel):
    emp_code: Annotated[str, Field(pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: Annotated[str, Field(pattern=HHMM_PATTERN)] = "09:30"
    shift_end: Annotated[str, Field(pattern=HHMM_PATTERN)] = "18:30"
    joined_on: DateStr

    @model_validator(mode="after")
    def _shift_not_empty(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    # Derived fields (work_hours, late_minutes, ...) are not declared, so they are ignored if sent.
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # Omitted means "unchanged"; an explicit null is not a valid value for these fields.
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} may be omitted but not null")
        return self


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    return f"{month}-01", f"{month}-{monthrange(year, mon)[1]:02d}"


# --------------------------------------------------------------------------- #
# 5. Serialisation
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _ms_if_dt(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    changes = {
        field: {"from": _ms_if_dt(c.get("from")), "to": _ms_if_dt(c.get("to"))}
        for field, c in (entry.get("changes") or {}).items()
    }
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def _int_or_zero(value: Any) -> int:
    return int(value) if value is not None else 0


def record_out(doc: dict) -> dict:
    """Stored record -> API record. Missing legacy fields get their documented defaults."""
    work_hours = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": float(work_hours) if work_hours is not None else None,
        "late_minutes": _int_or_zero(doc.get("late_minutes")),
        "overtime_minutes": _int_or_zero(doc.get("overtime_minutes")),
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 6. Indexes and startup
# --------------------------------------------------------------------------- #
INDEXES = {
    "employees": [
        ([("emp_code", ASCENDING)], {"name": "emp_code_unique", "unique": True}),
        ([("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "department_emp_code"}),
        ([("joined_on", ASCENDING)], {"name": "joined_on"}),
    ],
    "attendance_logs": [
        # one record per employee per day; also serves every per-employee query and $lookup
        ([("emp_code", ASCENDING), ("date", ASCENDING)], {"name": "emp_code_date_unique", "unique": True}),
        # list without emp filter (sort date desc, emp_code asc) and month-wide range scans
        ([("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_emp_code"}),
        # list filtered by status, same sort
        ([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp_code"}),
        # punch-out: latest record with punch_in <= t
        ([("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "emp_code_punch_in"}),
    ],
}
_indexes_ready = False


def ensure_indexes() -> None:
    """create_index is idempotent: it is a no-op when an identical index already exists."""
    global _indexes_ready
    for collection, specs in INDEXES.items():
        for keys, options in specs:
            db[collection].create_index(keys, **options)
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # MongoDB not reachable yet: start anyway, /health retries
        log.warning("index creation deferred: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
def _mongo_error(_request, exc: PyMongoError):
    log.error("database error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 7. Employees and attendance endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})
    return {"status": "ok"}


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees_col.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    created = now_utc()
    doc["created_at"] = created.replace(microsecond=created.microsecond // 1000 * 1000)  # BSON keeps ms
    try:
        employees_col.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    q = {"department": department} if department is not None else {}
    total = employees_col.count_documents(q)
    cursor = employees_col.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else truncate_seconds(now_utc())
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": emp["emp_code"],
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive_fields(body.status, ts, None, day, emp),
        "history": [],
    }
    try:
        logs_col.insert_one(doc)  # unique (emp_code, date): concurrent duplicates get DuplicateKeyError
    except DuplicateKeyError:
        raise HTTPException(409, "a record already exists for this employee and date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else truncate_seconds(now_utc())

    # Most recent record whose punch_in is at or before the punch-out (works across midnight).
    record = logs_col.find_one(
        {"emp_code": emp["emp_code"], "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)]
    )
    if record is None or record.get("punch_out") is not None:
        # An open record that starts *after* punched_at means the punch-out is before the punch-in.
        later_open = logs_col.find_one(
            {"emp_code": emp["emp_code"], "punch_in": {"$gt": ts}, "punch_out": None}, projection={"_id": 1}
        )
        if later_open is not None:
            raise validation_error(("body", "punched_at"), "punched_at must be after punch_in")
        if record is None:
            raise HTTPException(404, "no punch-in found")
        raise HTTPException(409, "already punched out")

    pin = truncate_seconds(record["punch_in"])
    if ts <= pin:
        raise validation_error(("body", "punched_at"), "punched_at must be after punch_in")
    if ts - pin > MAX_SHIFT:
        raise validation_error(("body", "punched_at"), "punched_at is more than 24 hours after punch_in")

    derived = derive_fields(record["status"], pin, ts, record["date"], emp)
    # Conditional update: only the request that still sees punch_out == null wins.
    updated = logs_col.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": {"punch_out": ts, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def build_attendance_query(emp_code, date_from, date_to, status) -> dict:
    if date_from and date_to and date_from > date_to:
        raise validation_error(("query", "date_from"), "date_from must not be after date_to")
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from or date_to:
        q["date"] = {}
        if date_from:
            q["date"]["$gte"] = date_from
        if date_to:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    return q


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    q = build_attendance_query(emp_code, date_from, date_to, status)
    total = logs_col.count_documents(q)  # exact; estimated_document_count() can be stale (R10)
    cursor = logs_col.find(q).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


TRACKED_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")
PATCH_RETRIES = 5


def _current_values(record: dict) -> dict:
    """The tracked fields of a stored record, with missing legacy fields at their defaults."""
    return {
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "work_hours": record.get("work_hours"),
        "late_minutes": _int_or_zero(record.get("late_minutes")),
        "overtime_minutes": _int_or_zero(record.get("overtime_minutes")),
        "half_day": bool(record.get("half_day") or False),
    }


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: DateStr, body: RegularizeIn):
    emp = get_employee_or_404(emp_code)

    # Optimistic concurrency: compute from what we read, then write only if the record is still
    # exactly what we read. If another correction got in first, re-read and recompute.
    for _ in range(PATCH_RETRIES):
        record = logs_col.find_one({"emp_code": emp_code, "date": date})
        if record is None:
            raise HTTPException(404, "no attendance record for this date")
        old = _current_values(record)

        status = body.status or old["status"]
        if status in NON_PRESENCE:
            if body.punch_in is not None or body.punch_out is not None:
                raise validation_error(("body",), f"punch times cannot be set with status {status}")
            pin, pout = None, None
        else:
            pin = ms_to_dt(body.punch_in) if body.punch_in is not None else old["punch_in"]
            pout = ms_to_dt(body.punch_out) if body.punch_out is not None else old["punch_out"]
            if pin is None:
                raise validation_error(("body", "punch_in"), f"status {status} requires a punch_in")
            pin = truncate_seconds(pin)
            if body.punch_in is not None and attendance_date(pin, emp) != date:
                raise validation_error(("body", "punch_in"), "punch_in must stay on the record's attendance date")
            if pout is not None:
                pout = truncate_seconds(pout)
                if pout <= pin:
                    raise validation_error(("body", "punch_out"), "punch_out must be after punch_in")
                if pout - pin > MAX_SHIFT:
                    raise validation_error(("body", "punch_out"), "punch_out must be within 24 hours of punch_in")

        new = {"status": status, "punch_in": pin, "punch_out": pout, **derive_fields(status, pin, pout, date, emp)}
        changes = {f: {"from": old[f], "to": new[f]} for f in TRACKED_FIELDS if old[f] != new[f]}
        if not changes:
            raise validation_error(("body",), "the request changes nothing")

        entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        guard = {
            "_id": record["_id"],
            "status": record["status"],
            "punch_in": record.get("punch_in"),
            "punch_out": record.get("punch_out"),
            # history only ever grows, so its length is a version number
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(record.get("history") or [])]},
        }
        updated = logs_col.find_one_and_update(
            guard, {"$set": new, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
        )
        if updated is not None:
            return record_out(updated)
    raise HTTPException(409, "the record was modified concurrently, please retry")


# --------------------------------------------------------------------------- #
# 8. Analytics pipeline builders
#    Each returns (collection, pipeline). The endpoint and /admin/explain call the same builder.
# --------------------------------------------------------------------------- #
def _date_of(field: str = "$date") -> dict:
    return {"$dateFromString": {"dateString": field, "format": "%Y-%m-%d", "timezone": "UTC"}}


IS_PRESENCE = {"$in": ["$status", list(PRESENCE)]}
IS_WEEKDAY = {"$lte": [{"$isoDayOfWeek": _date_of()}, 5]}  # ISO: Mon=1 ... Sun=7
DAY_VALUE = {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1]}  # R5: a half day is 0.5
LATE = {"$ifNull": ["$late_minutes", 0]}
IS_LATE = {"$gt": [LATE, 0]}


def _count_if(cond: dict) -> dict:
    return {"$sum": {"$cond": [cond, 1, 0]}}


def log_stats_group(group_id: Any) -> dict:
    """$group stage with the per-record metrics shared by the monthly and department summaries."""
    has_hours = {"$and": [IS_PRESENCE, {"$isNumber": "$work_hours"}]}
    return {
        "$group": {
            "_id": group_id,
            "present_days": {"$sum": {"$cond": [{"$and": [IS_PRESENCE, IS_WEEKDAY]}, DAY_VALUE, 0]}},
            "leave_count": _count_if({"$eq": ["$status", "LEAVE"]}),
            "on_duty_count": _count_if({"$eq": ["$status", "ON_DUTY"]}),
            "late_count": _count_if(IS_LATE),
            "total_late_minutes": {"$sum": {"$cond": [IS_LATE, LATE, 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            # Decimal128 sum, so the mean is exact before half-up rounding
            "work_hours_sum": {"$sum": {"$cond": [has_hours, {"$toDecimal": "$work_hours"}, Decimal128("0")]}},
            "work_hours_count": _count_if(has_hours),
        }
    }


def employee_monthly_pipeline(emp_code: str, month: str) -> tuple[str, list]:
    start, end = month_bounds(month)
    return "attendance_logs", [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        log_stats_group(None),
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> tuple[str, list]:
    start, end = month_bounds(month)
    match: dict = {"joined_on": {"$lte": end}}  # R9: joined on or before the last day of the month
    if department is not None:
        match["department"] = department
    return "employees", [
        # Start from employees, not logs, so people with zero logs are part of the headcount.
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [{"$match": {"date": {"$gte": start, "$lte": end}}}, log_stats_group(None)],
            "as": "stats",
        }},
        {"$unwind": {"path": "$stats", "preserveNullAndEmptyArrays": True}},  # keep employees with no logs
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": {"$ifNull": ["$stats.present_days", 0]}},
            "work_hours_sum": {"$sum": {"$ifNull": ["$stats.work_hours_sum", Decimal128("0")]}},
            "work_hours_count": {"$sum": {"$ifNull": ["$stats.work_hours_count", 0]}},
            "late_count": {"$sum": {"$ifNull": ["$stats.late_count", 0]}},
            "total_late_minutes": {"$sum": {"$ifNull": ["$stats.total_late_minutes", 0]}},
            "leave_count": {"$sum": {"$ifNull": ["$stats.leave_count", 0]}},
            "on_duty_count": {"$sum": {"$ifNull": ["$stats.on_duty_count", 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> tuple[str, list]:
    start, end = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}],
            "as": "emp",
        }},
        {"$unwind": "$emp"},  # drops logs whose employee does not exist
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department
    pipeline += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking, so ties at the cutoff stay
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
            "total_late_minutes": 1, "late_count": 1,
        }},
    ]
    return "attendance_logs", pipeline


def _round_half_up_expr(expr: Any, places: int) -> dict:
    """Half-up rounding inside MongoDB ($round is half-to-even). Decimal128 keeps it exact; values are >= 0."""
    scale = Decimal128(str(10 ** places))
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, scale]}, Decimal128("0.5")]}}, scale]}


def department_trend_pipeline(department: str, date_from: str, date_to: str) -> tuple[str, list]:
    upper = (date.fromisoformat(date_to) + timedelta(days=1)).isoformat()  # $densify upper bound is exclusive
    log_item = {
        "date": "$$log.date",
        "joins": 0,
        "present": {"$cond": [{"$in": ["$$log.status", list(PRESENCE)]},
                              {"$cond": [{"$ifNull": ["$$log.half_day", False]}, 0.5, 1]}, 0]},
        "late": {"$cond": [{"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]}, 1, 0]},
    }
    return "employees", [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": date_from, "$lte": date_to}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1, "late_minutes": 1}},
            ],
            "as": "logs",
        }},
        # One "join" event per employee who has joined by `to` (counted from max(joined_on, from)) for the
        # headcount, plus one item per log. Records count for the department regardless of joined_on.
        {"$project": {"_id": 0, "items": {"$concatArrays": [
            {"$cond": [{"$lte": ["$joined_on", date_to]},
                       [{"date": {"$max": ["$joined_on", date_from]}, "joins": 1, "present": 0, "late": 0}], []]},
            {"$map": {"input": "$logs", "as": "log", "in": log_item}},
        ]}}},
        {"$unwind": "$items"},
        {"$replaceRoot": {"newRoot": "$items"}},
        # A zero row on `from`, so the series exists even when nobody has joined yet.
        {"$unionWith": {"pipeline": [{"$documents": [{"date": date_from, "joins": 0, "present": 0, "late": 0}]}]}},
        {"$group": {"_id": "$date", "joins": {"$sum": "$joins"}, "present": {"$sum": "$present"},
                    "late": {"$sum": "$late"}}},
        {"$addFields": {"day": _date_of("$_id")}},
        # Gap filling: exactly one row per calendar day in [from, to].
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [
            datetime.fromisoformat(date_from).replace(tzinfo=UTC),
            datetime.fromisoformat(upper).replace(tzinfo=UTC),
        ]}}},
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            "output": {"headcount": {"$sum": {"$ifNull": ["$joins", 0]},
                                     "window": {"documents": ["unbounded", "current"]}}},
        }},
        {"$addFields": {
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            "present": {"$ifNull": ["$present", 0]},
            "late": {"$ifNull": ["$late", 0]},
        }},
        {"$addFields": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            _round_half_up_expr({"$divide": [{"$toDecimal": "$present"}, "$headcount"]}, 4),
            None,
        ]}}},
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            # $avg ignores nulls and returns null for an all-null window; rows before `from` don't exist here.
            "output": {"moving_avg": {"$avg": "$attendance_rate", "window": {"documents": [-6, "current"]}}},
        }},
        {"$sort": {"day": 1}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d", "timezone": "UTC"}},
            "is_working_day": 1,
            "headcount": 1,
            "present_count": {"$toDouble": "$present"},
            "late_count": "$late",
            "attendance_rate": {"$toDouble": "$attendance_rate"},
            "moving_avg_7d": {"$toDouble": _round_half_up_expr("$moving_avg", 4)},
        }},
    ]


def check_trend_range(date_from: str, date_to: str) -> None:
    days = (date.fromisoformat(date_to) - date.fromisoformat(date_from)).days + 1
    if days < 1:
        raise validation_error(("query", "to"), "to must not be before from")
    if days > TREND_MAX_DAYS:
        raise validation_error(("query", "to"), f"the range may not exceed {TREND_MAX_DAYS} days")


# --------------------------------------------------------------------------- #
# 9. Analytics endpoints
# --------------------------------------------------------------------------- #
MonthParam = Annotated[str, Query(pattern=MONTH_PATTERN)]
LimitParam = Annotated[int, Query(ge=1, le=50)]


def working_days(month: str, joined_on: str) -> int:
    """R7: Mon-Fri days of the month, counted from joined_on for a mid-month joiner (calendar maths only)."""
    start, end = (date.fromisoformat(d) for d in month_bounds(month))
    start = max(start, date.fromisoformat(joined_on))
    return sum(1 for i in range((end - start).days + 1) if (start + timedelta(days=i)).weekday() < 5)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthParam):
    emp = get_employee_or_404(emp_code)
    collection, pipeline = employee_monthly_pipeline(emp_code, month)
    rows = list(db[collection].aggregate(pipeline))
    stats = rows[0] if rows else {}
    wd = working_days(month, emp["joined_on"])
    present = to_decimal(stats.get("present_days", 0))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": wd,
        "present_days": round_half_up(present, 2),
        "leave_days": stats.get("leave_count", 0),
        "late_count": stats.get("late_count", 0),
        "total_late_minutes": int(stats.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(stats.get("total_overtime_minutes", 0)),
        "attendance_pct": round_half_up(present / wd * 100, 2) if wd else None,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthParam, department: Optional[str] = None):
    collection, pipeline = department_summary_pipeline(month, department)
    items = []
    for row in db[collection].aggregate(pipeline):
        count = row["work_hours_count"]
        avg = to_decimal(row["work_hours_sum"]) / count if count else None
        items.append({
            "department": row["_id"],
            "headcount": row["headcount"],
            "present_days": round_half_up(row["present_days"], 2),
            "avg_work_hours": round_half_up(avg, 2),
            "late_count": row["late_count"],
            "total_late_minutes": int(row["total_late_minutes"]),
            "leave_count": row["leave_count"],
            "on_duty_count": row["on_duty_count"],
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthParam, limit: LimitParam = 10, department: Optional[str] = None):
    collection, pipeline = late_leaderboard_pipeline(month, limit, department)
    items = [{**row, "total_late_minutes": int(row["total_late_minutes"])}
             for row in db[collection].aggregate(pipeline)]
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    date_from: Annotated[DateStr, Query(alias="from")],
    date_to: Annotated[DateStr, Query(alias="to")],
):
    check_trend_range(date_from, date_to)
    if employees_col.find_one({"department": department}, projection={"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    collection, pipeline = department_trend_pipeline(department, date_from, date_to)
    return {"department": department, "items": list(db[collection].aggregate(pipeline))}


# --------------------------------------------------------------------------- #
# 10. /admin/explain
# --------------------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                        "department_trend"]


def _require(**params) -> None:
    missing = [name for name, value in params.items() if value is None]
    if missing:
        raise RequestValidationError([
            {"type": "missing", "loc": ("query", name), "msg": "Field required", "input": None} for name in missing
        ])


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH_PATTERN)] = None,
    department: Optional[str] = None,
    limit: LimitParam = 10,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[DateStr], Query(alias="from")] = None,
    to: Optional[DateStr] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    if endpoint == "attendance_list":
        command = {
            "find": "attendance_logs",
            "filter": build_attendance_query(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
        collection = "attendance_logs"
    else:
        if endpoint == "employee_monthly":
            _require(emp_code=emp_code, month=month)
            collection, pipeline = employee_monthly_pipeline(emp_code, month)
        elif endpoint == "department_summary":
            _require(month=month)
            collection, pipeline = department_summary_pipeline(month, department)
        elif endpoint == "late_leaderboard":
            _require(month=month)
            collection, pipeline = late_leaderboard_pipeline(month, limit, department)
        else:  # department_trend
            _require(department=department, **{"from": from_, "to": to})
            check_trend_range(from_, to)
            collection, pipeline = department_trend_pipeline(department, from_, to)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    raw = db.command("explain", command, verbosity="executionStats")
    # BSON -> plain JSON (dates, Int64, Timestamp, ObjectId become JSON-safe values)
    as_json = json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection, "explain": as_json}
