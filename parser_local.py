# parser_local.py
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple

from pydantic import BaseModel, Field

from logutil import log, jdump

SGT = ZoneInfo("Asia/Singapore")

# ---------- output schema ----------
class ParsedEvent(BaseModel):
    summary: str
    start_date: str = Field(..., description="YYYY-MM-DD inclusive")
    start_time: Optional[str] = Field(None, description="HH:MM 24h or null for all-day")
    end_date: str = Field(..., description="YYYY-MM-DD inclusive")
    end_time: Optional[str] = Field(None, description="HH:MM 24h or null for all-day")
    location: Optional[str] = None
    description: Optional[str] = None

class ParsedCalendar(BaseModel):
    events: List[ParsedEvent]

# ---------- parsing helpers ----------
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# time: 8am, 7pm, 6.30pm, 8.00pm, 6:30pm
_TIME_RE = re.compile(
    r"(?P<h>\d{1,2})(?:[:.](?P<m>\d{2}))?\s*(?P<ampm>am|pm)\b",
    re.IGNORECASE,
)

# time range: 2pm-6pm, 2pm - 6pm, 2-6pm (end am/pm optional)
_TIME_RANGE_RE = re.compile(
    r"(?P<t1>\d{1,2}(?:[:.]\d{2})?\s*(?:am|pm))\s*-\s*(?P<t2>\d{1,2}(?:[:.]\d{2})?\s*(?:am|pm)?)\b",
    re.IGNORECASE,
)

# date: 12Dec (Fri) ...
_DATE_RE = re.compile(
    r"^\s*(?P<d>\d{1,2})\s*(?P<m>[A-Za-z]{3})\s*(?:\([^)]*\))?\s*(?P<rest>.*)\s*$"
)

# date range: 28Feb-3Mar ...
_DATE_RANGE_RE = re.compile(
    r"^\s*(?P<d1>\d{1,2})\s*(?P<m1>[A-Za-z]{3})\s*-\s*(?P<d2>\d{1,2})\s*(?P<m2>[A-Za-z]{3})\s*(?P<rest>.*)\s*$"
)

def _mon(mon3: str) -> int:
    mm = _MONTHS.get(mon3.strip().lower())
    if not mm:
        raise ValueError(f"Unknown month {mon3!r}")
    return mm

def _to_date(y: int, d: int, mon3: str) -> date:
    return date(y, _mon(mon3), d)

def _norm_hhmm(t: str) -> str:
    m = _TIME_RE.search(t)
    if not m:
        raise ValueError(f"Bad time {t!r}")
    h = int(m.group("h"))
    mi = int(m.group("m") or "0")
    ampm = m.group("ampm").lower()

    if h == 12:
        h = 0
    if ampm == "pm":
        h += 12

    if not (0 <= h <= 23 and 0 <= mi <= 59):
        raise ValueError(f"Time out of range {t!r}")

    return f"{h:02d}:{mi:02d}"

def _strip_time_tokens(s: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Returns: (summary_without_time, start_time, end_time)
    end_time may be None (caller can add +1h later).
    """
    # 1) time range
    m = _TIME_RANGE_RE.search(s)
    if m:
        t1 = m.group("t1")
        t2 = m.group("t2").strip()

        # infer missing am/pm in t2 from t1
        if re.search(r"(am|pm)", t2, re.IGNORECASE) is None:
            m1 = _TIME_RE.search(t1)
            if not m1:
                raise ValueError(f"Bad time in range (t1) {t1!r}")
            ampm = m1.group("ampm")  # "am" or "pm"
            t2 = f"{t2}{ampm}"

        st = _norm_hhmm(t1)
        et = _norm_hhmm(t2)
        summary = (s[:m.start()] + " " + s[m.end():]).strip()
        summary = re.sub(r"\s{2,}", " ", summary)
        return summary, st, et

    # 2) single time
    m = _TIME_RE.search(s)
    if m:
        st = _norm_hhmm(m.group(0))
        summary = (s[:m.start()] + " " + s[m.end():]).strip()
        summary = re.sub(r"\s{2,}", " ", summary)
        return summary, st, None

    return s.strip(), None, None

def _split_location(s: str) -> Tuple[str, Optional[str]]:
    # take first '@' as location delimiter
    if "@" not in s:
        return s.strip(), None
    left, right = s.split("@", 1)
    loc = right.strip()
    base = left.strip()
    return base, (loc if loc else None)

def parse_mastercal_local(mastercal_text: str) -> ParsedCalendar:
    strict = os.environ.get("MASTER_CAL_STRICT", "0") == "1"
    today = datetime.now(SGT).date()
    year_ctx = today.year

    lines = mastercal_text.splitlines()
    log(f"Local parse start: lines={len(lines)} default_year={year_ctx} strict={strict}")
    events: List[ParsedEvent] = []
    errors: List[str] = []

    for i, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue

        # ignore header-ish lines
        if line.lower().startswith("#mastercal"):
            log(f"skip line {i}: header")
            continue

        # year context line
        if re.fullmatch(r"\d{4}", line):
            year_ctx = int(line)
            log(f"year context set: {year_ctx} (line {i})")
            continue

        log(f"parse line {i}: {line!r}")

        # date range
        m = _DATE_RANGE_RE.match(line)
        if m:
            try:
                sd = _to_date(year_ctx, int(m.group("d1")), m.group("m1"))
                ed = _to_date(year_ctx, int(m.group("d2")), m.group("m2"))
            except Exception as e:
                msg = f"line {i} date-range parse failed: {e!r} :: {line!r}"
                log("WARN " + msg)
                errors.append(msg)
                if strict:
                    break
                continue

            rest = (m.group("rest") or "").strip()
            base, loc = _split_location(rest)
            summary, st, et = _strip_time_tokens(base)

            if not summary:
                msg = f"line {i} empty summary after parsing :: {line!r}"
                log("WARN " + msg)
                errors.append(msg)
                if strict:
                    break
                continue

            ev = ParsedEvent(
                summary=summary,
                start_date=sd.isoformat(),
                start_time=st,
                end_date=ed.isoformat(),
                end_time=et,
                location=loc,
                description=f"source_line={line}",
            )
            log(f"OK line {i}: range {sd}..{ed} summary={summary!r} st={st} et={et} loc={loc!r}")
            events.append(ev)
            continue

        # single date
        m = _DATE_RE.match(line)
        if m:
            try:
                sd = _to_date(year_ctx, int(m.group("d")), m.group("m"))
            except Exception as e:
                msg = f"line {i} date parse failed: {e!r} :: {line!r}"
                log("WARN " + msg)
                errors.append(msg)
                if strict:
                    break
                continue

            rest = (m.group("rest") or "").strip()
            base, loc = _split_location(rest)
            summary, st, et = _strip_time_tokens(base)

            if not summary:
                msg = f"line {i} empty summary after parsing :: {line!r}"
                log("WARN " + msg)
                errors.append(msg)
                if strict:
                    break
                continue

            ev = ParsedEvent(
                summary=summary,
                start_date=sd.isoformat(),
                start_time=st,
                end_date=sd.isoformat(),
                end_time=et,
                location=loc,
                description=f"source_line={line}",
            )
            log(f"OK line {i}: date {sd} summary={summary!r} st={st} et={et} loc={loc!r}")
            events.append(ev)
            continue

        msg = f"line {i} unrecognized format :: {line!r}"
        log("WARN " + msg)
        errors.append(msg)
        if strict:
            break

    out = ParsedCalendar(events=events)
    log(f"Local parse done: events={len(events)} errors={len(errors)}")
    if errors:
        jdump(errors, prefix="PARSER WARNINGS")
        if strict:
            raise RuntimeError("MASTER_CAL_STRICT=1 and parser encountered errors; see logs")

    jdump(out.model_dump(), prefix="PARSED_CALENDAR (LOCAL)")
    return out
