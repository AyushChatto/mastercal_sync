# gcal_sync.py
import re
import json
import hashlib
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional

from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_PATH = BASE_DIR / "credentials.json"
TOKEN_PATH = BASE_DIR / "token.json"

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from logutil import log, jdump

SGT = ZoneInfo("Asia/Singapore")
SCOPES = ["https://www.googleapis.com/auth/calendar.events"]

def _parse_ymd(s: str) -> date:
    return date.fromisoformat(s)

def _parse_hm(s: str) -> time:
    hh, mm = s.split(":")
    return time(int(hh), int(mm))

def _norm_summary(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def legacy_uid(chat_id: int, ev_dump: Dict[str, Any]) -> str:
    key = {"summary": _norm_summary(ev_dump.get("summary"))}
    h = hashlib.sha256(json.dumps(key, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"tg-{chat_id}-{h}@mastercal.local"


def collision_summaries(events: list[Dict[str, Any]]) -> set[str]:
    counts: Dict[str, int] = {}
    for ev in events:
        s = _norm_summary(ev.get("summary"))
        counts[s] = counts.get(s, 0) + 1
    return {s for s, c in counts.items() if c > 1}


def event_uid(chat_id: int, ev_dump: Dict[str, Any], collide: bool) -> str:
    # Backward compatible default: summary-only
    if not collide:
        return legacy_uid(chat_id, ev_dump)

    # Only add dates if collision (fixes monthly repeats like AVCTT)
    key = {
        "summary": _norm_summary(ev_dump.get("summary")),
        "start_date": ev_dump.get("start_date"),
        "end_date": ev_dump.get("end_date"),
    }
    h = hashlib.sha256(json.dumps(key, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"tg-{chat_id}-{h}@mastercal.local"


def _find_by_icaluid(svc, calendar_id: str, uid: str):
    resp = svc.events().list(
        calendarId=calendar_id,
        iCalUID=uid,
        maxResults=5,
        showDeleted=True,
        singleEvents=True,
    ).execute()
    items = resp.get("items", []) or []
    for it in items:
        if it.get("status") != "cancelled":
            return it, resp
    return (items[0] if items else None), resp


def _item_start_date(item: Dict[str, Any]) -> Optional[str]:
    start = item.get("start") or {}
    if start.get("date"):
        return start["date"]
    dt = start.get("dateTime")
    if dt:
        try:
            return datetime.fromisoformat(dt).astimezone(SGT).date().isoformat()
        except Exception:
            return dt[:10]
    return None


def _item_end_date_inclusive(item: Dict[str, Any]) -> Optional[str]:
    end = item.get("end") or {}
    if end.get("date"):
        try:
            d = date.fromisoformat(end["date"]) - timedelta(days=1)  # end.date is exclusive
            return d.isoformat()
        except Exception:
            return None
    dt = end.get("dateTime")
    if dt:
        try:
            return datetime.fromisoformat(dt).astimezone(SGT).date().isoformat()
        except Exception:
            return dt[:10]
    return None


def gcal_service():
    log("Google Calendar: initializing service")
    creds = None

    if TOKEN_PATH.exists():
        log(f"Google Calendar: loading {TOKEN_PATH}")
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        log("Google Calendar: token missing/invalid, refreshing or doing OAuth flow")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_PATH.exists():
                raise FileNotFoundError(f"Missing {CREDENTIALS_PATH} (download OAuth Desktop credentials JSON)")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)

        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
        log(f"Google Calendar: wrote {TOKEN_PATH}")

    svc = build("calendar", "v3", credentials=creds)
    log("Google Calendar: service ready")
    return svc

def _event_body(ev: Dict[str, Any], uid: str) -> Dict[str, Any]:
    sd = _parse_ymd(ev["start_date"])
    ed_incl = _parse_ymd(ev["end_date"])

    body: Dict[str, Any] = {
        "summary": ev["summary"],
        "iCalUID": uid,
    }
    if ev.get("location"):
        body["location"] = ev["location"]
    if ev.get("description"):
        body["description"] = ev["description"]

    if ev.get("start_time") is None:
        body["start"] = {"date": sd.isoformat()}
        body["end"] = {"date": (ed_incl + timedelta(days=1)).isoformat()}  # exclusive
    else:
        st = datetime.combine(sd, _parse_hm(ev["start_time"]), tzinfo=SGT)
        if ev.get("end_time") is None:
            et = st + timedelta(hours=1)
        else:
            et = datetime.combine(ed_incl, _parse_hm(ev["end_time"]), tzinfo=SGT)
        body["start"] = {"dateTime": st.isoformat(), "timeZone": "Asia/Singapore"}
        body["end"] = {"dateTime": et.isoformat(), "timeZone": "Asia/Singapore"}

    return body

def _patch_revive(svc, calendar_id: str, event_id: str, body: dict):
    patch_body = dict(body)
    patch_body.pop("iCalUID", None)
    patch_body["status"] = "confirmed"

    log(f"REVIVE/PATCH event_id={event_id}")
    jdump(patch_body, prefix="GCAL PATCH BODY", max_chars=6000)
    out = svc.events().patch(calendarId=calendar_id, eventId=event_id, body=patch_body).execute()
    jdump(out, prefix="GCAL PATCH RESPONSE", max_chars=6000)
    return out


def upsert_events(calendar_id: str, chat_id: int, parsed_calendar_dump: Dict[str, Any]) -> None:
    svc = gcal_service()
    events = parsed_calendar_dump.get("events") or []
    colliders = collision_summaries(events)

    log(f"Google Calendar upsert start calendar_id={calendar_id} events={len(events)} colliders={sorted(list(colliders))}")

    # Backward compat: cache one legacy event per colliding summary (summary-only UID)
    legacy_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    legacy_used_event_ids: Dict[str, set[str]] = {}

    for i, ev in enumerate(events, start=1):
        s_norm = _norm_summary(ev.get("summary"))
        collide = s_norm in colliders

        uid = event_uid(chat_id, ev, collide=collide)
        body = _event_body(ev, uid)

        existing_item, _ = _find_by_icaluid(svc, calendar_id, uid)
        status = (existing_item or {}).get("status")
        log(f"[{i}/{len(events)}] uid={uid} summary={ev.get('summary')!r} collide={collide} found={bool(existing_item)} status={status!r}")

        if existing_item:
            event_id = existing_item["id"]
            if status == "cancelled":
                _patch_revive(svc, calendar_id, event_id, body)
            else:
                log(f"[{i}/{len(events)}] UPDATE event_id={event_id}")
                out = svc.events().update(calendarId=calendar_id, eventId=event_id, body=body).execute()
                jdump(out, prefix="GCAL UPDATE RESPONSE", max_chars=6000)
            continue

        # Backward compatibility for colliders: try legacy summary-only event and "claim" it if same date range
        if collide:
            if s_norm not in legacy_cache:
                l_uid = legacy_uid(chat_id, ev)
                legacy_item, _ = _find_by_icaluid(svc, calendar_id, l_uid)
                legacy_cache[s_norm] = legacy_item
                log(f"[{i}/{len(events)}] legacy lookup uid={l_uid} found={bool(legacy_item)} status={(legacy_item or {}).get('status')!r}")

            legacy_item = legacy_cache.get(s_norm)
            if legacy_item:
                legacy_event_id = legacy_item["id"]
                used = legacy_used_event_ids.setdefault(s_norm, set())

                legacy_sd = _item_start_date(legacy_item)
                legacy_ed = _item_end_date_inclusive(legacy_item)
                want_sd = ev.get("start_date")
                want_ed = ev.get("end_date")

                log(f"[{i}/{len(events)}] legacy candidate event_id={legacy_event_id} used={legacy_event_id in used} legacy={legacy_sd}..{legacy_ed} want={want_sd}..{want_ed}")

                if legacy_event_id not in used and legacy_sd == want_sd and (legacy_ed is None or legacy_ed == want_ed):
                    used.add(legacy_event_id)

                    # Update legacy event while keeping legacy UID (avoid iCalUID conflicts)
                    l_uid = legacy_uid(chat_id, ev)
                    legacy_body = _event_body(ev, l_uid)
                    legacy_status = legacy_item.get("status")

                    if legacy_status == "cancelled":
                        _patch_revive(svc, calendar_id, legacy_event_id, legacy_body)
                    else:
                        log(f"[{i}/{len(events)}] UPDATE legacy event_id={legacy_event_id}")
                        out = svc.events().update(calendarId=calendar_id, eventId=legacy_event_id, body=legacy_body).execute()
                        jdump(out, prefix="GCAL UPDATE RESPONSE (legacy)", max_chars=6000)

                    continue

        # no existing -> insert
        try:
            log(f"[{i}/{len(events)}] INSERT")
            out = svc.events().insert(calendarId=calendar_id, body=body).execute()
            jdump(out, prefix="GCAL INSERT RESPONSE", max_chars=6000)
        except HttpError as e:
            if getattr(e, "resp", None) is not None and getattr(e.resp, "status", None) == 409:
                log(f"[{i}/{len(events)}] INSERT got 409 duplicate; re-list and patch/update")
                existing_item2, _ = _find_by_icaluid(svc, calendar_id, uid)
                if existing_item2:
                    eid2 = existing_item2["id"]
                    if existing_item2.get("status") == "cancelled":
                        _patch_revive(svc, calendar_id, eid2, body)
                    else:
                        out = svc.events().update(calendarId=calendar_id, eventId=eid2, body=body).execute()
                        jdump(out, prefix="GCAL UPDATE RESPONSE (after 409)", max_chars=6000)
                else:
                    raise
            else:
                raise

    log("Google Calendar upsert done")
