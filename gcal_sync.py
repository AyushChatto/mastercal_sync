import re
import json
import hashlib
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict

# gcal_sync.py (add near the top)
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_PATH = BASE_DIR / "credentials.json"
TOKEN_PATH = BASE_DIR / "token.json"


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

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

def event_uid(chat_id: int, ev_dump: Dict[str, Any]) -> str:
    key = {"summary": _norm_summary(ev_dump.get("summary"))}
    h = hashlib.sha256(json.dumps(key, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"tg-{chat_id}-{h}@mastercal.local"

# gcal_sync.py
from googleapiclient.errors import HttpError  # add

def _find_by_icaluid(svc, calendar_id: str, uid: str):
    resp = svc.events().list(
        calendarId=calendar_id,
        iCalUID=uid,
        maxResults=5,
        showDeleted=True,     # key change: include cancelled/deleted :contentReference[oaicite:2]{index=2}
        singleEvents=True,
    ).execute()
    items = resp.get("items", []) or []
    # prefer non-cancelled if present
    for it in items:
        if it.get("status") != "cancelled":
            return it, resp
    return (items[0] if items else None), resp


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
    # Don’t try to change iCalUID during revive; patch core fields + status.
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
    log(f"Google Calendar upsert start calendar_id={calendar_id} events={len(events)}")

    for i, ev in enumerate(events, start=1):
        uid = event_uid(chat_id, ev)
        body = _event_body(ev, uid)

        existing_item, list_resp = _find_by_icaluid(svc, calendar_id, uid)
        log(f"[{i}/{len(events)}] iCalUID lookup: found={bool(existing_item)} status={(existing_item or {}).get('status')!r}")

        existing_item, list_resp = _find_by_icaluid(svc, calendar_id, uid)
        status = (existing_item or {}).get("status")
        log(f"[{i}/{len(events)}] iCalUID lookup: found={bool(existing_item)} status={status!r}")

        if existing_item:
            event_id = existing_item["id"]

            if status == "cancelled":
                # Uncancel + set time/title/etc by patching the same eventId
                try:
                    _patch_revive(svc, calendar_id, event_id, body)
                except HttpError as e:
                    # If Google rejects modifying cancelled events in your scenario, you’ll see 403 here.
                    jdump(getattr(e, "content", b"").decode("utf-8", "ignore"), prefix="GCAL PATCH ERROR")
                    raise
            else:
                log(f"[{i}/{len(events)}] UPDATE event_id={event_id}")
                out = svc.events().update(calendarId=calendar_id, eventId=event_id, body=body).execute()
                jdump(out, prefix="GCAL UPDATE RESPONSE", max_chars=6000)

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