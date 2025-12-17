import re
from typing import Any, Tuple, List, Dict
from logutil import log, jdump

def td_wait(r, *, what: str = "") -> Dict[str, Any]:
    log(f"TDLib WAIT start {what}".strip())
    r.wait(raise_exc=True)
    upd = r.update
    log(f"TDLib WAIT done {what}".strip())
    jdump(upd, prefix=f"TDLib UPDATE {what}".strip())
    return upd or {}

def preload_chats(tg) -> None:
    log("Preloading chats via tg.get_chats() ...")
    r = tg.get_chats()
    r.wait(raise_exc=True)
    log("Preload chats done.")
    jdump(r.update, prefix="tg.get_chats() update")

def msg_text(m: Dict[str, Any]) -> str:
    c = m.get("content") or {}
    if c.get("@type") == "messageText":
        return ((c.get("text") or {}).get("text") or "")
    return c.get("@type") or ""

def get_all_pinned_messages(tg, chat_id: int, page_size: int = 100) -> List[Dict[str, Any]]:
    pinned: List[Dict[str, Any]] = []
    from_message_id = 0
    page = 0

    log(f"Fetching pinned messages: chat_id={chat_id} page_size={page_size}")

    while True:
        page += 1
        payload = {
            "chat_id": chat_id,
            "topic_id": None,
            "query": "",
            "sender_id": None,
            "from_message_id": from_message_id,
            "offset": 0,
            "limit": page_size,
            "filter": {"@type": "searchMessagesFilterPinned"},
        }
        log(f"TDLib call_method searchChatMessages page={page} from_message_id={from_message_id}")
        jdump(payload, prefix="TDLib REQUEST searchChatMessages")

        r = tg.call_method("searchChatMessages", payload)
        res = td_wait(r, what=f"searchChatMessages page={page}")

        msgs = res.get("messages") or []
        log(f"Pinned page={page}: got {len(msgs)} messages")
        pinned.extend(msgs)

        next_from = int(res.get("next_from_message_id") or 0)
        log(f"Pinned page={page}: next_from_message_id={next_from}")

        if not msgs or next_from == 0 or next_from == from_message_id:
            break
        from_message_id = next_from

    log(f"Total pinned messages fetched: {len(pinned)}")
    return pinned

def find_latest_pinned_message_text(tg, chat_id: int, pattern: str) -> Tuple[int, str]:
    log(f"Finding latest pinned message matching regex: {pattern!r}")
    rx = re.compile(pattern)

    msgs = get_all_pinned_messages(tg, chat_id)
    for idx, m in enumerate(msgs, start=1):
        text_raw = msg_text(m)
        mid = int(m.get("id") or 0)
        log(f"Check pinned[{idx}/{len(msgs)}] id={mid} text_len={len(text_raw)}")
        if rx.search(text_raw):
            log(f"Matched pinned message id={mid}")
            # print the raw text fully (this is what you asked for)
            log("RAW MASTER CAL MESSAGE BEGIN")
            print(text_raw, flush=True)
            log("RAW MASTER CAL MESSAGE END")
            return mid, text_raw

    raise RuntimeError(f"No pinned message matched pattern {pattern!r}")
