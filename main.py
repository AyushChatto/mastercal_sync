# main.py
import os
import yaml

from logutil import log, die
from telegram.client import Telegram

from telegram_td import preload_chats, find_latest_pinned_message_text
from parser_local import parse_mastercal_local
from gcal_sync import upsert_events

PATTERN = r"#MasterCal"

def load_telegram_config(path: str = "secrets.yaml") -> dict:
    log(f"Loading secrets from {path}")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    tg_cfg = cfg.get("telegram") or {}

    required = [
        "api_id",
        "api_hash",
        "phone",
        "database_encryption_key",
        "files_directory",
        "library_path",
    ]
    missing = [k for k in required if not tg_cfg.get(k)]
    if missing:
        die(f"Missing keys in secrets.yaml under telegram: {missing}")

    tg_cfg["api_id"] = int(tg_cfg["api_id"])
    log("Loaded telegram config (redacted).")
    log(f"api_id={tg_cfg['api_id']}")
    log(f"phone={tg_cfg['phone']}")
    log(f"files_directory={tg_cfg['files_directory']}")
    log(f"library_path={tg_cfg['library_path']}")
    return tg_cfg

def sync_mastercal(tg, chat_id: int, calendar_id: str) -> None:
    log("=== SYNC START ===")
    log(f"chat_id={chat_id} pattern={PATTERN!r} calendar_id={calendar_id!r}")

    preload_chats(tg)

    master_msg_id, text = find_latest_pinned_message_text(tg, chat_id, PATTERN)
    log(f"Using pinned master_msg_id={master_msg_id} text_len={len(text)}")
    parsed = parse_mastercal_local(text)
    upsert_events(calendar_id, chat_id, parsed.model_dump())
    log("=== SYNC END ===")

def main():
    # pip install pyyaml
    tg_cfg = load_telegram_config("secrets.yaml")

    chat_id = tg_cfg["chat_id"]
    calendar_id = os.environ.get("GCAL_ID", "primary")

    log("Creating Telegram client")
    tg = Telegram(
        api_id=tg_cfg["api_id"],
        api_hash=tg_cfg["api_hash"],
        phone=tg_cfg["phone"],
        database_encryption_key=tg_cfg["database_encryption_key"],
        files_directory=tg_cfg["files_directory"],
        library_path=tg_cfg["library_path"],
    )

    try:
        log("tg.login() start")
        tg.login()
        log("tg.login() done")

        sync_mastercal(tg, chat_id, calendar_id)
    finally:
        log("tg.stop()")
        tg.stop()

if __name__ == "__main__":
    main()
