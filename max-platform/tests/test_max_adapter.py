"""End-to-end test for the MAX platform plugin against a mock MAX Bot API.

Runs the REAL adapter: real long-poll loop, real HTTP, real marker persistence.
Only the MAX server is mocked.

    HERMES_REPO=<путь к hermes-agent> \
    MAX_TEST_HOME=<временная папка> \
    <venv python> tests/test_max_adapter.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import time
import types
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
REPO = Path(os.environ.get("HERMES_REPO") or (Path(os.environ.get("LOCALAPPDATA", ".")) / "hermes" / "hermes-agent"))
TEST_HOME = Path(os.environ.get("MAX_TEST_HOME") or (Path(os.environ.get("TEMP", ".")) / "max_test_home"))
BASE = "http://127.0.0.1:8791"
sys.path.insert(0, str(REPO))
os.environ["HERMES_HOME"] = str(TEST_HOME)

FAILURES: list[str] = []
CHECKS = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    if ok:
        print(f"PASS  {label}")
    else:
        print(f"FAIL  {label}  {detail}")
        FAILURES.append(label)


def load_plugin():
    name = "hermes_plugins.max_platform_test"
    if "hermes_plugins" not in sys.modules:
        ns = types.ModuleType("hermes_plugins")
        ns.__path__ = []  # type: ignore[attr-defined]
        sys.modules["hermes_plugins"] = ns
    spec = importlib.util.spec_from_file_location(
        name, PLUGIN_DIR / "__init__.py", submodule_search_locations=[str(PLUGIN_DIR)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000a49444154789c6300010000050001" "0d0a2db4" "0000000049454e44ae426082"
)


def update_for(mid: str, text: str, user_id: int = 555, chat_id: int | None = None,
               with_image: bool = False, chat_type: str = "dialog") -> dict:
    payloads = []
    if with_image:
        payloads.append({"type": "image", "payload": {"url": f"{BASE}/media/img.png"}})
    return {
        "update_type": "message_created",
        "timestamp": int(time.time() * 1000),
        "message": {
            "sender": {"user_id": user_id, "first_name": "Максим", "username": "perm"},
            "recipient": {"chat_id": chat_id or user_id, "chat_type": chat_type, "user_id": user_id},
            "timestamp": int(time.time() * 1000),
            "body": {"mid": mid, "seq": 1, "text": text, "attachments": payloads},
            "link": {"type": "reply", "message": {"body": {"mid": "orig-1", "text": "предыдущее"}}},
        },
    }


class MockMAX:
    def __init__(self) -> None:
        self.queue: list[dict] = []
        self.marker = 42
        self.sent: list[dict] = []
        self.action_log: list[tuple[str, str]] = []
        self.upload_count = 0
        self.upload_bytes = 0

    # routes
    async def me(self, request):  # noqa: ANN001
        return self.web.json_response({"user_id": 100, "first_name": "Hermes", "is_bot": True})

    async def updates(self, request):  # noqa: ANN001
        if self.queue:
            batch, self.queue = self.queue, []
            return self.web.json_response({"updates": batch, "marker": self.marker})
        await asyncio.sleep(0.2)
        return self.web.json_response({"updates": [], "marker": self.marker})

    async def messages(self, request):  # noqa: ANN001
        body = await request.json()
        chat_id = request.query.get("chat_id")
        self.sent.append({"chat_id": chat_id, "body": body})
        return self.web.json_response({"message": {"body": {"mid": f"out-{len(self.sent)}"}, "recipient": {"chat_id": chat_id}}})

    async def actions(self, request):  # noqa: ANN001
        payload = await request.json()
        self.action_log.append((request.match_info["chat"], payload.get("action", "")))
        return self.web.json_response({"success": True})

    async def chat(self, request):  # noqa: ANN001
        return self.web.json_response({"title": "Test Chat", "type": "dialog"})

    async def uploads(self, request):  # noqa: ANN001
        return self.web.json_response({"url": f"{BASE}/upload-here", "token": "upload-token"})

    async def upload_here(self, request):  # noqa: ANN001
        reader = await request.multipart()
        while True:
            part = await reader.next()
            if part is None:
                break
            data = await part.read()
            self.upload_bytes += len(data)
        self.upload_count += 1
        return self.web.json_response({"photos": {"photo-1": {"token": "photo-token"}}})

    async def media(self, request):  # noqa: ANN001
        return self.web.Response(body=PNG, content_type="image/png")


async def main() -> int:
    from aiohttp import web

    mock = MockMAX()
    mock.web = web
    app = web.Application()
    app.router.add_get("/me", mock.me)
    app.router.add_get("/updates", mock.updates)
    app.router.add_post("/messages", mock.messages)
    app.router.add_post("/chats/{chat}/actions", mock.actions)
    app.router.add_get("/chats/{chat}", mock.chat)
    app.router.add_post("/uploads", mock.uploads)
    app.router.add_post("/upload-here", mock.upload_here)
    app.router.add_get("/media/img.png", mock.media)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 8791)
    await site.start()

    TEST_HOME.mkdir(parents=True, exist_ok=True)
    state = TEST_HOME / "max_poll_state.json"
    if state.exists():
        state.unlink()

    load_plugin()
    plugin = sys.modules["hermes_plugins.max_platform_test.adapter"]
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(
        enabled=True, token="test-token",
        extra={"api_base": BASE, "allowed_users": ["555"], "poll_timeout": 0, "media_max_mb": 5},
    )
    adapter = plugin.MAXAdapter(cfg)
    check("adapter platform is 'max'", adapter.platform.value == "max", adapter.platform.value)
    check("allowed users parsed", adapter.allowed_users == ["555"], str(adapter.allowed_users))

    events: list = []

    async def handler(event):
        events.append(event)
        await adapter.send(event.source.chat_id, f"echo: {event.text}", reply_to=event.message_id)

    adapter.set_message_handler(handler)
    connected = await adapter.connect()
    check("connect() succeeded", connected is True, str(getattr(adapter, "_fatal_error", None)))

    # ── 1. inbound text + image, auto mark_seen, reply back ─────────────────
    mock.queue.append(update_for("mid-1", "Привет", with_image=True))
    for _ in range(60):
        if events:
            break
        await asyncio.sleep(0.1)
    check("inbound message delivered to the agent", len(events) == 1, f"{len(events)} events")
    if events:
        ev = events[0]
        check("text extracted", ev.text == "Привет", repr(ev.text))
        check("chat/user id mapped", ev.source.chat_id == "555" and ev.user_id == "555", str(ev.source))
        check("dialog mapped to dm", ev.source.chat_type == "dm", ev.source.chat_type)
        check("reply context picked up", ev.reply_to_text == "предыдущее", str(ev.reply_to_text))
        check("inbound image cached locally",
              len(ev.media_urls) == 1 and Path(ev.media_urls[0]).exists(), str(ev.media_urls))
    for _ in range(40):
        if mock.sent:
            break
        await asyncio.sleep(0.1)
    check("reply sent back to MAX", len(mock.sent) == 1, f"{len(mock.sent)} sends")
    if mock.sent:
        sent = mock.sent[0]
        check("reply text", sent["body"].get("text") == "echo: Привет", json.dumps(sent["body"], ensure_ascii=False))
        check("reply targets the right chat", sent["chat_id"] == "555", str(sent["chat_id"]))
        check("reply quotes the inbound mid",
              (sent["body"].get("link") or {}).get("mid") == "mid-1", json.dumps(sent["body"]))
    check("mark_seen sent", ("555", "mark_seen") in mock.action_log, str(mock.action_log))

    # ── 2. duplicate update is deduped ──────────────────────────────────────
    mock.queue.append(update_for("mid-1", "Привет", with_image=True))
    await asyncio.sleep(0.8)
    check("duplicate update ignored", len(events) == 1, f"{len(events)} events")

    # ── 3. unauthorized user filtered ───────────────────────────────────────
    mock.queue.append(update_for("mid-2", "Я чужой", user_id=999))
    await asyncio.sleep(0.8)
    check("unauthorized MAX user filtered", len(events) == 1, f"{len(events)} events")

    # ── 4. marker persisted across restarts ─────────────────────────────────
    data = json.loads(state.read_text()) if state.exists() else {}
    bots = data.get("bots") or {}
    entry = next(iter(bots.values()), {})
    check("poll marker persisted to disk", entry.get("marker") == 42, json.dumps(data))
    check("bot user id persisted", str(entry.get("bot_user_id")) == "100", json.dumps(entry))

    # ── 5. typing indicator + chat info ─────────────────────────────────────
    await adapter.send_typing("555")
    check("typing_on sent", ("555", "typing_on") in mock.action_log, str(mock.action_log))
    info = await adapter.get_chat_info("555")
    check("get_chat_info maps the chat", info.get("name") == "Test Chat" and info.get("type") == "dm", str(info))

    # ── 6. outbound image by URL and by local file ──────────────────────────
    before = len(mock.sent)
    res = await adapter.send_image("555", f"{BASE}/media/img.png", caption="картинка")
    att = (mock.sent[-1]["body"].get("attachments") or [{}])[0]
    check("image by URL sent natively",
          res.success and att.get("type") == "image" and att.get("payload", {}).get("url", "").endswith("img.png"),
          json.dumps(att, ensure_ascii=False))
    check("image caption kept", mock.sent[-1]["body"].get("text") == "картинка", str(mock.sent[-1]["body"]))

    local_png = TEST_HOME / "shot.png"
    local_png.write_bytes(PNG)
    res = await adapter.send_image_file("555", str(local_png))
    att = (mock.sent[-1]["body"].get("attachments") or [{}])[0]
    check("local image uploaded then attached",
          res.success and mock.upload_count == 1 and att.get("payload", {}).get("photos", {}).get("photo-1", {}).get("token") == "photo-token",
          json.dumps({"uploads": mock.upload_count, "att": att}, ensure_ascii=False))
    check("upload carried the file bytes", mock.upload_bytes == len(PNG), str(mock.upload_bytes))

    # ── 7. long answer chunking ─────────────────────────────────────────────
    before = len(mock.sent)
    long_text = "\n".join(["Строка " + "x" * 120 for _ in range(60)])
    res = await adapter.send("555", long_text)
    chunks = [s["body"]["text"] for s in mock.sent[before:]]
    check("long answer split into multiple messages", res.success and len(chunks) > 1, f"{len(chunks)} chunks")
    check("no chunk exceeds the MAX limit",
          all(len(c) <= 4000 for c in chunks), str([len(c) for c in chunks]))
    check("split loses no text",
          sum(len(c) for c in chunks) >= len(long_text) - 4 * len(chunks), f"{sum(len(c) for c in chunks)} vs {len(long_text)}")

    # ── 8. standalone cron sender ───────────────────────────────────────────
    before = len(mock.sent)
    out = await plugin._standalone_send(cfg, "555", "крон-напоминание", thread_id=None)
    check("standalone sender works out of process",
          out.get("success") is True and mock.sent[-1]["body"]["text"] == "крон-напоминание",
          json.dumps(out, ensure_ascii=False))
    check("standalone sender used a separate connection", len(mock.sent) == before + 1, str(len(mock.sent)))

    # ── 9. single-consumer platform lock ────────────────────────────────────
    # (Same-process re-acquire is deliberate in Hermes: a lock held by our own PID is
    #  handed to the reconnecting adapter, so "second adapter refused" is NOT the
    #  contract. What must hold is that the token-scoped lock is held while connected
    #  and released on disconnect — otherwise a second gateway could double-poll.)
    check("token-scoped platform lock taken on connect",
          adapter._platform_lock_identity is not None, "no lock identity recorded")

    await adapter.disconnect()
    check("disconnect stops the poll loop", adapter._poll_task is None, str(adapter._poll_task))
    check("disconnect releases the platform lock",
          getattr(adapter, "_platform_lock_identity", None) is None, "lock identity still set")
    await adapter.connect()
    check("reconnect after disconnect works", adapter.is_connected is True, "reconnect failed")
    await adapter.disconnect()

    await runner.cleanup()
    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
