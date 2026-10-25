"""MAX (max.ru) platform adapter for Hermes Agent.

Talks to the MAX Bot API at ``https://platform-api2.max.ru`` (the old
``platform-api.max.ru`` host is deprecated) over Long Polling:

* ``GET  /updates``                     — long poll (``marker``/``timeout``/``types``)
* ``POST /messages``                    — send text / attachments
* ``POST /uploads``                     — get an upload URL + token for media
* ``POST /chats/{chatId}/actions``      — ``typing_on`` / ``mark_seen``
* ``GET  /me``                          — bot identity (self-message filtering)
* ``GET  /chats/{chatId}``              — chat metadata

Auth is the ``Authorization: <token>`` header only (query-string tokens were removed).
The poll marker is persisted per bot token, so a gateway restart does not replay
already-answered messages.

config.yaml ``gateway.platforms.max.extra`` keys: token, api_base, allowed_users,
allow_all_users, home_channel, poll_timeout, text_format, ca_bundle, insecure_tls,
mark_seen, media_max_mb, max_message_length.
Env vars override config.yaml: MAX_BOT_TOKEN, MAX_API_BASE, MAX_ALLOWED_USERS,
MAX_ALLOW_ALL_USERS, MAX_HOME_CHANNEL, MAX_POLL_TIMEOUT, MAX_TEXT_FORMAT,
MAX_CA_BUNDLE, MAX_INSECURE_TLS.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import logging
import os
import ssl
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.platforms._shared import (
    apply_yaml_bridge,
    env_is_connected,
    extra_or_secret,
    get_scoped_secret as _get_scoped_secret,
    seed_extra_from_env,
    send_error,
)
from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    cache_media_bytes_async,
)
from gateway.platforms.helpers import cancel_task
from gateway.platforms.event import MessageEvent, MessageType
from gateway.config import Platform


logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://platform-api2.max.ru"
# MAX rejects text over 4000 characters; keep slack for the chunking math.
MAX_TEXT_LIMIT = 4000
_SAFE_TEXT_LIMIT = 3900
_TRUTHY = {"1", "true", "yes", "on"}
# Only these update types are requested — anything else would be dropped anyway.
_UPDATE_TYPES = "message_created,message_callback,bot_started"
_STATE_FILENAME = "max_poll_state.json"
_SEEN_MIDS_MAX = 2000
_SEND_MIN_INTERVAL = 0.55  # MAX: max 2 messages/second per chat


# ── small helpers ────────────────────────────────────────────────────────────

def _truthy(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in _TRUTHY


def _as_id(value: Any) -> Optional[str]:
    """Normalise a MAX id (int64 or numeric string) to a plain string, else None."""
    if value is None or value == "":
        return None
    return str(value)


def _split_text(content: str, limit: int = _SAFE_TEXT_LIMIT) -> List[str]:
    """Split ``content`` into <=``limit``-character chunks, preferring blank lines,
    then newlines, then spaces."""
    chunks: List[str] = []
    for paragraph in content.split("\n"):
        if len(paragraph) <= limit:
            if paragraph.strip() or chunks:
                chunks.append(paragraph)
            continue
        rest = paragraph
        while len(rest) > limit:
            window = rest[:limit]
            cut = window.rfind(" ")
            if cut < limit // 2:
                cut = limit
            chunks.append(rest[:cut].rstrip())
            rest = rest[cut:].lstrip()
        if rest:
            chunks.append(rest)
    out: List[str] = []
    buf = ""
    for chunk in chunks:
        if not buf:
            buf = chunk
        elif len(buf) + 1 + len(chunk) <= limit:
            buf = f"{buf}\n{chunk}"
        else:
            out.append(buf)
            buf = chunk
    if buf:
        out.append(buf)
    return out or [content[:limit]]


def _token_fingerprint(token: str) -> str:
    import hashlib
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _platform(name: str) -> Platform:
    """``Platform(name)`` for a plugin platform.

    The enum only mints a dynamic member for names the platform registry already
    knows (``Platform._missing_``), so a direct construction before/without the
    registry entry falls back to minting the pseudo member ourselves.
    """
    try:
        return Platform(name)
    except ValueError:
        return Platform._add_pseudo_member(name)


def _state_path() -> Path:
    from hermes_constants import get_hermes_home
    return Path(get_hermes_home()) / _STATE_FILENAME


def _load_state() -> dict:
    try:
        with open(_state_path(), "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        logger.debug("MAX: unreadable poll state, starting fresh", exc_info=True)
        return {}


def _save_state(state: dict) -> None:
    path = _state_path()
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh)
        os.replace(tmp, path)
    except Exception:
        logger.debug("MAX: could not persist poll state to %s", path, exc_info=True)
        with contextlib.suppress(Exception):
            tmp.unlink()


def _sender_name(sender: dict) -> str:
    if not isinstance(sender, dict):
        return ""
    name = " ".join(p for p in (sender.get("first_name"), sender.get("last_name")) if p).strip()
    return name or (sender.get("username") or sender.get("name") or "")


def _media_payload_token(response: dict) -> Optional[dict]:
    """Extract the ``attachments[].payload`` dict from a /uploads file response.

    MAX returns ``{"photos": {"<id>": {"token": ...}}}`` for images and a plain
    ``{"token": ...}`` (or the same nested shape) for video/audio/file.
    """
    if not isinstance(response, dict):
        return None
    if isinstance(response.get("photos"), dict) and response["photos"]:
        return {"photos": response["photos"]}
    if isinstance(response.get("token"), str) and response["token"]:
        return {"token": response["token"]}
    for value in response.values():
        if isinstance(value, dict):
            if isinstance(value.get("token"), str) and value["token"]:
                return {"token": value["token"]}
            for inner in value.values():
                if isinstance(inner, dict) and isinstance(inner.get("token"), str):
                    return {"token": inner["token"]}
    return None


def _guess_media_kind(path: str) -> str:
    ext = Path(path).suffix.lower()
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".tiff", ".bmp", ".heic", ".webp"}:
        return "image"
    if ext in {".mp4", ".mov", ".mkv", ".webm"}:
        return "video"
    if ext in {".mp3", ".m4a", ".ogg", ".wav", ".opus", ".aac", ".flac"}:
        return "audio"
    return "file"


def _mime_for(path: str) -> str:
    import mimetypes
    return mimetypes.guess_type(path)[0] or "application/octet-stream"


# ── adapter ──────────────────────────────────────────────────────────────────

class MAXAdapter(BasePlatformAdapter):
    """Long-polling MAX Bot API adapter implementing the BasePlatformAdapter interface."""

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=_platform("max"))
        extra = getattr(config, "extra", {}) or {}
        self.token = str(
            getattr(config, "token", None)
            or extra_or_secret(extra, "token", "MAX_BOT_TOKEN", "")
            or _get_scoped_secret("MAX_BOT_TOKEN", "")
            or ""
        ).strip()
        self.api_base = str(
            extra_or_secret(extra, "api_base", "MAX_API_BASE", DEFAULT_API_BASE) or DEFAULT_API_BASE
        ).rstrip("/")
        self.allowed_users = self._parse_allowed(extra)
        self.allow_all = _truthy(extra_or_secret(extra, "allow_all_users", "MAX_ALLOW_ALL_USERS", False))
        with contextlib.suppress(TypeError, ValueError):
            self.poll_timeout = max(0, min(90, int(extra_or_secret(extra, "poll_timeout", "MAX_POLL_TIMEOUT", 30))))
        if not hasattr(self, "poll_timeout"):
            self.poll_timeout = 30
        self.text_format = str(extra_or_secret(extra, "text_format", "MAX_TEXT_FORMAT", "") or "").strip().lower()
        self.mark_seen = _truthy(extra_or_secret(extra, "mark_seen", "MAX_MARK_SEEN", True), True)
        self.ca_bundle = str(extra_or_secret(extra, "ca_bundle", "MAX_CA_BUNDLE", "") or "")
        self.insecure_tls = _truthy(extra_or_secret(extra, "insecure_tls", "MAX_INSECURE_TLS", False))
        # MAX is a domestic service: going direct is the sane default, so a stray
        # HTTP_PROXY/HTTPS_PROXY in the gateway env must not silently hijack it.
        self.use_env_proxy = _truthy(extra_or_secret(extra, "use_env_proxy", "MAX_USE_ENV_PROXY", False))
        with contextlib.suppress(TypeError, ValueError):
            self.media_max_mb = int(extra_or_secret(extra, "media_max_mb", "MAX_MEDIA_MAX_MB", 20))
        if not hasattr(self, "media_max_mb"):
            self.media_max_mb = 20
        max_msg = extra.get("max_message_length")
        if max_msg is None:
            with contextlib.suppress(Exception):
                from gateway.platform_registry import platform_registry
                max_msg = platform_registry.get("max").max_message_length
        with contextlib.suppress(TypeError, ValueError):
            self.max_message_length = int(max_msg or _SAFE_TEXT_LIMIT)
        if not hasattr(self, "max_message_length"):
            self.max_message_length = _SAFE_TEXT_LIMIT

        self._session: Optional[Any] = None          # aiohttp.ClientSession
        self._poll_task: Optional[asyncio.Task] = None
        self._closing = False
        self._marker: Optional[int] = None
        self._bot_user_id: Optional[str] = None
        self._seen_mids: "OrderedDict[str, None]" = OrderedDict()
        self._send_locks: Dict[str, asyncio.Lock] = {}
        self._last_send_at: Dict[str, float] = {}
        self._state_key = _token_fingerprint(self.token) if self.token else ""

    # -- config parsing ------------------------------------------------------

    @staticmethod
    def _parse_allowed(extra: dict) -> List[str]:
        raw = extra_or_secret(extra, "allowed_users", "MAX_ALLOWED_USERS", "")
        if isinstance(raw, (list, tuple)):
            return [str(x).strip() for x in raw if str(x).strip()]
        return [part.strip() for part in str(raw or "").replace(";", ",").split(",") if part.strip()]

    @property
    def name(self) -> str:
        return "MAX"

    # -- state ---------------------------------------------------------------

    def _restore_state(self) -> None:
        entry = (_load_state().get("bots") or {}).get(self._state_key) or {}
        marker = entry.get("marker")
        if isinstance(marker, int):
            self._marker = marker
        bot_id = entry.get("bot_user_id")
        if bot_id is not None:
            self._bot_user_id = str(bot_id)

    def _persist_state(self) -> None:
        if not self._state_key:
            return
        state = _load_state()
        bots = state.setdefault("bots", {})
        bots[self._state_key] = {
            "marker": self._marker,
            "bot_user_id": self._bot_user_id,
            "updated_at": int(datetime.datetime.now().timestamp()),
        }
        _save_state(state)

    # -- lifecycle -----------------------------------------------------------

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    def _ssl_context(self):
        if self.insecure_tls:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            logger.warning("MAX: TLS certificate verification is DISABLED (MAX_INSECURE_TLS)")
            return ctx
        if self.ca_bundle:
            return ssl.create_default_context(cafile=self.ca_bundle)
        return None  # aiohttp default trust store

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.token:
            return self._fail("config_missing", "MAX_BOT_TOKEN is not set", retryable=False)
        if not self._acquire_platform_lock(
            "max", self._state_key or self.token[-8:], f"MAX bot token …{self.token[-6:]}"
        ):
            return False
        try:
            import aiohttp
        except ImportError as exc:  # pragma: no cover - aiohttp ships with Hermes
            return self._fail("missing_dependency", f"aiohttp is required: {exc}", retryable=False)

        self._closing = False
        self._restore_state()
        connector = aiohttp.TCPConnector(ssl=self._ssl_context()) if self._ssl_context() is not None else None
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
        self._session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, trust_env=self.use_env_proxy
        )
        try:
            me = await self._request("GET", "/me")
        except Exception as exc:
            await self.disconnect()
            return self._fail("connect_failed", f"MAX /me failed: {exc}", retryable=True)
        if me is None:
            await self.disconnect()
            return self._fail("auth_failed", "MAX rejected the bot token (check MAX_BOT_TOKEN)", retryable=False)
        self._bot_user_id = _as_id(me.get("user_id")) or self._bot_user_id
        self._persist_state()
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        logger.info(
            "MAX: connected as %s (id=%s); marker=%s",
            me.get("first_name") or me.get("name") or "bot", self._bot_user_id, self._marker,
        )
        self._wire_plugin_handlers(None)
        return True

    async def disconnect(self) -> None:
        self._closing = True
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        await cancel_task(self._poll_task)
        self._poll_task = None
        if self._session is not None:
            with contextlib.suppress(Exception):
                await self._session.close()
            self._session = None
        self._mark_disconnected()

    # -- HTTP ----------------------------------------------------------------

    @property
    def _headers(self) -> Dict[str, str]:
        return {"Authorization": self.token, "Content-Type": "application/json"}

    async def _request(self, method: str, path: str, *, params=None, json_body=None,
                       expect_json: bool = True) -> Optional[Any]:
        """One API call. Returns parsed JSON (or True for empty 2xx bodies), None on failure."""
        if self._session is None:
            return None
        url = f"{self.api_base}{path}"
        try:
            async with self._session.request(
                method, url, params=params, json=json_body, headers=self._headers
            ) as resp:
                if resp.status == 401:
                    logger.error("MAX: 401 unauthorized — the bot token is invalid or revoked")
                    self._fail("auth_failed", "MAX rejected the bot token", retryable=False)
                    return None
                if resp.status >= 400:
                    body = (await resp.text())[:400]
                    logger.warning("MAX: %s %s -> HTTP %s: %s", method, path, resp.status, body)
                    return None
                if not expect_json:
                    return True
                text = await resp.text()
                if not text.strip():
                    return True
                try:
                    return json.loads(text)
                except ValueError:
                    logger.debug("MAX: non-JSON body from %s: %s", path, text[:200])
                    return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("MAX: %s %s failed: %s", method, path, exc)
            raise

    # -- long polling --------------------------------------------------------

    async def _poll_loop(self) -> None:
        backoff = 1.0
        while not self._closing:
            params: Dict[str, Any] = {"limit": 100, "timeout": self.poll_timeout, "types": _UPDATE_TYPES}
            if self._marker is not None:
                params["marker"] = self._marker
            try:
                data = await self._request("GET", "/updates", params=params)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("MAX: poll error, backing off %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            if data is None:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            backoff = 1.0
            if not isinstance(data, dict):
                continue
            updates = data.get("updates") or []
            if self._marker is None and updates:
                logger.info("MAX: first poll without a stored marker — starting from the newest update")
            for update in updates:
                if isinstance(update, dict):
                    with contextlib.suppress(Exception):
                        await self._handle_update(update)
            marker = data.get("marker")
            if isinstance(marker, int):
                self._marker = marker
                self._persist_state()

    # -- inbound -------------------------------------------------------------

    def _authorized(self, user_id: Optional[str]) -> bool:
        if self.allow_all or not self.allowed_users:
            return True
        return user_id is not None and user_id in self.allowed_users

    async def _handle_update(self, update: dict) -> None:
        kind = update.get("update_type")
        if kind == "bot_started":
            user = update.get("user") or {}
            logger.info(
                "MAX: user %s (%s) started the bot in chat %s",
                _as_id(user.get("user_id")), _sender_name(user), _as_id(update.get("chat_id")),
            )
            return
        if kind == "message_callback":
            await self._handle_callback(update)
            return
        if kind != "message_created":
            return
        await self._handle_message(update)

    async def _handle_message(self, update: dict) -> None:
        message = update.get("message") or {}
        body = message.get("body") or {}
        mid = body.get("mid") or (message.get("body") or {}).get("mid")
        if mid:
            if mid in self._seen_mids:
                logger.debug("MAX: duplicate update for message %s, skipping", mid)
                return
            self._seen_mids[mid] = None
            while len(self._seen_mids) > _SEEN_MIDS_MAX:
                self._seen_mids.popitem(last=False)

        sender = message.get("sender") or {}
        if sender.get("is_bot") or (self._bot_user_id and _as_id(sender.get("user_id")) == self._bot_user_id):
            return  # our own outbound message
        recipient = message.get("recipient") or {}
        chat_type_raw = str(recipient.get("chat_type") or "dialog")
        chat_type = {"dialog": "dm", "chat": "group", "channel": "channel"}.get(chat_type_raw, "group")
        chat_id = _as_id(recipient.get("chat_id")) or _as_id(recipient.get("user_id"))
        user_id = _as_id(sender.get("user_id"))
        if not chat_id:
            logger.debug("MAX: update without recipient id, dropping")
            return
        if not self._authorized(user_id):
            logger.debug("MAX: ignoring message from unauthorized user %s", user_id)
            return

        text = (body.get("text") or "").strip()
        media_urls: List[str] = []
        media_types: List[str] = []
        notes: List[str] = []
        for attachment in body.get("attachments") or []:
            if not isinstance(attachment, dict):
                continue
            cached, note = await self._cache_inbound_attachment(attachment)
            if cached is not None:
                media_urls.append(cached.path)
                media_types.append(cached.media_type)
            if note:
                notes.append(note)
        if notes:
            text = "\n".join([text] + notes).strip()
        if not text and not media_urls:
            return  # stickers/reactions we don't render

        replied_mid = replied_text = None
        link = message.get("link") or {}
        if isinstance(link, dict) and str(link.get("type") or "") == "reply":
            linked = link.get("message") or {}
            replied_mid = (linked.get("body") or {}).get("mid")
            replied_text = (linked.get("body") or {}).get("text")

        source = self.build_source(
            chat_id=chat_id,
            chat_name=(recipient.get("title") if isinstance(recipient, dict) else None) or chat_id,
            chat_type=chat_type,
            user_id=user_id,
            user_name=_sender_name(sender) or user_id or "",
            message_id=mid,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=update,
            message_id=mid,
            user_id=user_id,
            user_name=_sender_name(sender),
            media_urls=media_urls,
            media_types=media_types,
            media_text_inlined=[False] * len(media_urls),
            timestamp=datetime.datetime.fromtimestamp(
                (message.get("timestamp") or 0) / 1000.0
            ) if message.get("timestamp") else datetime.datetime.now(),
            reply_to_message_id=str(replied_mid) if replied_mid else None,
            reply_to_text=replied_text,
        )
        if self._message_handler is None:
            logger.debug("MAX: no message handler wired yet, dropping message %s", mid)
            return
        if self.mark_seen:
            asyncio.create_task(self._send_action(chat_id, "mark_seen"))
        await self.handle_message(event)

    async def _handle_callback(self, update: dict) -> None:
        callback = update.get("callback") or {}
        user = callback.get("user") or {}
        payload = str(callback.get("payload") or "").strip()
        if user.get("is_bot"):
            return
        message = update.get("message") or {}
        recipient = message.get("recipient") or {}
        chat_id = _as_id(recipient.get("chat_id")) or _as_id(user.get("user_id"))
        user_id = _as_id(user.get("user_id"))
        if not chat_id or not payload:
            logger.debug("MAX: button press without payload, ignoring")
            return
        if not self._authorized(user_id):
            return
        source = self.build_source(
            chat_id=chat_id, chat_name=chat_id, chat_type="dm" if not recipient.get("chat_id") else "group",
            user_id=user_id, user_name=_sender_name(user) or user_id or "",
            message_id=str(callback.get("callback_id") or ""),
        )
        await self.handle_message(MessageEvent(
            text=payload, message_type=MessageType.TEXT, source=source, raw_message=update,
            message_id=str(callback.get("callback_id") or "") or None,
            user_id=user_id, user_name=_sender_name(user),
            timestamp=datetime.datetime.fromtimestamp(
                (callback.get("timestamp") or 0) / 1000.0
            ) if callback.get("timestamp") else datetime.datetime.now(),
        ))

    async def _cache_inbound_attachment(self, attachment: dict):
        """Download one inbound attachment into the media cache; returns (CachedMedia|None, note|None)."""
        kind = str(attachment.get("type") or "")
        payload = attachment.get("payload") or {}
        if not isinstance(payload, dict):
            return None, None
        url = payload.get("url")
        filename = payload.get("file_name") or payload.get("filename") or f"max_{kind}"
        mime = payload.get("mime_type") or payload.get("content_type") or ""
        if not url:
            if kind == "sticker":
                return None, "[стикер]"
            if kind == "contact":
                return None, "[контакт]"
            if kind == "location":
                lat, lon = payload.get("latitude"), payload.get("longitude")
                return None, f"[локация {lat},{lon}]"
            return None, None
        try:
            async with self._session.get(url) as resp:
                if resp.status >= 400:
                    return None, f"[вложение не загрузилось: HTTP {resp.status}]"
                data = await resp.read()
        except Exception as exc:
            logger.debug("MAX: attachment download failed: %s", exc)
            return None, "[вложение не загрузилось]"
        if len(data) > self.media_max_mb * 1024 * 1024:
            return None, f"[вложение пропущено: больше {self.media_max_mb} МБ]"
        cached = await cache_media_bytes_async(data, filename=filename, mime_type=mime)
        if cached is None:
            return None, None
        return cached, None

    # -- outbound ------------------------------------------------------------

    def _lock_for(self, chat_id: str) -> asyncio.Lock:
        lock = self._send_locks.get(chat_id)
        if lock is None:
            lock = asyncio.Lock()
            self._send_locks[chat_id] = lock
        return lock

    async def _post_message(self, chat_id: str, body: Dict[str, Any]) -> Optional[dict]:
        """Rate-limited ``POST /messages``; returns the created message dict or None."""
        async with self._lock_for(chat_id):
            loop = asyncio.get_running_loop()
            wait = _SEND_MIN_INTERVAL - (loop.time() - self._last_send_at.get(chat_id, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                result = await self._request("POST", "/messages", params={"chat_id": chat_id}, json_body=body)
            finally:
                self._last_send_at[chat_id] = asyncio.get_running_loop().time()
        if isinstance(result, dict):
            return result.get("message") or {}
        return {} if result is True else None

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        if self._session is None:
            return SendResult(success=False, error="MAX is not connected")
        chunks = _split_text(content or "", self.max_message_length)
        last_id = None
        for chunk in chunks:
            body: Dict[str, Any] = {"text": chunk}
            if self.text_format in {"markdown", "html"}:
                body["format"] = self.text_format
            if reply_to:
                body["link"] = {"type": "reply", "mid": str(reply_to)}
            message = await self._post_message(chat_id, body)
            if message is None:
                return SendResult(success=False, error="MAX send failed")
            last_id = (message.get("body") or {}).get("mid") or last_id
            reply_to = None  # only the first chunk quotes the original
        return SendResult(success=True, message_id=last_id)

    async def _send_action(self, chat_id: str, action: str) -> bool:
        if self._session is None:
            return False
        try:
            result = await self._request(
                "POST", f"/chats/{chat_id}/actions", json_body={"action": action}
            )
        except Exception:
            return False
        return bool(result) if not isinstance(result, dict) else bool(result.get("success", True))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        await self._send_action(chat_id, "typing_on")

    # MAX has no explicit "typing off"; the indicator clears when the reply lands.
    async def stop_typing(self, chat_id: str) -> None:
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        info = None
        with contextlib.suppress(Exception):
            info = await self._request("GET", f"/chats/{chat_id}")
        if isinstance(info, dict) and info:
            return {
                "name": info.get("title") or info.get("name") or chat_id,
                "type": {"channel": "channel", "chat": "group"}.get(str(info.get("type") or ""), "dm"),
            }
        return {"name": chat_id, "type": "dm"}

    # -- media out -----------------------------------------------------------

    async def _upload(self, path: str, kind: str) -> Optional[dict]:
        """Upload a local file; returns the ``attachments[].payload`` dict or None."""
        if self._session is None:
            return None
        try:
            data = Path(path).read_bytes()
        except Exception as exc:
            logger.warning("MAX: cannot read %s for upload: %s", path, exc)
            return None
        try:
            import aiohttp
            upload = await self._request("POST", "/uploads", params={"type": kind})
            if not isinstance(upload, dict) or not upload.get("url"):
                logger.warning("MAX: /uploads returned no URL for %s", path)
                return None
            form = aiohttp.FormData()
            form.add_field("data", data, filename=Path(path).name, content_type=_mime_for(path))
            async with self._session.post(upload["url"], data=form, headers={"Authorization": self.token}) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    logger.warning("MAX: upload of %s failed: HTTP %s %s", path, resp.status, text[:200])
                    return None
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = {}
            payload = _media_payload_token(parsed) or _media_payload_token(upload)
            if payload is None:
                logger.warning("MAX: upload response for %s had no token: %s", path, text[:200])
            return payload
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("MAX: upload of %s raised: %s", path, exc)
            return None

    async def _send_attachment(self, chat_id: str, kind: str, payload: dict,
                               caption: Optional[str] = None, reply_to: Optional[str] = None) -> SendResult:
        """Send one attachment (retrying MAX's "attachment not ready" window once)."""
        body: Dict[str, Any] = {"attachments": [{"type": kind, "payload": payload}]}
        if caption:
            body["text"] = caption
        if reply_to:
            body["link"] = {"type": "reply", "mid": str(reply_to)}
        for attempt in (1, 2, 3):
            message = await self._post_message(chat_id, body)
            if message is not None:
                return SendResult(success=True, message_id=(message.get("body") or {}).get("mid"))
            if attempt < 3:
                await asyncio.sleep(1.5 * attempt)  # MAX processes uploads in the background
        return SendResult(success=False, error=f"MAX rejected the {kind} attachment")

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        url = str(image_url or "")
        if url.startswith(("http://", "https://")):
            return await self._send_attachment(chat_id, "image", {"url": url}, caption, reply_to)
        return await self.send_image_file(chat_id, url, caption, reply_to, metadata)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
                              **kwargs) -> SendResult:
        payload = await self._upload(image_path, "image")
        if payload is None:
            return SendResult(success=False, error=f"MAX could not upload {image_path}")
        return await self._send_attachment(chat_id, "image", payload, caption, reply_to)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
                         **kwargs) -> SendResult:
        payload = await self._upload(audio_path, "audio")
        if payload is None:
            return SendResult(success=False, error=f"MAX could not upload {audio_path}")
        return await self._send_attachment(chat_id, "audio", payload, caption, reply_to)

    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None,
                            reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
                            **kwargs) -> SendResult:
        kind = _guess_media_kind(file_path)
        payload = await self._upload(file_path, kind if kind != "image" else "image")
        if payload is None:
            return SendResult(success=False, error=f"MAX could not upload {file_path}")
        return await self._send_attachment(chat_id, kind, payload, caption, reply_to)


# ── plugin surface: probes, setup, standalone sender ─────────────────────────

def check_requirements() -> bool:
    """Passive dependency probe: aiohttp is the only requirement (ships with Hermes)."""
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(str(getattr(config, "token", None) or extra_or_secret(extra, "token", "MAX_BOT_TOKEN", "") or "").strip())


def interactive_setup() -> None:
    """``hermes gateway setup`` flow for MAX."""
    from hermes_cli.setup import (
        get_env_value, print_header, print_info, print_success, print_warning, prompt, prompt_yes_no,
        save_env_value,
    )
    from hermes_cli.setup_platforms import declines_reconfigure

    print_header("MAX")
    if declines_reconfigure("MAX", "Reconfigure MAX?", "MAX_BOT_TOKEN"):
        return
    print_info("Connect Hermes to the MAX messenger (max.ru) over the Bot API long-polling endpoint.",
               "   Get the token in MAX: Чат-боты → your bot → ⋮ → Настройки → copy the token.")
    token = prompt("MAX bot token", password=True)
    if not token:
        print_warning("A bot token is required — skipping MAX setup")
        return
    save_env_value("MAX_BOT_TOKEN", token.strip())
    print()
    if prompt_yes_no("Restrict the bot to specific MAX users?", False):
        allowed = prompt("Allowed MAX user IDs (comma-separated)",
                         default=get_env_value("MAX_ALLOWED_USERS") or "")
        save_env_value("MAX_ALLOWED_USERS", allowed.replace(" ", ""))
    else:
        save_env_value("MAX_ALLOW_ALL_USERS", "true")
        print_warning("⚠️  Open access — any MAX user who finds the bot can command it.")
    home = prompt("Home chat/user ID for cron + notifications (empty = first chat seen)",
                  default=get_env_value("MAX_HOME_CHANNEL") or "")
    save_env_value("MAX_HOME_CHANNEL", home.strip())
    if prompt_yes_no("Does MAX serve TLS with a CA your machine does not trust yet?", False):
        ca = prompt("CA bundle path (Минцифры root + sub certificates)")
        save_env_value("MAX_CA_BUNDLE", ca.strip())
    print_success("MAX configuration saved to your .env")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from the profile's env before adapter construction."""
    spec = (
        ("MAX_BOT_TOKEN", "token", None),
        ("MAX_API_BASE", "api_base", None),
        ("MAX_ALLOWED_USERS", "allowed_users", None),
        ("MAX_ALLOW_ALL_USERS", "allow_all_users", None),
        ("MAX_POLL_TIMEOUT", "poll_timeout", int),
        ("MAX_TEXT_FORMAT", "text_format", None),
        ("MAX_CA_BUNDLE", "ca_bundle", None),
        ("MAX_INSECURE_TLS", "insecure_tls", None),
        ("MAX_USE_ENV_PROXY", "use_env_proxy", None),
    )
    seeded = seed_extra_from_env(spec, home_env="MAX_HOME_CHANNEL")
    return seeded or None


def _apply_yaml_config(cfg: dict, platform_cfg: dict) -> Optional[dict]:
    """YAML → env bridge for ``gateway.platforms.max`` (env wins over YAML)."""
    if not isinstance(cfg, dict) or not cfg:
        return None
    return apply_yaml_bridge(cfg, (
        ("token", "MAX_BOT_TOKEN", "str"),
        ("api_base", "MAX_API_BASE", "str"),
        ("allowed_users", "MAX_ALLOWED_USERS", "csv"),
        ("allow_all_users", "MAX_ALLOW_ALL_USERS", "lower"),
        ("home_channel", "MAX_HOME_CHANNEL", "str"),
        ("poll_timeout", "MAX_POLL_TIMEOUT", "str"),
        ("text_format", "MAX_TEXT_FORMAT", "lower"),
        ("ca_bundle", "MAX_CA_BUNDLE", "str"),
        ("insecure_tls", "MAX_INSECURE_TLS", "lower"),
        ("use_env_proxy", "MAX_USE_ENV_PROXY", "lower"),
    ))


async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files: Optional[List[str]] = None,
                           force_document: bool = False) -> Dict[str, Any]:
    """Out-of-process send for cron delivery (``send_message`` tool).

    Async like every bundled plugin's hook — the tool ``await``s this function.
    ``thread_id`` has no MAX equivalent and is ignored.
    """
    token = str(getattr(pconfig, "token", None) or _get_scoped_secret("MAX_BOT_TOKEN", "") or "").strip()
    if not token:
        return send_error("MAX standalone send: MAX_BOT_TOKEN is not set")
    adapter = MAXAdapter(pconfig)
    adapter._state_key = _token_fingerprint(token)
    try:
        import aiohttp
        ssl_ctx = adapter._ssl_context()
        adapter._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_ctx) if ssl_ctx is not None else None,
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=30),
            trust_env=adapter.use_env_proxy,
        )
        last_id = None
        for chunk in _split_text(message or "", adapter.max_message_length):
            body: Dict[str, Any] = {"text": chunk}
            if adapter.text_format in {"markdown", "html"}:
                body["format"] = adapter.text_format
            sent = await adapter._post_message(chat_id, body)
            if sent is None:
                return send_error(f"MAX standalone send failed for chat {chat_id}")
            last_id = (sent.get("body") or {}).get("mid") or last_id
        for path in media_files or []:
            kind = _guess_media_kind(path)
            if not force_document and kind == "image":
                result = await adapter.send_image_file(chat_id, path)
            else:
                result = await adapter.send_document(chat_id, path)
            if not result.success:
                return send_error(result.error or f"MAX standalone media send failed: {path}")
            last_id = result.message_id or last_id
        return {"success": True, "message_id": last_id or ""}
    except Exception as exc:
        logger.debug("MAX standalone send raised", exc_info=True)
        return send_error(f"MAX standalone send failed: {exc}")
    finally:
        if adapter._session is not None:
            with contextlib.suppress(Exception):
                await adapter._session.close()


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="max",
        label="MAX",
        adapter_factory=MAXAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MAX_BOT_TOKEN"],
        install_hint="No extra packages needed (aiohttp ships with Hermes)",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=MAX_TEXT_LIMIT,
        emoji="🅜",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting through the MAX messenger (max.ru). MAX renders plain text; "
            "keep formatting light (no tables) and split long answers into short paragraphs. "
            "Messages longer than 4000 characters are split automatically. Images and files "
            "arriving from the user are downloaded locally for you to inspect."
        ),
    )
