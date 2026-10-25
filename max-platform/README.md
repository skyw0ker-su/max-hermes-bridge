# MAX platform plugin for Hermes Agent

Connects Hermes to the **MAX** messenger (max.ru, the Russian national messenger) as a
first-class channel: a message from MAX runs a normal Hermes turn and the answer is sent
back into MAX, with typing indicator, read receipts, images and files in both directions,
allowlist enforcement, home channel for cron delivery, and a poll marker that survives
gateway restarts.

* Transport: MAX **Bot API, Long Polling** — `https://platform-api2.max.ru`
  (webhooks need a public HTTPS endpoint with trusted certs; polling is the right fit for a
  home PC behind CGNAT/NAT).
* Dependencies: **aiohttp only** (already shipped with Hermes). Nothing to pip-install.
* Hermes core is untouched: everything lives in this plugin directory.

## Install

```bash
# 1. token in .env (never on the command line — it lands in shell history)
#    add to  %LOCALAPPDATA%\hermes\.env
#      MAX_BOT_TOKEN=<token from MAX: Чат-боты → bot → ⋮ → Настройки>
#      MAX_ALLOWED_USERS=1234567            # comma-separated MAX user IDs; empty = allow all
#      MAX_HOME_CHANNEL=1234567             # cron / notification target (usually your own user id)

# 2. enable the plugin (opt-in; writes plugins.enabled in config.yaml)
hermes plugins enable max-platform

# 3. restart the gateway so the plugin loads
hermes gateway restart
```

`hermes gateway setup` also asks for MAX and writes the same env vars
(the flow is implemented in `interactive_setup()`).

## Verify

```bash
HERMES_REPO=<hermes-agent repo> MAX_TEST_HOME=<scratch dir> \
  <venv python> tests/test_max_adapter.py          # 33 checks, mock MAX API
HERMES_REPO=<hermes-agent repo> MAX_TEST_HOME=<scratch dir> \
  <venv python> tests/test_plugin_registration.py  # 17 checks, real plugin API
```

Then live: write to the bot in MAX → the turn runs, the reply comes back, "печатает…" is
shown while the model works.

## Configuration

`config.yaml` → `gateway.platforms.max.extra` (env always wins over YAML):

| key | env | default | meaning |
| --- | --- | --- | --- |
| `token` | `MAX_BOT_TOKEN` | — | bot token (Authorization header only) |
| `api_base` | `MAX_API_BASE` | `https://platform-api2.max.ru` | API host |
| `allowed_users` | `MAX_ALLOWED_USERS` | empty = everyone | comma-separated MAX user IDs |
| `allow_all_users` | `MAX_ALLOW_ALL_USERS` | false | explicit open access |
| `home_channel` | `MAX_HOME_CHANNEL` | — | cron/notification target |
| `poll_timeout` | `MAX_POLL_TIMEOUT` | 30 | long-poll seconds (0–90) |
| `text_format` | `MAX_TEXT_FORMAT` | plain | `markdown` or `html` |
| `ca_bundle` | `MAX_CA_BUNDLE` | system store | CA file (Минцифры root+sub) if MAX TLS is not trusted |
| `insecure_tls` | `MAX_INSECURE_TLS` | false | disable TLS verification (last resort) |
| `use_env_proxy` | `MAX_USE_ENV_PROXY` | false | honour `HTTP(S)_PROXY` from the gateway env (off = direct) |
| `mark_seen` | `MAX_MARK_SEEN` | true | send `mark_seen` (read receipt) on inbound |
| `media_max_mb` | `MAX_MEDIA_MAX_MB` | 20 | max inbound attachment size to download |
| `max_message_length` | — | 4000 | outbound chunking cap |

## How it works

`GET /updates` long-polls with `types=message_created,message_callback,bot_started`; the
returned `marker` is persisted to `<HERMES_HOME>/max_poll_state.json` **keyed by a hash of
the bot token**, so a restart resumes where it left off instead of replaying history (the
`max-bridge` DeepSeek harness kept the marker in memory and lost it on restart). Inbound
messages are deduplicated by `body.mid` in a bounded LRU, and the bot's own messages are
dropped via `sender.is_bot` / `/me` user id.

Outbound goes through `POST /messages?chat_id=…` with a per-chat 0.55 s send lock (MAX
allows 2 messages/second per chat), automatic chunking at 4000 characters, and
`link: {type: "reply", mid}` so answers quote the message that triggered them. Local files
are uploaded with `POST /uploads` → multipart `data` field → the returned token/photos map
is sent as an attachment, with retries for MAX's asynchronous attachment processing.

A second consumer on the same bot token is blocked by Hermes' scoped platform lock, so the
gateway and a standalone cron sender cannot double-poll.

## Known limitations

* Stickers, contacts and locations arrive as a text placeholder, not as media.
* Inline keyboards are not sent (button presses from other bots/hand-made keyboards are
  treated as text: the callback payload is fed to the agent).
* `markdown`/`html` formatting is off by default because MAX's renderer is stricter than
  Hermes' markdown; plain text is safer for model output. Turn it on with `MAX_TEXT_FORMAT`.
* Mock-API tests live in
  `tests/`; the mock server follows the official `max-messenger/api-schema`.
