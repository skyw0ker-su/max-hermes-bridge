"""Registration test: the plugin registers through the REAL Hermes plugin API.

Proves `register(ctx)` + `ctx.register_platform(...)` are accepted by the core
(no signature drift), that the platform lands in the live platform registry, that
the Platform enum resolves "max" afterwards, and that the factory/env hooks wired
on the PlatformEntry are the ones the gateway will call.

    HERMES_REPO=... MAX_TEST_HOME=... <venv python> tests/test_plugin_registration.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
REPO = Path(os.environ.get("HERMES_REPO") or (Path(os.environ.get("LOCALAPPDATA", ".")) / "hermes" / "hermes-agent"))
TEST_HOME = Path(os.environ.get("MAX_TEST_HOME") or (Path(os.environ.get("TEMP", ".")) / "max_test_home"))
sys.path.insert(0, str(REPO))
os.environ["HERMES_HOME"] = str(TEST_HOME)
os.environ.pop("MAX_BOT_TOKEN", None)

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
    name = "hermes_plugins.max_platform_regtest"
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
    return sys.modules[f"{name}.adapter"]


def main() -> int:
    adapter_mod = load_plugin()

    from hermes_cli.plugins import PluginContext, PluginManager
    from hermes_cli.plugins_manifest import PluginManifest
    from gateway.config import Platform, PlatformConfig
    from gateway.platform_registry import platform_registry

    check("platform is not registered before register()", not platform_registry.is_registered("max"))

    manifest = PluginManifest(
        name="max-platform", key="max-platform", kind="platform", source="user",
        path=str(PLUGIN_DIR), version="1.0.0",
        description="MAX (max.ru) messenger adapter",
    )
    ctx = PluginContext(manifest, PluginManager())
    adapter_mod.register(ctx)  # raises if any kwarg is not part of the plugin API

    check("register(ctx) accepted by the core API", True)
    check("platform registered in the live registry", platform_registry.is_registered("max"))
    entry = platform_registry.get("max")
    check("entry carries the MAX metadata",
          entry.label == "MAX" and entry.emoji == "🅜" and entry.max_message_length == 4000,
          f"{entry.label} {entry.emoji} {entry.max_message_length}")
    check("entry is owned by our plugin",
          entry.source == "plugin" and entry.plugin_name == "max-platform",
          f"{entry.source} / {entry.plugin_name}")
    check("allowlist hooks wired",
          entry.allowed_users_env == "MAX_ALLOWED_USERS" and entry.allow_all_env == "MAX_ALLOW_ALL_USERS",
          f"{entry.allowed_users_env} {entry.allow_all_env}")
    check("required env declared", entry.required_env == ["MAX_BOT_TOKEN"], str(entry.required_env))
    check("cron delivery env wired", entry.cron_deliver_env_var == "MAX_HOME_CHANNEL", entry.cron_deliver_env_var)
    check("setup flow/standalone sender/hooks present",
          callable(entry.setup_fn) and callable(entry.standalone_sender_fn)
          and callable(entry.env_enablement_fn) and callable(entry.apply_yaml_config_fn)
          and callable(entry.is_connected),
          "a hook is missing")

    # Platform enum: a registered plugin platform must resolve to a member.
    check("Platform('max') resolves once registered", Platform("max").value == "max")

    # The gateway's passive dependency probe + config validation.
    check("check_fn (passive deps probe) passes", entry.check_fn() is True)
    good = PlatformConfig(enabled=True, token="token-123", extra={})
    bare = PlatformConfig(enabled=True, token=None, extra={})
    check("validate_config needs a token", entry.validate_config(good) is True and entry.validate_config(bare) is False)

    # The factory the gateway calls to build the adapter.
    adapter = entry.adapter_factory(good)
    check("adapter_factory builds a configured adapter",
          type(adapter).__name__ == "MAXAdapter" and adapter.name == "MAX" and adapter.platform.value == "max",
          f"{type(adapter).__name__} {getattr(adapter, 'name', '?')}")

    # Env/YAML seeding hooks.
    config_yaml = {"allowed_users": "111,222", "poll_timeout": 45, "text_format": "markdown", "nonsense": "x"}
    seeded = entry.apply_yaml_config_fn(config_yaml, {}) or {}
    check("YAML bridge seeds extra keys",
          seeded.get("allowed_users") == "111,222" and seeded.get("poll_timeout") == 45
          and seeded.get("text_format") == "markdown" and "nonsense" not in seeded,
          str(seeded))
    # (The YAML bridge above writes its env rows when unset — that IS the documented
    #  env-wins-over-YAML contract, so clear them before asserting the empty-env case.)
    for key in ("MAX_ALLOWED_USERS", "MAX_POLL_TIMEOUT", "MAX_TEXT_FORMAT", "MAX_BOT_TOKEN", "MAX_HOME_CHANNEL"):
        os.environ.pop(key, None)
    check("env_enablement returns None without env",
          entry.env_enablement_fn() is None, str(entry.env_enablement_fn()))

    os.environ["MAX_BOT_TOKEN"] = "env-token"
    os.environ["MAX_ALLOWED_USERS"] = "42,43"
    os.environ["MAX_HOME_CHANNEL"] = "42"
    try:
        seeded_env = entry.env_enablement_fn() or {}
        check("env_enablement seeds token/allowlist/home from env",
              seeded_env.get("token") == "env-token" and seeded_env.get("allowed_users") == "42,43"
              and (seeded_env.get("home_channel") or {}).get("chat_id") == "42",
              str(seeded_env))
        from_env = entry.adapter_factory(PlatformConfig(enabled=True, token=None, extra={}))
        check("adapter picks the token up from the env",
              from_env.token == "env-token" and from_env.allowed_users == ["42", "43"],
              f"{from_env.token} {from_env.allowed_users}")
    finally:
        for key in ("MAX_BOT_TOKEN", "MAX_ALLOWED_USERS", "MAX_HOME_CHANNEL"):
            os.environ.pop(key, None)

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
