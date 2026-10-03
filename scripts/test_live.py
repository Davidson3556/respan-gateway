#!/usr/bin/env python3
"""Explicit, charge-incurring live tests. Keys stay in child environments.

Examples:
  python3 scripts/test_live.py anthropic gemini typesafe respan
  OPENAI_MODEL=gpt-4o-mini python3 scripts/test_live.py openai
Environment variables take precedence over optional files in --keys-dir.
"""

import argparse
import os
from pathlib import Path
import re
import signal
import subprocess
import sys


def credential(env, name, directory, filename):
    if not env.get(name):
        path = directory / filename
        if path.exists():
            env[name] = path.read_text().strip()
    if not env.get(name):
        raise ValueError(f"{name} is unset and {filename} is missing or empty")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("providers", nargs="+", choices=["anthropic", "gemini", "typesafe", "respan", "openai", "mcp"])
    parser.add_argument("--keys-dir", type=Path, default=Path.home())
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    failed = False
    for provider in args.providers:
        env = os.environ.copy()
        cases = []
        if provider == "mcp":
            cases.append(("mcp", "real_server", "official_stdio_server_initializes_lists_calls_and_deletes"))
            cases.append(("mcp", "real_server", "official_http_server_initializes_lists_calls_and_deletes"))
        elif provider == "anthropic":
            credential(env, "ANTHROPIC_API_KEY", args.keys_dir, "ant_key")
            cases.append(("gateway", "real_providers", "real_anthropic_streamed_tool_call_translates_to_openai"))
            cases.append(("gateway", "real_providers", "real_openai_client_to_anthropic_streamed"))
        elif provider == "gemini":
            credential(env, "GEMINI_API_KEY", args.keys_dir, "gemini_key")
            cases.append(("gateway", "real_providers", "real_gemini_native_stream_through_passthrough"))
            cases.append(("axum", "real_ws", "real_gemini_live_audio_and_transcription_through_tls_gateway"))
        elif provider == "typesafe":
            credential(env, "TYPESAFE_API_KEY", args.keys_dir, "typesafe_key")
            cases.append(("gateway", "real_providers", "real_typesafe_custom_decisions_through_passthrough"))
        elif provider == "respan":
            if not env.get("RESPAN_API_KEY"):
                text = (args.keys_dir / "respan_key").read_text()
                key = re.search(r"OPENAI API KEY\s*=\s*(\S+)", text)
                base = re.search(r"OPENAI BASE\s*=\s*(\S+)", text)
                if key is None or base is None:
                    raise ValueError("respan_key needs OPENAI API KEY = ... and OPENAI BASE = ...")
                env["RESPAN_API_KEY"] = key[1]
                env.setdefault("RESPAN_BASE_URL", base[1])
            env["OPENAI_API_KEY"] = env["RESPAN_API_KEY"]
            env["OPENAI_BASE_URL"] = env.get("RESPAN_BASE_URL", "https://api.respan.ai/api")
            env["OPENAI_MODEL"] = env.get("RESPAN_MODEL", "openrouter/openai/gpt-4o-mini")
            cases.extend(("gateway", "real_providers", name) for name in ["real_openai_chat_relay", "real_compatible_native_tool_stream_through_passthrough"])
        else:
            credential(env, "OPENAI_API_KEY", args.keys_dir, "openai_key")
            cases.extend(("gateway", "real_providers", name) for name in ["real_openai_chat_relay", "real_openai_streamed_unicode_tool_call"])
            cases.append(("axum", "real_ws", "real_openai_realtime_text_turn_through_tls_gateway"))
        for package, target, name in cases:
            command = ["cargo", "test", "-p", f"respan-gateway-{package}" if package != "gateway" else "respan-gateway"]
            if package == "axum":
                command.extend(["--features", "ws"])
            command.extend(["--test", target, name, "--", "--ignored", "--exact", "--nocapture"])
            print(f"\n{provider}: {name}", flush=True)
            process = subprocess.Popen(command, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
            try:
                output, _ = process.communicate(timeout=180)
            except subprocess.TimeoutExpired:
                # Include Cargo's test binary and any MCP child in cleanup.
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                print("FAILED: live test exceeded 180 seconds")
                failed = True
                continue
            for var, value in env.items():
                if var.endswith("API_KEY") and value:
                    output = output.replace(value, "<redacted>")
            print(output, end="")
            if not re.search(r"(?m)^running 1 test$", output):
                print("FAILED: Cargo did not select exactly one live test")
                failed = True
            failed |= process.returncode != 0
    return int(failed)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError) as error:
        sys.exit(str(error))
