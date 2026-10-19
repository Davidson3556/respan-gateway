"""Publish every workspace crate whose version is not on crates.io yet.

`--allow-dirty` lets in the built UI copied into apps/gateway/ui-dist/, which
git ignores but the crate carries; a release checks out a clean tree.

Crates go up in dependency order, so each finds the ones it needs. A crate
whose version is already there is skipped, which makes a rerun safe and lets
each crate keep its own version: bump one, and only it is published. When
crates.io rate-limits new crates, this waits as long as it asks and retries.

    python3 .github/scripts/publish-crates.py [--dry-run]
"""

import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

DRY_RUN = "--dry-run" in sys.argv


def workspace():
    """The publishable workspace crates, each after the crates it needs."""
    meta = json.loads(
        subprocess.check_output(["cargo", "metadata", "--no-deps", "--format-version", "1"])
    )
    crates = {
        p["name"]: p
        for p in meta["packages"]
        if p["id"] in meta["workspace_members"] and p["publish"] != []
    }
    order, done = [], set()

    def visit(name):
        if name in done:
            return
        done.add(name)
        for dep in crates[name]["dependencies"]:
            # Dev-dependencies on the workspace are path-only and dropped on publish.
            if dep["name"] in crates and dep["kind"] != "dev":
                visit(dep["name"])
        order.append(name)

    for name in sorted(crates):
        visit(name)
    return [(name, crates[name]["version"]) for name in order]


def published(name, version):
    """Whether crates.io has `name` at `version`, from its sparse index."""
    lower = name.lower()
    path = {1: "1", 2: "2", 3: f"3/{lower[0]}"}.get(len(lower), f"{lower[:2]}/{lower[2:4]}")
    request = urllib.request.Request(
        f"https://index.crates.io/{path}/{lower}",
        headers={"User-Agent": "respan-gateway release (github.com/respanai/respan-gateway)"},
    )
    try:
        with urllib.request.urlopen(request) as response:
            lines = response.read().decode().splitlines()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise
    return any(json.loads(line)["vers"] == version for line in lines if line)


def wait_for_rate_limit(output):
    """Sleep until the time a 429 names, or ten minutes if it names none."""
    found = re.search(r"try again after (.+? GMT)", output)
    delay = 600
    if found:
        try:
            when = datetime.strptime(found.group(1), "%a, %d %b %Y %H:%M:%S GMT")
            delay = (when.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)).total_seconds()
        except ValueError:
            pass
    delay = max(delay, 0) + 15
    print(f"  rate-limited by crates.io; waiting {delay:.0f}s", flush=True)
    time.sleep(delay)


def publish(name, version):
    while True:
        result = subprocess.run(
            ["cargo", "publish", "-p", name, "--locked", "--allow-dirty"],
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        if result.returncode == 0:
            print(f"  published {name} {version}", flush=True)
            return
        if "already exists" in output:
            print(f"  {name} {version} is already on crates.io", flush=True)
            return
        if "429" in output or "Too Many Requests" in output:
            wait_for_rate_limit(output)
            continue
        print(output, file=sys.stderr)
        sys.exit(f"publishing {name} {version} failed")


def main():
    crates = workspace()
    todo = [(n, v) for n, v in crates if not published(n, v)]
    for name, version in crates:
        print(f"{name} {version}: {'to publish' if (name, version) in todo else 'on crates.io'}")
    if not todo:
        print("Nothing to publish.")
        return
    if DRY_RUN:
        # Packages and builds every crate as published, resolving the ones not
        # on crates.io yet against each other.
        subprocess.run(
            ["cargo", "package", "--locked", "--allow-dirty"] + [arg for n, _ in todo for arg in ("-p", n)],
            check=True,
        )
        return
    for name, version in todo:
        print(f"Publishing {name} {version}", flush=True)
        publish(name, version)


if __name__ == "__main__":
    main()
