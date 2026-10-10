"""The `respan-gateway` command: the same command line as the Rust binary."""

import sys

from . import _native


def main() -> None:
    sys.exit(_native.run_cli(["respan-gateway", *sys.argv[1:]]))


if __name__ == "__main__":
    main()
