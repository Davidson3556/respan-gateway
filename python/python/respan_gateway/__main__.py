"""The `respan-gateway` command: the same command line as the Rust binary."""

import signal
import sys

from . import _native


def main() -> None:
    # The command line handles Ctrl-C itself (a graceful stop, like the
    # binary); Python's handler would add a KeyboardInterrupt traceback.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    sys.exit(_native.run_cli(["respan-gateway", *sys.argv[1:]]))


if __name__ == "__main__":
    main()
