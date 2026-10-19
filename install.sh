#!/bin/sh
# Install respan-gateway from its GitHub release, on Linux or macOS.
#
#   curl -fsSL https://raw.githubusercontent.com/respanai/respan-gateway/main/install.sh | sh
#
# RESPAN_GATEWAY_VERSION picks a release (e.g. v0.1.0; the latest by default),
# RESPAN_GATEWAY_INSTALL_DIR where the binary goes (~/.local/bin by default).
# The archive is checked against the release's SHA256SUMS before it is unpacked.

set -eu

REPO="respanai/respan-gateway"
BIN="respan-gateway"

say() { printf '%s\n' "$*" >&2; }
die() { say "error: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required"; }

fetch() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --proto '=https' --tlsv1.2 -o "$2" "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -q --https-only -O "$2" "$1"
    else
        die "curl or wget is required"
    fi
}

# The tag the releases/latest page redirects to, without the rate-limited API.
latest() {
    url="https://github.com/$REPO/releases/latest"
    if command -v curl >/dev/null 2>&1; then
        final=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$url")
    else
        final=$(wget -q --max-redirect=5 -S --spider "$url" 2>&1 | sed -n 's/^ *[Ll]ocation: *//p' | tail -n 1 | tr -d '\r')
    fi
    tag=${final##*/}
    case "$tag" in
        v*) printf '%s' "$tag" ;;
        *) die "no release of $REPO is published yet; build from source (see the README)" ;;
    esac
}

target() {
    os=$(uname -s)
    arch=$(uname -m)
    case "$os" in
        Linux)
            if ldd --version 2>&1 | grep -qi musl; then
                die "only glibc Linux builds are published; on musl, build from source"
            fi
            case "$arch" in
                x86_64 | amd64) echo x86_64-unknown-linux-gnu ;;
                aarch64 | arm64) echo aarch64-unknown-linux-gnu ;;
                *) die "no Linux build for $arch" ;;
            esac
            ;;
        Darwin)
            # A shell under Rosetta reports x86_64 on Apple silicon.
            if [ "$arch" = x86_64 ] && [ "$(sysctl -n hw.optional.arm64 2>/dev/null)" = 1 ]; then
                arch=arm64
            fi
            case "$arch" in
                arm64 | aarch64) echo aarch64-apple-darwin ;;
                x86_64) echo x86_64-apple-darwin ;;
                *) die "no macOS build for $arch" ;;
            esac
            ;;
        *) die "unsupported OS $os; on Windows, use install.ps1" ;;
    esac
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d ' ' -f 1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d ' ' -f 1
    else
        die "sha256sum or shasum is required"
    fi
}

main() {
    need uname
    need tar
    target=$(target)
    version=${RESPAN_GATEWAY_VERSION:-$(latest)}
    case "$version" in v*) ;; *) version="v$version" ;; esac
    dir=${RESPAN_GATEWAY_INSTALL_DIR:-$HOME/.local/bin}

    name="$BIN-$version-$target"
    base="https://github.com/$REPO/releases/download/$version"
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT INT TERM

    say "Downloading $name"
    fetch "$base/$name.tar.gz" "$tmp/$name.tar.gz" || die "no $name.tar.gz in release $version"
    fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" || die "no SHA256SUMS in release $version"

    want=$(grep " \*\{0,1\}$name.tar.gz\$" "$tmp/SHA256SUMS" | cut -d ' ' -f 1)
    [ -n "$want" ] || die "$name.tar.gz is not in SHA256SUMS"
    [ "$(sha256 "$tmp/$name.tar.gz")" = "$want" ] || die "checksum mismatch for $name.tar.gz"

    tar xzf "$tmp/$name.tar.gz" -C "$tmp"
    mkdir -p "$dir"
    mv "$tmp/$name/$BIN" "$dir/$BIN.tmp"
    chmod 755 "$dir/$BIN.tmp"
    mv "$dir/$BIN.tmp" "$dir/$BIN"

    say "Installed $BIN $version to $dir/$BIN"
    case ":$PATH:" in
        *":$dir:"*) ;;
        *) say "Add $dir to your PATH:  export PATH=\"$dir:\$PATH\"" ;;
    esac
    say "Run '$BIN init' to write a config, then '$BIN' to start it."
}

main "$@"
