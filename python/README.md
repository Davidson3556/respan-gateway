# Python distribution

This directory is reserved for the Python package intended for PyPI, including
its PyO3 extension, Python API, packaging metadata and Python tests. No Python
bindings or installable package are implemented yet.

The intended structure when implementation begins is:

```text
python/
  pyproject.toml          # Python build and distribution metadata
  Cargo.toml             # PyO3 extension crate
  src/                   # Rust bindings over the gateway libraries
  python/respan_gateway/ # Python API and type information
  tests/                 # Python API and package installation tests
```

Add the extension crate to the root Cargo workspace when its manifest exists.
It should depend on the reusable libraries in `crates/`. The standalone Rust
gateway executable remains in `apps/gateway/`.

The PyPI project name, supported Python versions and wheel build matrix will
be established with the package implementation.
