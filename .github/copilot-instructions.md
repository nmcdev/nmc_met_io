# Repository instructions

## Development commands

Follow [AGENTS.md](../AGENTS.md): use `uv` exclusively for Python environments, dependencies, and execution. Its toolchain requirements take precedence over the older installation commands in [README.md](../README.md).

[pyproject.toml](../pyproject.toml) defines package metadata, runtime dependencies, and the `dev` dependency group. [setup.py](../setup.py) is a compatibility entry point. [uv.lock](../uv.lock) locks dependencies across supported Python versions; [.python-version](../.python-version) selects Python 3.12 for development. Run commands from the repository root; `uv sync` creates `.venv` and installs the local package editable with pytest and Ruff.

```powershell
# Restore the locked development environment
uv sync --locked

# Full regression suite
uv run pytest

# One test (runs its parametrized cases)
uv run pytest tests\test_read_micaps.py::test_pressure_is_decoded_once

# Formatting and linting required by AGENTS.md
uv run ruff format .
uv run ruff check --fix .

# Build the source distribution and wheel
uv build
```

Ruff targets Python 3.9 and excludes generated `DataBlock_pb2.py` and example notebooks (some contain non-Python code cells); its standard Python rule selection is explicit in `pyproject.toml`. Whole-tree formatting/fixes can touch legacy code: inspect the diff and retain only changes belonging to the requested task.

The existing pytest suite, [tests/test_read_micaps.py](../tests/test_read_micaps.py), exercises MICAPS 1 pressure decoding, 24/26-column layouts, missing values, spatial filtering, and bundled surface samples. Keep these tests offline; sample paths are resolved relative to the repository, and synthetic files use pytest's `tmp_path`.

## Architecture

- This is an importable scientific I/O library, not an application. [nmc_met_io/__init__.py](../nmc_met_io/__init__.py) supplies the dynamic package version to setuptools; other package metadata lives in `pyproject.toml`. Keep initialization lightweight.
- `read_*.py` modules decode local meteorological formats; `retrieve_*.py` modules implement source-specific access and often decode responses themselves; `export_*.py` modules write MICAPS formats or adapt radar objects. There is no universal reader/retrieval facade. Extend the relevant format or provider module rather than adding logic to package initialization.
- [retrieve_micaps_server.py](../nmc_met_io/retrieve_micaps_server.py) uses the GDS HTTP service and protobuf response envelopes. [retrieve_cassandraDB.py](../nmc_met_io/retrieve_cassandraDB.py) accesses Cassandra directly. Similar decoder implementations exist in both; inspect the corresponding path when changing shared binary-format behavior without conflating their transports.
- [config.py](../nmc_met_io/config.py) centralizes service credentials and cache paths. [util.py](../nmc_met_io/util.py) provides product filenames, forecast-time selection, and related helpers. [examples/](../examples/) notebooks demonstrate provider-specific usage; [documents/](../documents/) contains format specifications and service dictionaries.
- Edit the source under `nmc_met_io`, not copies in `build/lib`, distribution archives, or egg metadata. [DataBlock_pb2.py](../nmc_met_io/DataBlock_pb2.py) is generated from `DataBlock.proto`; do not hand-edit its descriptors.

## Configuration and data contracts

- Importing `nmc_met_io.config` creates `~/.nmcdev` and immediately reads `~/.nmcdev/config.ini`, falling back to `~/config_met_io.ini`; it raises if neither exists. Retrieval-module imports can therefore require configuration before any request is made. Use the section/key names in the [README configuration template](../README.md#设置配置文件), and keep actual credentials outside the repository.
- Retrieval functions commonly expose `cache` and `cache_clear`. Shared `get_cache_file` appends `cache` to the configured `CACHE_DIR`, or uses `~/.nmcdev/cache`; with clearing enabled it uses `cached_YYYYMMDD` directories and removes expired directories (default seven days). Many retrievers pickle decoded results. Account for filesystem side effects when testing cache behavior.
- Station observations generally return pandas DataFrames; regular grids generally return xarray Datasets. Preserve variable names, coordinate attributes, units, and singleton dimensions. Common grid dimensions are `time`, optional `level`, `lat`, `lon`; ensemble dimensions differ by provider (`number` in MICAPS versus `realization` in CMADaaS).
- Forecast grids attach `forecast_reference_time` and a `forecast_period` coordinate on `time` with units `hour`. Preserve these alongside valid-time coordinates. MICAPS server grids sort latitude ascending before returning.
- Geographic argument order is API-specific: local MICAPS readers and many CMADaaS functions use `[min_lat, min_lon, max_lat, max_lon]`, while MICAPS server station retrieval documents `[lon0, lon1, lat0, lat1]`. Check the individual function rather than applying one convention globally.
- MICAPS text readers try UTF-8, UTF-8 with BOM, GB18030, and GBK; numeric missing values such as `9999` become NaN while station `ID` stays textual. Other services have different missing-value thresholds. Preserve each format's decoding and missing-data rules.
- Many readers/retrievers return `None` for missing files, unavailable data, or empty selections, while malformed records can raise. Preserve each function's existing behavior. MICAPS 1 spatial filtering retains the original DataFrame index, so numerical transformations must preserve row alignment.
- GRIB support requires cfgrib/ecCodes, radar conversion requires Py-ART, and direct Cassandra access requires cassandra-driver; these are optional and not included in `pyproject.toml` runtime dependencies. Keep format-specific optional dependencies out of unrelated import paths.

For changed/new functions, follow the annotation and pytest conventions in [AGENTS.md](../AGENTS.md), while preserving the Python `>=3.9` package requirement declared in `pyproject.toml` unless a runtime-support change is explicitly requested. Use Conventional Commits as specified there.
