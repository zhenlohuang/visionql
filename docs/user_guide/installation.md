# Installation and configuration

This guide covers the CLI, Python API, `vqld`, Docker, Workbench, and shared runtime configuration. Commands run from the repository root unless stated otherwise. The [SQL reference](sql-reference.md) describes VQL extensions; [examples](../../examples/README.md) cover complete workflows.

## Availability

The latest published release is v0.2.0, distributed as source through the [GitHub release](https://github.com/zhenlohuang/visionql/releases/tag/v0.2.0). The Python package has not yet been published to PyPI. Build from source for the Python API and standalone `vql` CLI. Workbench is implemented in the current source checkout for the upcoming v0.3 release and is absent from the v0.2.0 release.

The commands below use the current source checkout. See the [Roadmap](../../ROADMAP.md) and [Changelog](../../CHANGELOG.md) for released and unreleased scope, including the current `SHOW JOBS` spelling.

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install), installed with `rustup`. The workspace declares Rust 1.88 as its minimum supported version; `rust-toolchain.toml` selects Rust 1.91.1 for repository builds.
- Python 3.10 or newer, available as `python` in your active environment. Use `python3` to create that environment if your system has no `python` command.
- [FFmpeg 8](https://ffmpeg.org/download.html), including development libraries for the default native video build, and `ffmpeg` / `ffprobe` on `PATH` for sample preparation.
- A C/C++ build toolchain, Clang/libclang, `pkg-config`, `make`, CMake, and Perl for native dependencies. The bundled `librdkafka` build uses vendored OpenSSL; a system `librdkafka` installation is unnecessary.
- For Workbench: Node.js 22.12 or newer and pnpm 10.28.0, selected by the frontend's `packageManager` field.
- For Docker: Docker with Compose support. The image build supplies its Rust and native build dependencies.

## Build from source

```bash
git clone https://github.com/zhenlohuang/visionql.git
cd visionql

export VQL_HOME="$PWD/data/.vql"
python3 -m venv .venv
source .venv/bin/activate
cargo build --workspace --locked
```

Keep `VQL_HOME` set to the same absolute path in each terminal that should share this instance. `data/.vql` is ignored development state; when the variable is unset, the default instance is `$HOME/.vql`.

The development executables are `target/debug/vql` and `target/debug/vqld`. To install the CLI and daemon into Cargo's executable directory:

```bash
cargo install --path vql-cli --locked
cargo install --path vql-server --bin vqld --locked
```

Ensure Cargo's executable directory, normally `$HOME/.cargo/bin`, is on `PATH`. You can also run directly from the checkout as shown below.

## Sample data and models

Fetch the small image and video samples:

```bash
python scripts/fetch_datasets.py
```

Export the detector used by the typed Model examples:

```bash
python -m pip install ultralytics huggingface_hub onnx
python scripts/export_yolo26.py --task detect --size n
```

This writes `data/models/yolo26n.onnx`. To use `VQL_CLASSIFY` and `VQL_DETECT` without registering Catalog Models, install their release-managed artifacts under the active `VQL_HOME`:

```bash
python scripts/export_yolo26.py --task classify --install
python scripts/export_yolo26.py --task detect --install
```

The classifier is installed at `$VQL_HOME/models/yolo26n-cls.onnx` and the detector at `$VQL_HOME/models/yolo26n.onnx`. A Catalog Model's exported artifact and these built-in artifacts have separate lifecycles. The [dataset notes](../../data/datasets/README.md) document provenance; the [model notes](../../data/models/README.md) document the export contract.

## CLI

Open an embedded SQL shell:

```bash
cargo run -q -p vql-cli -- shell
```

Register the downloaded images and run a bounded query:

```sql
CREATE TABLE sample_images
USING IMAGES
LOCATION './data/datasets/images/coco128/images'
OPTIONS (recursive = true);

SELECT uri, width, height
FROM sample_images
ORDER BY uri
LIMIT 5;
```

The Catalog persists the definition. New CLI or Python Sessions using the same `VQL_HOME` can query `sample_images` without registering it again.

The command surface is:

```text
vql shell [--endpoint URI] [--token TOKEN] [--tls-ca PEM]
vql run <script.sql>
```

During source development, replace `vql` with `cargo run -p vql-cli --`. `vql run` executes a script through the embedded engine. An unbounded statement must be last. `EXPLAIN` is a SQL statement, usable in the shell or a script.

In the shell, enter `\q` on its own line or press Ctrl-D to exit. Ctrl-C clears pending input at the prompt. During embedded unbounded execution, the first Ctrl-C requests graceful stop and a second cancels immediately. A remote shell cancels the active Flight execution. See the [SQL reference](sql-reference.md#tumble-and-streaming-queries) for continuous execution.

## Python API

Build the extension into the active virtual environment:

```bash
source .venv/bin/activate
export VQL_HOME="$PWD/data/.vql"
python -m pip install "maturin>=1.9,<2"

cd vql-python
maturin develop --locked
cd ..
```

The synchronous API returns a `QueryHandle`; `collect()` materializes a PyArrow table. This example reuses `sample_images` registered in the CLI above:

```python
import visionql

session = visionql.connect()
result = session.sql("""
    SELECT uri, width, height
    FROM sample_images
    ORDER BY uri
    LIMIT 5
""")
print(result.collect())
```

`visionql.connect(catalog=..., session_memory_limit_bytes=...)` can override the SQLite Catalog path and Session memory limit. Python-hosted UDFs receive and return Arrow arrays in batches; execution requires the Python host and is unavailable through the standalone CLI, `vqld`, or Workbench. See the [Python examples](../../examples/README.md#python) and [binding design](../design/python_binding.md) for the API and UDF contract.

## Run vqld

The daemon serves Arrow Flight SQL on `127.0.0.1:6031`, and health and aggregate metrics on `127.0.0.1:6032`:

```bash
export VQL_HOME="$PWD/data/.vql"
cargo run -p vql-server --bin vqld --locked
```

Keep it running. From another terminal at the repository root:

```bash
export VQL_HOME="$PWD/data/.vql"
curl http://127.0.0.1:6032/health/live
curl http://127.0.0.1:6032/health/ready
cargo run -q -p vql-cli -- shell --endpoint http://127.0.0.1:6031
```

Without `--endpoint`, the shell embeds the engine; with it, the shell executes against the daemon's Catalog through Flight SQL. A remote shell's local `VQL_HOME` controls local history and does not select the server's Catalog.

Loopback development accepts an empty credential and maps it to the daemon's configured principal, `service` by default. Idle logical Sessions expire after 15 minutes by default. Remote SQL and Workbench share the daemon's Catalog and persistent Jobs.

### Secure Flight SQL access

A non-loopback Flight listener requires a service credential and both a TLS certificate and private key. With certificate files supplied by your deployment:

```bash
export VQLD_SERVICE_TOKEN='<choose-a-service-credential>'
cargo run -p vql-server --bin vqld --locked -- \
  --flight-addr 0.0.0.0:6031 \
  --tls-cert ./certs/server.pem \
  --tls-key ./certs/server-key.pem
```

The remote shell reads `VQLD_SERVICE_TOKEN` when `--token` is absent. Set the same credential in its environment, then connect to a hostname covered by the server certificate:

```bash
cargo run -q -p vql-cli -- shell \
  --endpoint https://<server-hostname>:6031 \
  --tls-ca ./certs/ca.pem
```

`--token` takes precedence over the environment fallback. Use the fallback when the credential must stay out of process arguments. `--tls-ca` adds a PEM CA to platform trust roots and requires an HTTPS endpoint. The daemon maps the credential to its configured principal; the client does not select an identity through a username.

The HTTP health and metrics listener remains loopback-only because it has no TLS termination. Run `cargo run -p vql-server --bin vqld --locked -- --help` for listener, Session, attach, thumbnail, result-size, and Job-history limits. See the [service design](../design/vqld.md) for the Flight profile and lifecycle.

## Docker

Build the image containing `vqld` and `vql`:

```bash
docker build -t visionql .
docker run --rm visionql vql --version
docker run --rm -it \
  --mount source=visionql-home,target=/var/lib/visionql \
  visionql vql shell
```

The image runs as an unprivileged user, stores local state under `/var/lib/visionql`, and starts `vqld` by default. Mount media and model directories separately when container SQL needs them, and use their container paths in `LOCATION` and `FROM`. The image does not include the Python package or Workbench.

The daemon keeps its secure listener defaults inside the container. To access Flight SQL from the host, provide a certificate and key, configure the credential, and bind Flight explicitly to a non-loopback container address. For example, with `certs/server.pem` and `certs/server-key.pem` readable by container UID 10001 and `VQLD_SERVICE_TOKEN` set in your shell:

```bash
docker run --rm --name visionql \
  -p 127.0.0.1:6031:6031 \
  --mount source=visionql-home,target=/var/lib/visionql \
  --mount type=bind,src="$PWD/certs",dst=/certs,readonly \
  -e VQLD_SERVICE_TOKEN \
  -e VQLD_FLIGHT_ADDR=0.0.0.0:6031 \
  -e VQLD_TLS_CERT=/certs/server.pem \
  -e VQLD_TLS_KEY=/certs/server-key.pem \
  visionql
```

Connect with HTTPS using a certificate-matching hostname and the appropriate CA. The HTTP endpoint stays loopback-only inside the container and is used by the image health check.

## Workbench

Start `vqld` as above, then build and launch Workbench from another terminal at the repository root:

```bash
pnpm --dir vql-workbench/frontend install --frozen-lockfile
pnpm --dir vql-workbench/frontend build
cargo run --manifest-path vql-workbench/Cargo.toml --bin vql-workbench --locked -- \
  --static-dir vql-workbench/frontend/dist
```

Open [http://127.0.0.1:6040](http://127.0.0.1:6040), then follow the [Workbench user guide](workbench.md#connect-to-a-daemon) to connect and run SQL. It covers connection settings, drafts, visual inspection, Catalog, Jobs, and history.

## Runtime configuration

`VQL_HOME` defaults to `$HOME/.vql`. The optional `$VQL_HOME/config.toml` is loaded by embedded CLI and Python hosts and by `vqld`. Missing settings use these defaults:

```toml
version = 1

[log]
level = "info"

[catalog]
backend = "sqlite"

[catalog.sqlite]
path = "catalog/vql.db"

[kernel.session]
memory_limit = "512 MiB"
```

Relative configuration paths resolve from `VQL_HOME`. Configuration is strict: unsupported versions, backends, fields, log levels, or memory units stop startup.

| State or setting | Default and behavior |
|---|---|
| Configuration | `$VQL_HOME/config.toml`; optional, schema `version = 1` |
| Catalog | `$VQL_HOME/catalog/vql.db`; SQLite stores definitions and persistent Jobs |
| Shell history | `$VQL_HOME/history` |
| Model cache | `$VQL_HOME/cache/models/`; local artifacts remain at their source paths |
| Built-in AI artifacts | `$VQL_HOME/models/`; installed separately from Catalog Model resolution |
| Session memory | 512 MiB shared by queries and retained results in one Session |
| `HF_TOKEN` | Authenticates private `hf://` resolution; never persisted |
| `VQL_LOG_LEVEL` | Overrides `log.level`; accepts `error`, `warn`, `info`, `debug`, or `trace` |

An explicit Rust or Python Catalog override changes only the SQLite path. Configuration, history, and model cache remain under the same `VQL_HOME`. A new statement takes an immutable definition snapshot, so later DDL cannot change its running plan. `vqld` uses this same Catalog database and creates no separate service database.

Service listeners and lifecycle limits are daemon flags or `VQLD_*` variables, rather than fields in `config.toml`. Workbench connection settings select one remote daemon and do not create a separate durable Catalog.

## Troubleshooting

- **Native build cannot find FFmpeg or Clang:** install the development libraries and ensure `pkg-config` can discover them. The default build links FFmpeg 8; an `ffmpeg` executable alone is insufficient for that build.
- **Built-in AI artifact is missing:** run the corresponding `--install` command with the same `VQL_HOME` as the query host. For `vqld`, install into the daemon's instance, rather than a client's local instance.
- **A Table or Model appears missing:** check the selected `VQL_HOME`, explicit Catalog override, or remote endpoint. The remote daemon owns the Catalog used by Flight SQL and Workbench.
- **Flight connection fails:** confirm the daemon is listening, the URL scheme matches its TLS configuration, and the credential and certificate trust are correct. Non-loopback startup requires all three security inputs.
- **SQL reports a hard failure:** use the stable code and symbol rather than matching message text. Python exposes them on `visionql.VisionQLError`; see the [error registry](../design/error_codes.md).

Development checks, system tests, coverage, and Git hooks are documented in [CONTRIBUTING.md](../../CONTRIBUTING.md).
