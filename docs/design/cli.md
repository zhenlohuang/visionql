# VisionQL CLI Design

> This document defines the standalone `vql` host. System boundaries come from the [High-Level Design](../high_level_design.md); SQL and execution semantics belong to the [Kernel Design](./kernel.md), while the shared instance root and Catalog path are defined by the [Catalog Design](./catalog.md#catalog-location).

## Boundary

`vql-cli` owns clap command parsing, terminal input, SQL-script loading, result rendering, history, and process signals. The shell is one front end over two execution backends: `EmbeddedBackend` constructs a local `Engine` and `Session`, while `FlightBackend` connects to `vqld` through its public Arrow Flight SQL profile. `vql run` remains embedded. Neither backend changes SQL semantics.

It does not own SQL syntax, planning, Catalog semantics, memory policy, model loading, or connector behavior. It does not embed Python, so a Python UDF encountered by the CLI fails with guidance to use the Python host.

## Command Surface

The executable exposes exactly two subcommands:

```text
vql shell [--endpoint URI] [--token TOKEN] [--tls-ca PEM]
vql run <script.sql>
```

SQL `EXPLAIN` is a SQL statement executed through either host path. It is not a CLI subcommand.

Without `--endpoint`, `vql shell` uses `EmbeddedBackend` and the local instance selected by `VQL_HOME`. With an `http://` or `https://` endpoint, it uses `FlightBackend`, performs one Flight SQL handshake, and preserves that logical service Session across statements. Loopback development needs no client configuration. A protected endpoint accepts its single credential through `--token`; when that flag is absent, the shell falls back to `VQLD_SERVICE_TOKEN`. The flag takes precedence. The server maps the credential to its configured Catalog principal, so the shell has no principal setting. `--tls-ca` adds a PEM CA certificate to the platform trust roots and is valid only with an `https://` endpoint. The endpoint URI never implies access to a local Catalog. Because command arguments may be visible to other local processes, use the environment fallback when that exposure matters.

Engine settings are not CLI flags. `VQL_HOME` selects embedded engine configuration and the shell history location; `VQL_LOG_LEVEL` may override the configured log level. In repository development, `cargo run -p vql-cli -- shell`, `cargo run -p vql-cli -- shell --endpoint http://127.0.0.1:6031`, and `cargo run -p vql-cli -- run <script.sql>` replace the installed executable.

## Interactive Shell

The shell supports multiline statements and executes input only when the buffered text ends with a complete statement terminator. Complete scripts are split by the kernel's SQL-aware splitter so semicolons inside strings, comments, and quoted identifiers do not split statements.

On start the shell prints a banner naming the package version and the exit keys. Interactive terminals use Reedline and store at most 1,000 entries at `$VQL_HOME/history`. Non-terminal stdin and `TERM=dumb` use the basic line reader. Both loops share these contracts:

- `\q` exits only when it is the trimmed standalone line, without `;`;
- Ctrl-D exits;
- a trailing incomplete statement in stdin mode returns `INVALID_SQL`;
- bounded statements collect and render once;
- unbounded statements render each `RecordBatch` as it arrives;
- remote update statements use Flight SQL `ExecuteUpdate` and print the affected-row count.

At an interactive prompt, Ctrl-C clears pending input. While an embedded unbounded query is active, the first Ctrl-C requests a graceful stop and a second requests immediate cancellation. `EmbeddedBackend` forwards that lifecycle to the kernel Session. `FlightBackend` instead sends `CancelFlightInfo` for the active server execution ID; the public Flight profile provides exact cancellation rather than the embedded graceful-drain extension. Dropping or completing the result stream clears the shell's active remote execution.

A statement error is printed and the interactive shell continues with the next statement. Script-splitting, terminal input, and history failures terminate the host with a structured kernel error.

## Script Runner

`vql run` reads the complete UTF-8 script, splits it with the kernel parser, and executes statements in order through one Session.

An unbounded statement must be last. It remains attached and renders batches until graceful stop, immediate cancellation, or failure. Rejecting later statements prevents a stopped continuous query from unexpectedly starting additional SQL.

Any read, parse, planning, or execution error terminates the command with a non-zero process exit.

## Rendering

Bounded and continuous paths share one Arrow `RecordBatch` renderer, and an empty result prints `(no rows)`. Process-local `IMAGE` buffer identifiers never appear in terminal output: an `IMAGE` column is replaced by a text summary of the form `<image uri=URI WIDTHxHEIGHT>`, with `?` standing in for missing parts. Every other column keeps its Arrow field and value. The CLI does not decode thumbnails or introduce an alternative result schema.

Error output uses the stable `[VQL-CCDDD] SYMBOL: message` kernel format. Shell behavior must not depend on matching human-readable error strings; the complete registry is defined by the [Error Code Design](./error_codes.md).

## Verification

CLI-owned tests protect:

- the exact `shell` and `run` command surface;
- selection of embedded execution by default and Flight execution only with `--endpoint`;
- Flight prepare metadata, query/update routing, logical Session reuse, and structured VQL error decoding;
- absence of `explain`, metrics, Catalog-path, and memory-limit CLI options;
- standalone `\q` behavior in both terminal loops;
- rejection of an unbounded statement before later script SQL;
- rendering and signal behavior that can be exercised without external services.

Cross-host SQL behavior belongs to the [Testing Design](./testing.md).
