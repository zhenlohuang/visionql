# VisionQL CLI Design

> This document defines the standalone `vql` host. SQL and execution semantics belong to the [Kernel Design](./kernel.md); the shared instance root and Catalog path are defined by the [Catalog Design](./catalog.md#catalog-location).

## Boundary

`vql-cli` owns clap command parsing, terminal input, SQL-script loading, result rendering, history, and process signals. It loads `EngineConfig`, constructs one embedded `Engine` and `Session`, and uses only public kernel APIs.

It does not own SQL syntax, planning, Catalog semantics, memory policy, model loading, or connector behavior. It does not embed Python, so a Python UDF encountered by the CLI fails with guidance to use the Python host.

## Command Surface

The executable exposes exactly two subcommands:

```text
vql shell
vql run <script.sql>
```

SQL `EXPLAIN` is a SQL statement executed through either host path. It is not a CLI subcommand.

Engine settings are not CLI flags. `VQL_HOME` selects the instance and its strict `config.toml`; `VQL_LOG_LEVEL` may override the configured log level. In repository development, `cargo run -p vql-cli -- shell` and `cargo run -p vql-cli -- run <script.sql>` replace the installed executable.

## Interactive Shell

The shell supports multiline statements and executes input only when the buffered text ends with a complete statement terminator. Complete scripts are split by the kernel's SQL-aware splitter so semicolons inside strings, comments, and quoted identifiers do not split statements.

On start the shell prints a banner naming the package version and the exit keys. Interactive terminals use Reedline and store at most 1,000 entries at `$VQL_HOME/history`. Non-terminal stdin and `TERM=dumb` use the basic line reader. Both loops share these contracts:

- `\q` exits only when it is the trimmed standalone line, without `;`;
- Ctrl-D exits;
- a trailing incomplete statement in stdin mode returns `INVALID_SQL`;
- bounded statements collect and render once;
- unbounded statements render each `RecordBatch` as it arrives.

At an interactive prompt, Ctrl-C clears pending input. While a query is active, signal handling follows the shared cancellation lifecycle: the first Ctrl-C requests a graceful stop; a second Ctrl-C requests immediate cancellation.

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
- absence of `explain`, metrics, Catalog-path, and memory-limit CLI options;
- standalone `\q` behavior in both terminal loops;
- rejection of an unbounded statement before later script SQL;
- rendering and signal behavior that can be exercised without external services.

Cross-host SQL behavior belongs to the [Testing Design](./testing.md).
