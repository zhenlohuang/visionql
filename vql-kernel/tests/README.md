# vql-kernel SQL tests

Public SQL behavior is tested with paired files under `tests/sql`:

```text
scenario.sql
scenario.expected.json
```

The runner discovers cases recursively, creates an isolated `VQL_HOME` and deterministic test
data for every case, executes statements in order through `Session::sql`, and compares the
canonical JSON result. A case stops after its first error, matching script execution semantics.

Use `${TEST_DATA}` and `${VQL_HOME}` in SQL and expected JSON instead of machine-specific paths.
Multi-row queries must use an explicit `ORDER BY` when row order is part of the contract.

Run all cases:

```bash
cargo test -p vql-kernel --test sql_cases
```

Select cases by path substring:

```bash
VQL_SQL_CASE=models/function cargo test -p vql-kernel --test sql_cases
```

Accept intentional result changes:

```bash
VQL_UPDATE_GOLDEN=1 cargo test -p vql-kernel --test sql_cases
```

The update flag is intentionally explicit. Review every changed expected JSON file before
committing it.
