# vql-kernel owner integration tests

Keep crate-local integration tests focused on contracts owned entirely by `vql-kernel`. They must
use synthetic inputs or `mock://` models and must not require downloaded datasets, Docker, or
network services.

Run them with:

```bash
cargo test -p vql-kernel --locked
```

`result_schema.rs` protects the exact DDL result field name, Arrow type, and nullability. Similar
tests belong here when a kernel API exposes metadata that the standard sqllogictest format cannot
express.

SQL conformance cases, real fixtures, and external-service scenarios live in
[`vql-testing`](../../vql-testing/README.md).
