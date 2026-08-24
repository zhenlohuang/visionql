use std::path::Path;

#[path = "slt/harness.rs"]
mod harness;

fn main() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_root
        .parent()
        .expect("vql-testing has a workspace parent");
    harness::run(crate_root.join("tests/slt"), workspace);
}
