use std::process::Command;

use tempfile::tempdir;

#[test]
fn run_executes_a_script_through_the_public_binary() {
    let temp = tempdir().unwrap();
    let script = temp.path().join("query.sql");
    std::fs::write(&script, "SELECT 42 AS answer;").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_vql"))
        .args(["run", script.to_str().unwrap()])
        .env("VQL_HOME", temp.path().join("vql-home"))
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "vql run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("answer"), "missing column header: {stdout}");
    assert!(stdout.contains("42"), "missing query result: {stdout}");
}
