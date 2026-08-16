use std::fs;
use std::path::{Path, PathBuf};

use libtest_mimic::{Arguments, Completion, Failed, Trial};
use vql_testing::{FILTER_ENV, FixturePaths, REQUIRE_ENV, VqlColumnType, run_slt_file};

fn main() {
    let mut arguments = Arguments::from_args();
    let environment_filter = arguments
        .filter
        .is_none()
        .then(|| std::env::var(FILTER_ENV).ok())
        .flatten();
    if arguments.test_threads.is_none() {
        arguments.test_threads = Some(1);
    }

    let workspace = workspace_root();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cases");
    let mut cases = discover(&root).expect("discover sqllogictest files");
    cases.sort();
    if let Some(filter) = &environment_filter {
        cases.retain(|path| case_name(&root, path).contains(filter));
    }

    let mut trials = Vec::new();
    if cases.is_empty() {
        let message = environment_filter.map_or_else(
            || format!("no sqllogictest cases under {}", root.display()),
            |filter| {
                format!(
                    "no sqllogictest cases matched {FILTER_ENV}={filter:?} under {}",
                    root.display()
                )
            },
        );
        trials.push(Trial::test("case_layout", move || {
            Err(message.clone().into())
        }));
    }

    let fixtures = FixturePaths::from_workspace(&workspace);
    let require_fixtures = std::env::var_os(REQUIRE_ENV).is_some();
    trials.extend(cases.into_iter().map(|path| {
        let name = case_name(&root, &path);
        let source = fs::read_to_string(&path);
        let parse_result = source.as_ref().map_err(ToString::to_string).and_then(|_| {
            sqllogictest::parse_file::<VqlColumnType>(&path)
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
        let missing = source
            .as_ref()
            .map(|source| fixtures.missing_for(source))
            .unwrap_or_default();
        let fixtures = fixtures.clone();
        Trial::ignorable_test(name, move || {
            if let Err(error) = &parse_result {
                return Err(Failed::from(format!("{}: {error}", path.display())));
            }
            if !missing.is_empty() {
                let message = format!("missing integration fixtures:\n  {}", missing.join("\n  "));
                return if require_fixtures {
                    Err(Failed::from(message))
                } else {
                    Ok(Completion::ignored_with(message))
                };
            }
            run_slt_file(&path, &fixtures)
                .map(|()| Completion::Completed)
                .map_err(Failed::from)
        })
    }));

    libtest_mimic::run(&arguments, trials).exit();
}

fn discover(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("slt") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn case_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .with_extension("")
        .to_string_lossy()
        .replace('\\', "/")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("vql-testing has a workspace parent")
        .to_path_buf()
}
