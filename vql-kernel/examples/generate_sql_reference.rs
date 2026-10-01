//! Export the registered VQL functions' DataFusion documentation metadata.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write;
use std::fs;
use std::path::Path;

use vql_kernel::documentation::{FunctionDocumentation, builtin_functions};

const GENERATE: &str = "cargo run -p vql-kernel --example generate_sql_reference --locked";

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let check = match arguments.as_slice() {
        [] => false,
        [argument] if argument == "--check" => true,
        [argument] if argument == "--help" => {
            println!("{GENERATE} [-- --check]");
            return Ok(());
        }
        _ => return Err("expected no arguments or --check".into()),
    };
    let generated = render(builtin_functions()?);
    let output = Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/user_guide/sql-functions.md");
    if check {
        let current = match fs::read_to_string(&output) {
            Ok(current) => current,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        if current != generated {
            return Err(format!("{} is stale; run `{GENERATE}`", output.display()).into());
        }
        println!("SQL function reference is current: {}", output.display());
    } else {
        fs::create_dir_all(output.parent().expect("reference has a parent directory"))?;
        fs::write(&output, generated)?;
        println!("Generated {}", output.display());
    }
    Ok(())
}

fn render(functions: Vec<FunctionDocumentation>) -> String {
    let mut sections: BTreeMap<&str, Vec<&FunctionDocumentation>> = BTreeMap::new();
    for function in &functions {
        sections
            .entry(function.documentation.doc_section.label)
            .or_default()
            .push(function);
    }
    let mut output = String::from(
        "<!-- Generated from DataFusion #[user_doc] metadata. Run cargo run -p vql-kernel --example generate_sql_reference --locked. -->\n\n\
         # VQL built-in function reference\n\n\
         This reference covers the built-in functions registered by the current source checkout. \
         See the [SQL reference](sql-reference.md) for types, Tables, Models, Functions, streaming queries, and Jobs, \
         and [sample setup](installation.md#sample-data-and-models) for data and artifacts. \
         Catalog Models and user-defined Functions are described by their declarations.\n\n",
    );
    for (section, functions) in &sections {
        writeln!(output, "- [{section}](#{})", section_anchor(section)).unwrap();
        for function in functions {
            writeln!(
                output,
                "  - [{}](#{})",
                function.name.to_ascii_uppercase(),
                function.name
            )
            .unwrap();
        }
    }
    for (section, functions) in sections {
        writeln!(output, "\n## {section}").unwrap();
        for function in functions {
            let doc = &function.documentation;
            writeln!(
                output,
                "\n### {}\n\n{}\n\n```text\n{}\n```",
                function.name.to_ascii_uppercase(),
                doc.description,
                doc.syntax_example
            )
            .unwrap();
            if let Some(syntaxes) = &doc.alternative_syntax {
                output.push_str("\nAlternative syntax:\n");
                for syntax in syntaxes {
                    writeln!(output, "\n```text\n{syntax}\n```").unwrap();
                }
            }
            if let Some(arguments) = &doc.arguments {
                output.push_str("\nArguments:\n\n");
                for (name, description) in arguments {
                    writeln!(output, "- `{name}`: {description}").unwrap();
                }
            }
            if let Some(example) = &doc.sql_example {
                writeln!(output, "\n{example}").unwrap();
            }
            if let Some(related) = &doc.related_udfs {
                let links = related
                    .iter()
                    .map(|name| format!("[{}](#{name})", name.to_ascii_uppercase()))
                    .collect::<Vec<_>>();
                writeln!(output, "\nRelated functions: {}.", links.join(", ")).unwrap();
            }
        }
    }
    output
}

fn section_anchor(section: &str) -> String {
    section.to_ascii_lowercase().replace(' ', "-")
}
