use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde_json::{Map, Value};
use serde_pickle::de::DeOptions;
use serde_pickle::value::{HashableValue as PickleHashableValue, Value as PickleValue};
use std::fs;
use std::io::Write;
use std::path::Path;

use crate::{parse_arxiv_url, utils, MigrateAnalysisArgs, MigrateDbArgs, Paper};

fn read_pickle(path: &Path) -> Result<PickleValue, String> {
    let bytes = fs::read(path).map_err(|err| format!("Failed to read {path:?}: {err}"))?;
    serde_pickle::from_slice(&bytes, DeOptions::default())
        .map_err(|err| format!("Failed to decode pickle {path:?}: {err}"))
}

fn hashable_to_json(value: PickleHashableValue) -> Value {
    match value {
        PickleHashableValue::None => Value::Null,
        PickleHashableValue::Bool(v) => Value::Bool(v),
        PickleHashableValue::I64(v) => Value::Number(v.into()),
        PickleHashableValue::Int(v) => Value::String(v.to_string()),
        PickleHashableValue::F64(v) => serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        PickleHashableValue::Bytes(bytes) => Value::String(BASE64_STANDARD.encode(bytes)),
        PickleHashableValue::String(s) => Value::String(s),
        PickleHashableValue::Tuple(values) => {
            Value::Array(values.into_iter().map(hashable_to_json).collect())
        }
        PickleHashableValue::FrozenSet(values) => {
            Value::Array(values.into_iter().map(hashable_to_json).collect())
        }
    }
}

fn pickle_key_to_string(value: PickleHashableValue) -> String {
    match value {
        PickleHashableValue::String(s) => s,
        PickleHashableValue::I64(v) => v.to_string(),
        PickleHashableValue::Int(v) => v.to_string(),
        PickleHashableValue::F64(v) => v.to_string(),
        PickleHashableValue::Bool(v) => v.to_string(),
        PickleHashableValue::Bytes(bytes) => BASE64_STANDARD.encode(bytes),
        PickleHashableValue::Tuple(values) => {
            let parts: Vec<String> = values.into_iter().map(pickle_key_to_string).collect();
            format!("({})", parts.join(", "))
        }
        PickleHashableValue::FrozenSet(values) => {
            let parts: Vec<String> = values.into_iter().map(pickle_key_to_string).collect();
            format!("frozenset({})", parts.join(", "))
        }
        PickleHashableValue::None => "None".to_string(),
    }
}

fn pickle_to_json(value: PickleValue) -> Value {
    match value {
        PickleValue::None => Value::Null,
        PickleValue::Bool(v) => Value::Bool(v),
        PickleValue::I64(v) => Value::Number(v.into()),
        PickleValue::Int(v) => Value::String(v.to_string()),
        PickleValue::F64(v) => serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        PickleValue::Bytes(bytes) => Value::String(BASE64_STANDARD.encode(bytes)),
        PickleValue::String(s) => Value::String(s),
        PickleValue::List(values) | PickleValue::Tuple(values) => {
            Value::Array(values.into_iter().map(pickle_to_json).collect())
        }
        PickleValue::Set(values) | PickleValue::FrozenSet(values) => Value::Array(
            values
                .into_iter()
                .map(hashable_to_json)
                .collect(),
        ),
        PickleValue::Dict(entries) => {
            let mut map = Map::new();
            for (key, value) in entries {
                map.insert(pickle_key_to_string(key), pickle_to_json(value));
            }
            Value::Object(map)
        }
    }
}

fn write_json_pretty(path: &Path, payload: &Value) -> Result<(), String> {
    let json = serde_json::to_string_pretty(payload)
        .map_err(|err| format!("Failed to serialize JSON {path:?}: {err}"))?;
    utils::ensure_parent_dir(path)?;
    utils::write_atomic(path, |file| {
        writeln!(file, "{json}")
            .map_err(|err| format!("Failed to write JSON {path:?}: {err}"))
    })
}

fn load_pickle_json(path: &Path) -> Result<Value, String> {
    let value = read_pickle(path)?;
    Ok(pickle_to_json(value))
}

pub fn run_migrate_db(args: &MigrateDbArgs) -> Result<(), String> {
    let payload = load_pickle_json(&args.input)?;
    let db_entries = payload
        .as_object()
        .ok_or_else(|| "Expected pickle database to be a dict".to_string())?;
    let mut papers = Vec::new();

    for entry in db_entries.values() {
        let entry = entry
            .as_object()
            .ok_or_else(|| "Expected db entry to be a dict".to_string())?;
        let id_url = entry
            .get("id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| "Missing id field in db entry".to_string())?;
        let (raw_id, version) = parse_arxiv_url(id_url)?;
        let title = entry
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let abstract_text = entry
            .get("summary")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let updated = entry
            .get("updated")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string();
        let authors = entry
            .get("authors")
            .and_then(|value| value.as_array())
            .map(|values| {
                values
                    .iter()
                    .filter_map(|author| {
                        author
                            .as_object()
                            .and_then(|obj| obj.get("name"))
                            .and_then(|value| value.as_str())
                            .map(|name| name.to_string())
                    })
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        let categories = entry
            .get("tags")
            .and_then(|value| value.as_array())
            .map(|values| {
                values
                    .iter()
                    .filter_map(|tag| {
                        tag.as_object()
                            .and_then(|obj| obj.get("term"))
                            .and_then(|value| value.as_str())
                            .map(|term| term.to_string())
                    })
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        papers.push(Paper {
            id: raw_id,
            version,
            title,
            authors,
            abstract_text,
            updated,
            categories,
            citation_count: None,
            is_accepted: None,
            is_published: None,
        });
    }

    utils::ensure_parent_dir(&args.output)?;
    utils::write_atomic(&args.output, |file| {
        for paper in papers.iter() {
            let line = serde_json::to_string(paper)
                .map_err(|err| format!("Failed to serialize paper: {err}"))?;
            writeln!(file, "{line}")
                .map_err(|err| format!("Failed to write JSONL line: {err}"))?;
        }
        Ok(())
    })?;

    println!("Wrote {} papers to {}", papers.len(), args.output.display());
    Ok(())
}

fn maybe_convert(
    label: &str,
    input_path: &Path,
    output_path: &Path,
    allow_missing: bool,
) -> Result<(), String> {
    if !input_path.is_file() {
        if allow_missing {
            println!("Skipping {label}: {} not found", input_path.display());
            return Ok(());
        }
        return Err(format!("Missing {label} pickle: {}", input_path.display()));
    }
    println!(
        "Converting {label}: {} -> {}",
        input_path.display(),
        output_path.display()
    );
    let payload = load_pickle_json(input_path)?;
    write_json_pretty(output_path, &payload)?;
    Ok(())
}

pub fn run_migrate_analysis(args: &MigrateAnalysisArgs) -> Result<(), String> {
    maybe_convert(
        "tfidf_meta",
        &args.tfidf_meta_in,
        &args.tfidf_meta_out,
        args.allow_missing,
    )?;
    maybe_convert(
        "sim_dict",
        &args.sim_in,
        &args.sim_out,
        args.allow_missing,
    )?;
    maybe_convert(
        "user_sim",
        &args.user_sim_in,
        &args.user_sim_out,
        args.allow_missing,
    )?;
    Ok(())
}
