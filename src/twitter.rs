use regex::Regex;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use url::form_urlencoded;

use crate::{load_db_jsonl, utils, PipelineConfig, TwitterDaemonArgs};

fn encode_query(query: &str) -> String {
    form_urlencoded::byte_serialize(query.as_bytes()).collect()
}

fn extract_arxiv_ids(text: &str, re: &Regex) -> Vec<String> {
    re.captures_iter(text)
        .filter_map(|cap| cap.get(2).map(|m| {
            let mut id = m.as_str().to_string();
            if let Some(stripped) = id.strip_suffix(".pdf") {
                id = stripped.to_string();
            }
            id
        }))
        .collect()
}

fn load_known_ids(config: &PipelineConfig) -> Result<HashSet<String>, String> {
    let db = load_db_jsonl(Path::new(&config.db_path))?;
    Ok(db.keys().cloned().collect())
}

fn append_jsonl(path: &Path, payload: &Value) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| format!("Failed to open {}: {err}", path.display()))?;
    let line = serde_json::to_string(payload)
        .map_err(|err| format!("Failed to serialize tweet: {err}"))?;
    writeln!(file, "{line}")
        .map_err(|err| format!("Failed to append to {}: {err}", path.display()))?;
    Ok(())
}

pub fn run_twitter_daemon(
    args: &TwitterDaemonArgs,
    config: &PipelineConfig,
) -> Result<(), String> {
    let token = std::env::var("TWITTER_BEARER_TOKEN")
        .map_err(|_| "TWITTER_BEARER_TOKEN is required for the Rust twitter daemon".to_string())?;
    let client = Client::builder()
        .user_agent("arxiv-sanity-twitter-daemon")
        .build()
        .map_err(|err| format!("Failed to build HTTP client: {err}"))?;
    let known_ids = load_known_ids(config)?;
    let arxiv_re = Regex::new(r"arxiv\.org/(abs|pdf)/([^\s?#/]+)")
        .map_err(|err| format!("Failed to compile regex: {err}"))?;

    utils::ensure_parent_dir(&args.output)?;

    loop {
        let url = format!(
            "https://api.twitter.com/2/tweets/search/recent?query={}&max_results={}&tweet.fields=created_at,lang",
            encode_query(&args.query),
            args.max_results
        );
        let response = client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .map_err(|err| format!("Failed to query Twitter API: {err}"))?;
        let status = response.status();
        let payload: Value = response
            .json()
            .map_err(|err| format!("Failed to parse Twitter response: {err}"))?;

        if !status.is_success() {
            eprintln!(
                "Twitter API error ({}): {}",
                status,
                payload.to_string()
            );
        } else if let Some(tweets) = payload.get("data").and_then(|data| data.as_array()) {
            let mut stored = 0;
            for tweet in tweets {
                let text = tweet
                    .get("text")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                let mut pids = extract_arxiv_ids(text, &arxiv_re);
                pids.retain(|pid| {
                    let raw = utils::strip_version(pid);
                    known_ids.contains(&raw)
                });
                if pids.is_empty() {
                    continue;
                }

                let entry = json!({
                    "id": tweet.get("id").cloned().unwrap_or(Value::Null),
                    "pids": pids,
                    "created_at": tweet.get("created_at").cloned().unwrap_or(Value::Null),
                    "lang": tweet.get("lang").cloned().unwrap_or(Value::Null),
                    "text": text,
                });
                append_jsonl(&args.output, &entry)?;
                stored += 1;
            }
            println!(
                "processed {} results, stored {} tweets",
                tweets.len(),
                stored
            );
        } else {
            println!("No tweets returned for query {}", args.query);
        }

        println!("sleeping {}", args.sleep_seconds);
        sleep(Duration::from_secs(args.sleep_seconds));
    }
}
