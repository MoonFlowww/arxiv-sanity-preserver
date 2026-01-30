use chrono::{DateTime, NaiveDateTime, Utc};
use once_cell::sync::Lazy;
use reqwest::blocking::Client;
use reqwest::Url;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_pickle::ser::SerOptions;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::utils;

const DEFAULT_CONFIG_PATH: &str = "pipeline_config.json";
const DEFAULT_OUTPUT_DIR: &str = ".pipeline";
const DEFAULT_DB_PICKLE_PATH: &str = ".pipeline/db.p";
const DEFAULT_DB_JSONL_PATH: &str = ".pipeline/db.jsonl";
const DEFAULT_TFIDF_META_PICKLE_PATH: &str = ".pipeline/tfidf_meta.p";
const DEFAULT_TFIDF_META_JSON_PATH: &str = ".pipeline/tfidf_meta.json";
const DEFAULT_DATABASE_PATH: &str = ".pipeline/as.db";
const DEFAULT_SERVE_CACHE_PATH: &str = ".pipeline/serve_cache.p";
const DEFAULT_DB_SERVE_PATH: &str = ".pipeline/db2.p";
const OPENALEX_WORKS_ENDPOINT: &str = "https://api.openalex.org/works";
const SECONDS_PER_YEAR: f64 = 365.25 * 24.0 * 60.0 * 60.0;

static PUNCTUATION: Lazy<HashSet<char>> = Lazy::new(|| {
    let chars = "'!\"#$%&'()*+,./:;<=>?@[\\]^_`{|}~";
    chars.chars().collect()
});

#[derive(Debug, Deserialize)]
struct CacheConfigFile {
    #[serde(default)]
    output_dir: Option<String>,
    #[serde(default)]
    db_path: Option<String>,
    #[serde(default)]
    tfidf_meta_path: Option<String>,
    #[serde(default)]
    database_path: Option<String>,
}

#[derive(Debug, Clone)]
struct CacheConfig {
    db_pickle_path: PathBuf,
    db_jsonl_path: PathBuf,
    tfidf_meta_pickle_path: PathBuf,
    tfidf_meta_json_path: PathBuf,
    database_path: PathBuf,
    serve_cache_path: PathBuf,
    db_serve_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct Author {
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct Tag {
    term: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct PaperRecord {
    #[serde(rename = "_rawid")]
    raw_id: String,
    #[serde(rename = "_version")]
    version: i64,
    #[serde(default)]
    title: String,
    #[serde(default)]
    authors: Vec<Author>,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    updated: String,
    #[serde(default)]
    published: String,
    #[serde(default)]
    link: String,
    #[serde(default)]
    tags: Vec<Tag>,
    #[serde(default)]
    arxiv_primary_category: HashMap<String, String>,
    #[serde(default)]
    arxiv_comment: String,
    #[serde(default)]
    repo_links: Vec<String>,
    #[serde(default)]
    is_opensource: bool,
    #[serde(default)]
    time_updated: Option<i64>,
    #[serde(default)]
    time_published: Option<i64>,
    #[serde(default)]
    citation_count: Option<i64>,
    #[serde(default)]
    is_accepted: Option<bool>,
    #[serde(default)]
    is_published: Option<bool>,
    #[serde(default)]
    impact_score: Option<f64>,
    #[serde(default)]
    years_since_pub: Option<f64>,
    #[serde(default)]
    tscore: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ServeCache {
    date_sorted_pids: Vec<String>,
    library_sorted_pids: Vec<String>,
    top_sorted_pids: Vec<String>,
    search_dict: HashMap<String, HashMap<String, f64>>,
}

#[derive(Debug, Deserialize)]
struct TfidfMeta {
    vocab: HashMap<String, usize>,
    idf: Vec<f64>,
}

#[derive(Debug, Deserialize)]
struct TfidfMetaF32 {
    vocab: HashMap<String, usize>,
    idf: Vec<f32>,
}

pub fn run_make_cache(config_path: &Path) -> Result<(), String> {
    let config = CacheConfig::load(config_path)?;
    let mut db = load_paper_db(&config)?;
    let meta = load_tfidf_meta(&config)?;

    println!("decorating the database with additional information...");
    for (_pid, paper) in db.iter_mut() {
        normalize_paper(_pid, paper);
        ensure_time_metadata(paper)?;
    }

    println!("fetching OpenAlex metadata (if missing)...");
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("Failed to build HTTP client: {err}"))?;
    let mut updated_openalex = 0;
    for (pid, paper) in db.iter_mut() {
        if paper.citation_count.is_some()
            && paper.is_accepted.is_some()
            && paper.is_published.is_some()
        {
            continue;
        }
        println!("Fetching OpenAlex metadata for {}: {}", pid, paper.title);
        let metadata = fetch_openalex_metadata(&client, &paper.title);
        if metadata.citation_count.is_some()
            || metadata.is_accepted.is_some()
            || metadata.is_published.is_some()
        {
            updated_openalex += 1;
        }
        paper.citation_count = paper.citation_count.or(metadata.citation_count);
        paper.is_accepted = paper.is_accepted.or(metadata.is_accepted);
        paper.is_published = paper.is_published.or(metadata.is_published);
        sleep(Duration::from_millis(100));
    }
    println!("Updated OpenAlex metadata for {} papers.", updated_openalex);

    println!("computing OpenAlex-inspired recency-aware scores...");
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("Failed to get current time: {err}"))?
        .as_secs_f64();
    for paper in db.values_mut() {
        compute_impact_score(paper, now_ts)?;
    }

    println!("computing min/max time for all papers...");
    let mut times = Vec::with_capacity(db.len());
    for paper in db.values() {
        let ts = parse_timestamp(&paper.updated)?;
        times.push(ts as f64);
    }
    let (ttmin, ttmax) = match (times.iter().cloned().reduce(f64::min), times.iter().cloned().reduce(f64::max)) {
        (Some(min), Some(max)) => (min, max),
        _ => (0.0, 0.0),
    };
    for paper in db.values_mut() {
        let tt = parse_timestamp(&paper.updated)? as f64;
        let tscore = if (ttmax - ttmin).abs() < f64::EPSILON {
            0.0
        } else {
            (tt - ttmin) / (ttmax - ttmin)
        };
        paper.tscore = Some(tscore);
    }

    println!("precomputing papers date sorted...");
    let mut date_scores: Vec<(i64, String)> = db
        .iter()
        .map(|(pid, paper)| (paper.time_updated.unwrap_or(0), pid.clone()))
        .collect();
    date_scores.sort_by(|a, b| b.0.cmp(&a.0));
    let date_sorted_pids = date_scores.into_iter().map(|entry| entry.1).collect();

    println!("computing top papers...");
    let library_sorted_pids = compute_library_sorted_pids(&config.database_path)?;

    println!("computing citation-based popularity...");
    let mut citation_scores: Vec<(f64, String)> = db
        .iter()
        .filter_map(|(pid, paper)| paper.impact_score.map(|score| (score, pid.clone())))
        .collect();
    citation_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let top_sorted_pids = citation_scores.into_iter().map(|entry| entry.1).collect();

    println!("building an index for faster search...");
    let search_dict = build_search_dict(&db, &meta.vocab, &meta.idf);

    let cache = ServeCache {
        date_sorted_pids,
        library_sorted_pids,
        top_sorted_pids,
        search_dict,
    };

    println!("writing {}", config.serve_cache_path.display());
    write_pickle(&config.serve_cache_path, &cache)?;
    println!("writing {}", config.db_serve_path.display());
    write_pickle(&config.db_serve_path, &db)?;
    Ok(())
}

impl CacheConfig {
    fn load(path: &Path) -> Result<Self, String> {
        let config_file = if path.exists() {
            let contents = fs::read_to_string(path)
                .map_err(|err| format!("Failed to read config {path:?}: {err}"))?;
            serde_json::from_str::<CacheConfigFile>(&contents)
                .map_err(|err| format!("Failed to parse config {path:?}: {err}"))?
        } else {
            CacheConfigFile {
                output_dir: None,
                db_path: None,
                tfidf_meta_path: None,
                database_path: None,
            }
        };
        let output_dir = config_file
            .output_dir
            .unwrap_or_else(|| DEFAULT_OUTPUT_DIR.to_string());
        let db_jsonl_path = config_file.db_path.unwrap_or_else(|| {
            Path::new(&output_dir)
                .join("db.jsonl")
                .to_string_lossy()
                .to_string()
        });
        let tfidf_meta_json_path = config_file.tfidf_meta_path.unwrap_or_else(|| {
            Path::new(&output_dir)
                .join("tfidf_meta.json")
                .to_string_lossy()
                .to_string()
        });
        let database_path = config_file.database_path.unwrap_or_else(|| {
            Path::new(&output_dir)
                .join("as.db")
                .to_string_lossy()
                .to_string()
        });
        Ok(Self {
            db_pickle_path: PathBuf::from(DEFAULT_DB_PICKLE_PATH),
            db_jsonl_path: PathBuf::from(db_jsonl_path),
            tfidf_meta_pickle_path: PathBuf::from(DEFAULT_TFIDF_META_PICKLE_PATH),
            tfidf_meta_json_path: PathBuf::from(tfidf_meta_json_path),
            database_path: PathBuf::from(database_path),
            serve_cache_path: PathBuf::from(DEFAULT_SERVE_CACHE_PATH),
            db_serve_path: PathBuf::from(DEFAULT_DB_SERVE_PATH),
        })
    }
}

fn load_paper_db(config: &CacheConfig) -> Result<HashMap<String, PaperRecord>, String> {
    if config.db_pickle_path.exists() {
        println!("loading the paper database {}", config.db_pickle_path.display());
        let mut db: HashMap<String, PaperRecord> = load_pickle(&config.db_pickle_path)?;
        for (pid, paper) in db.iter_mut() {
            normalize_paper(pid, paper);
        }
        return Ok(db);
    }
    if config.db_jsonl_path.exists() {
        println!("loading the paper database {}", config.db_jsonl_path.display());
        return load_db_jsonl(&config.db_jsonl_path);
    }
    Err("No paper database found. Expected db.p or db.jsonl.".to_string())
}

fn load_tfidf_meta(config: &CacheConfig) -> Result<TfidfMeta, String> {
    if config.tfidf_meta_pickle_path.exists() {
        println!("loading tfidf_meta {}", config.tfidf_meta_pickle_path.display());
        if let Ok(meta) = load_pickle::<TfidfMeta>(&config.tfidf_meta_pickle_path) {
            return Ok(meta);
        }
        let meta = load_pickle::<TfidfMetaF32>(&config.tfidf_meta_pickle_path)?;
        return Ok(TfidfMeta {
            vocab: meta.vocab,
            idf: meta.idf.into_iter().map(|val| val as f64).collect(),
        });
    }
    if config.tfidf_meta_json_path.exists() {
        println!("loading tfidf_meta {}", config.tfidf_meta_json_path.display());
        let contents = fs::read_to_string(&config.tfidf_meta_json_path).map_err(|err| {
            format!(
                "Failed to read tfidf_meta json {}: {err}",
                config.tfidf_meta_json_path.display()
            )
        })?;
        let meta = serde_json::from_str::<TfidfMeta>(&contents)
            .map_err(|err| format!("Failed to parse tfidf_meta json: {err}"))?;
        return Ok(meta);
    }
    Err("No TF-IDF metadata found. Expected tfidf_meta.p or tfidf_meta.json.".to_string())
}

fn load_db_jsonl(path: &Path) -> Result<HashMap<String, PaperRecord>, String> {
    let file = fs::File::open(path).map_err(|err| format!("Failed to open {path:?}: {err}"))?;
    let reader = BufReader::new(file);
    let mut db = HashMap::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|err| format!("Failed to read {path:?} line {}: {err}", index + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed).map_err(|err| {
            format!(
                "Failed parsing {} at line {}: {err}",
                path.display(),
                index + 1
            )
        })?;
        let raw_id = value
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("Missing id in {} at line {}", path.display(), index + 1))?;
        let mut paper = convert_json_paper(raw_id, &value)?;
        normalize_paper(raw_id, &mut paper);
        db.insert(raw_id.to_string(), paper);
    }
    Ok(db)
}

fn convert_json_paper(raw_id: &str, value: &Value) -> Result<PaperRecord, String> {
    let version = value.get("version").and_then(Value::as_i64).unwrap_or(1);
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let authors = value
        .get("authors")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    if let Some(name) = item.as_str() {
                        Some(Author {
                            name: name.to_string(),
                        })
                    } else {
                        item.get("name").and_then(Value::as_str).map(|name| Author {
                            name: name.to_string(),
                        })
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let summary = value
        .get("abstract")
        .or_else(|| value.get("abstract_text"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let updated = value
        .get("updated")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let published = value
        .get("published")
        .and_then(Value::as_str)
        .unwrap_or(&updated)
        .to_string();
    let categories: Vec<String> = value
        .get("categories")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|val| val.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let link = value
        .get("link")
        .and_then(Value::as_str)
        .map(|val| val.to_string())
        .unwrap_or_else(|| build_link(raw_id, version));
    let tags = categories
        .iter()
        .map(|term| Tag { term: term.clone() })
        .collect::<Vec<_>>();
    let mut arxiv_primary_category = HashMap::new();
    if let Some(term) = categories.first() {
        arxiv_primary_category.insert("term".to_string(), term.clone());
    }
    let arxiv_comment = value
        .get("arxiv_comment")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let repo_links = value
        .get("repo_links")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|val| val.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let is_opensource = value
        .get("is_opensource")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let citation_count = value.get("citation_count").and_then(Value::as_i64);
    let is_accepted = value.get("is_accepted").and_then(Value::as_bool);
    let is_published = value.get("is_published").and_then(Value::as_bool);

    Ok(PaperRecord {
        raw_id: raw_id.to_string(),
        version,
        title,
        authors,
        summary,
        updated,
        published,
        link,
        tags,
        arxiv_primary_category,
        arxiv_comment,
        repo_links,
        is_opensource,
        citation_count,
        is_accepted,
        is_published,
        ..PaperRecord::default()
    })
}

fn normalize_paper(pid: &str, paper: &mut PaperRecord) {
    if paper.raw_id.is_empty() {
        paper.raw_id = pid.to_string();
    }
    if paper.version == 0 {
        paper.version = 1;
    }
    if paper.published.is_empty() {
        paper.published = paper.updated.clone();
    }
    if paper.link.is_empty() {
        paper.link = build_link(pid, paper.version);
    }
    if paper.arxiv_primary_category.is_empty() && !paper.tags.is_empty() {
        paper
            .arxiv_primary_category
            .insert("term".to_string(), paper.tags[0].term.clone());
    }
}

fn build_link(raw_id: &str, version: i64) -> String {
    format!("https://arxiv.org/abs/{raw_id}v{version}")
}

fn ensure_time_metadata(paper: &mut PaperRecord) -> Result<(), String> {
    if paper.time_updated.is_none() {
        paper.time_updated = Some(parse_timestamp(&paper.updated)?);
    }
    if paper.time_published.is_none() {
        paper.time_published = Some(parse_timestamp(&paper.published)?);
    }
    Ok(())
}

fn compute_impact_score(paper: &mut PaperRecord, now_ts: f64) -> Result<(), String> {
    ensure_time_metadata(paper)?;
    let years_since_pub = ((now_ts - paper.time_published.unwrap_or(0) as f64) / SECONDS_PER_YEAR)
        .max(0.0);
    paper.years_since_pub = Some(years_since_pub);
    if let Some(citations) = paper.citation_count {
        let citations = citations as f64;
        paper.impact_score = Some((1.0 + citations).ln() - 0.3 * years_since_pub);
    } else {
        paper.impact_score = None;
    }
    Ok(())
}

fn parse_timestamp(value: &str) -> Result<i64, String> {
    if value.trim().is_empty() {
        return Ok(0);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.timestamp());
    }
    if let Ok(dt) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f%z") {
        return Ok(dt.timestamp());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().timestamp());
    }
    if let Ok(dt) = DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%z") {
        return Ok(dt.timestamp());
    }
    if let Ok(dt) = value.parse::<DateTime<Utc>>() {
        return Ok(dt.timestamp());
    }
    Err(format!("Failed to parse timestamp: {value}"))
}

#[derive(Debug, Default)]
struct OpenAlexMetadata {
    citation_count: Option<i64>,
    is_accepted: Option<bool>,
    is_published: Option<bool>,
}

fn fetch_openalex_metadata(client: &Client, title: &str) -> OpenAlexMetadata {
    let mut metadata = OpenAlexMetadata::default();
    let title = title.trim();
    if title.is_empty() {
        return metadata;
    }
    let mut url = match Url::parse(OPENALEX_WORKS_ENDPOINT) {
        Ok(url) => url,
        Err(_) => return metadata,
    };
    url.query_pairs_mut()
        .append_pair("search", title)
        .append_pair("per-page", "5");
    let resp = client
        .get(url)
        .header(reqwest::header::USER_AGENT, "arxiv-sanity-preserver/1.0")
        .send();
    match resp {
        Ok(response) => {
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                println!("OpenAlex returned 404 for {}", title);
                return metadata;
            }
            if !response.status().is_success() {
                println!(
                    "OpenAlex returned status {} for {}",
                    response.status(),
                    title
                );
                return metadata;
            }
            let body = match response.text() {
                Ok(text) => text,
                Err(err) => {
                    println!(
                        "Unexpected error fetching OpenAlex metadata for {}: {}",
                        title, err
                    );
                    return metadata;
                }
            };
            let payload: Value = match serde_json::from_str(&body) {
                Ok(value) => value,
                Err(err) => {
                    println!(
                        "Unexpected error parsing OpenAlex metadata for {}: {}",
                        title, err
                    );
                    return metadata;
                }
            };
            let results = match payload.get("results").and_then(Value::as_array) {
                Some(results) => results,
                None => return metadata,
            };
            let normalized_title = normalize_title(title);
            let mut best_match = None;
            for result in results {
                if let Some(display_name) = result.get("display_name").and_then(Value::as_str) {
                    if normalize_title(display_name) == normalized_title {
                        best_match = Some(result);
                        break;
                    }
                }
                if best_match.is_none() {
                    best_match = Some(result);
                }
            }
            if let Some(result) = best_match {
                metadata.citation_count = result.get("cited_by_count").and_then(Value::as_i64);
                metadata.is_accepted = result.get("is_accepted").and_then(Value::as_bool);
                metadata.is_published = result.get("is_published").and_then(Value::as_bool);
            }
            metadata
        }
        Err(err) => {
            println!("Unexpected error fetching OpenAlex metadata for {}: {}", title, err);
            metadata
        }
    }
}

fn normalize_title(title: &str) -> String {
    title
        .to_lowercase()
        .chars()
        .filter(|ch| !PUNCTUATION.contains(ch))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn compute_library_sorted_pids(database_path: &Path) -> Result<Vec<String>, String> {
    let conn = Connection::open(database_path)
        .map_err(|err| format!("Failed to open database at {database_path:?}: {err}"))?;
    let mut stmt = conn
        .prepare("select paper_id from library")
        .map_err(|err| format!("Failed to query library: {err}"))?;
    let mut counts: HashMap<String, i64> = HashMap::new();
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|err| format!("Failed to read library rows: {err}"))?;
    for row in rows {
        let pid = row.map_err(|err| format!("Failed to read library row: {err}"))?;
        *counts.entry(pid).or_insert(0) += 1;
    }
    let mut top: Vec<(i64, String)> = counts
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .map(|(pid, count)| (count, pid))
        .collect();
    top.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(top.into_iter().map(|entry| entry.1).collect())
}

fn build_search_dict(
    db: &HashMap<String, PaperRecord>,
    vocab: &HashMap<String, usize>,
    idf: &[f64],
) -> HashMap<String, HashMap<String, f64>> {
    let mut search_dict = HashMap::new();
    for (pid, paper) in db.iter() {
        let dict_title = makedict(&paper.title, Some(5.0), 3.0, vocab, idf);
        let authors_str = paper
            .authors
            .iter()
            .map(|author| author.name.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let mut dict_authors = makedict(&authors_str, Some(5.0), 1.0, vocab, idf);
        if dict_authors.contains_key("and") {
            dict_authors.remove("and");
        }
        let mut dict_categories = HashMap::new();
        for tag in &paper.tags {
            dict_categories.insert(tag.term.to_lowercase(), 5.0);
        }
        let dict_summary = makedict(&paper.summary, None, 1.0, vocab, idf);
        let merged = merge_dicts(&[dict_title, dict_authors, dict_categories, dict_summary]);
        search_dict.insert(pid.clone(), merged);
    }
    search_dict
}

fn makedict(
    text: &str,
    force_idf: Option<f64>,
    scale: f64,
    vocab: &HashMap<String, usize>,
    idf: &[f64],
) -> HashMap<String, f64> {
    let cleaned = text
        .to_lowercase()
        .chars()
        .filter(|ch| !PUNCTUATION.contains(ch))
        .collect::<String>();
    let words: HashSet<&str> = cleaned.split_whitespace().collect();
    let mut output = HashMap::new();
    for word in words {
        let idf_val = if let Some(force) = force_idf {
            force
        } else if let Some(idx) = vocab.get(word) {
            idf.get(*idx).copied().unwrap_or(1.0) * scale
        } else {
            1.0 * scale
        };
        output.insert(word.to_string(), idf_val);
    }
    output
}

fn merge_dicts(dicts: &[HashMap<String, f64>]) -> HashMap<String, f64> {
    let mut merged = HashMap::new();
    for dict in dicts {
        for (key, val) in dict {
            *merged.entry(key.clone()).or_insert(0.0) += val;
        }
    }
    merged
}

fn load_pickle<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let data =
        fs::read(path).map_err(|err| format!("Failed to read pickle {}: {err}", path.display()))?;
    serde_pickle::from_slice(&data, Default::default())
        .map_err(|err| format!("Failed to decode pickle {}: {err}", path.display()))
}

fn write_pickle<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let options = SerOptions::new().proto_v2();
    let encoded =
        serde_pickle::to_vec(value, options).map_err(|err| format!("Failed to encode pickle: {err}"))?;
    utils::write_atomic_bytes(path, &encoded)
}

#[allow(dead_code)]
pub fn run_make_cache_with_default_config() -> Result<(), String> {
    run_make_cache(Path::new(DEFAULT_CONFIG_PATH))
}
