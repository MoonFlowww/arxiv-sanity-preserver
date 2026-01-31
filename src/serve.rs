use axum::extract::{Path as AxumPath, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use clap::Parser;
use chrono::{Datelike, NaiveDate};
use minijinja::value::Value as MiniValue;
use minijinja::Environment;
use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use serde_pickle::de::DeOptions;
use serde_pickle::value::{HashableValue as PickleHashableValue, Value as PickleValue};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower_http::services::ServeDir;
use url::form_urlencoded;

use arxiv_sanity_pipeline::db::Database;
use arxiv_sanity_pipeline::utils;
use crate::hnsw_index::HnswIndex;

const SINGLE_USER_ID: i64 = 1;
const RECOMPUTE_STALE_SECONDS: i64 = 30 * 60;
const DEFAULT_PIPELINE_DIR: &str = ".pipeline";
const SIMILAR_RESULTS: usize = 50;
const SETTINGS_KEY_SHOW_PROMPT: &str = "show_prompt";
fn pipeline_path(entry: &str) -> PathBuf {
    Path::new(DEFAULT_PIPELINE_DIR).join(entry)
}

#[derive(Parser, Debug, Clone)]
#[command(name = "serve.py")]
pub struct ServeArgs {
    #[arg(
        short = 'p',
        long = "prod",
        action = clap::ArgAction::SetTrue,
        help = "run in prod?"
    )]
    pub prod: bool,
    #[arg(
        short = 'r',
        long = "num_results",
        default_value_t = 200,
        help = "number of results to return per query"
    )]
    pub num_results: usize,
    #[arg(
        long = "port",
        default_value_t = 5000,
        help = "port to serve on"
    )]
    pub port: u16,
}

impl ServeArgs {
    pub fn new(prod: bool, num_results: usize, port: u16) -> Self {
        Self {
            prod,
            num_results,
            port,
        }
    }
}

#[derive(Clone)]
struct ServeConfig {
    num_results: usize,
    port: u16,
    single_user_name: String,
    db_path: PathBuf,
    db_jsonl_path: PathBuf,
    pdf_dir: PathBuf,
    txt_dir: PathBuf,
    thumbs_dir: PathBuf,
    tfidf_path: PathBuf,
    meta_path: PathBuf,
    tfidf_meta_json_path: PathBuf,
    hnsw_index_path: PathBuf,
    user_sim_path: PathBuf,
    user_sim_json_path: PathBuf,
    db_serve_path: PathBuf,
    serve_cache_path: PathBuf,
    database_path: PathBuf,
    ingest_jobs_dir: PathBuf,
    tmp_dir: PathBuf,
    download_settings_path: PathBuf,
    ui_settings_path: PathBuf,
}

impl ServeConfig {
    fn new(args: &ServeArgs) -> Self {
        Self {
            num_results: args.num_results,
            port: args.port,
            single_user_name: std::env::var("ASP_SINGLE_USER_NAME")
                .unwrap_or_else(|_| "localuser".to_string()),
            db_path: pipeline_path("db.p"),
            db_jsonl_path: pipeline_path("db.jsonl"),
            pdf_dir: pipeline_path("pdf"),
            txt_dir: pipeline_path("txt"),
            thumbs_dir: pipeline_path("thumb"),
            tfidf_path: pipeline_path("tfidf.p"),
            meta_path: pipeline_path("tfidf_meta.p"),
            tfidf_meta_json_path: pipeline_path("tfidf_meta.json"),
            hnsw_index_path: pipeline_path("hnsw_index.bin"),
            user_sim_path: pipeline_path("user_sim.p"),
            user_sim_json_path: pipeline_path("user_sim.json"),
            db_serve_path: pipeline_path("db2.p"),
            serve_cache_path: pipeline_path("serve_cache.p"),
            database_path: pipeline_path("as.db"),
            ingest_jobs_dir: pipeline_path("ingest_jobs"),
            tmp_dir: pipeline_path("tmp"),
            download_settings_path: pipeline_path("download_settings.json"),
            ui_settings_path: pipeline_path("ui_settings.json"),
        }
    }
}

#[derive(Clone)]
struct TopicInfo {
    name: String,
    display_name: String,
    count: usize,
}

#[derive(Clone)]
struct TopicStat {
    display_name: String,
    paper_count: usize,
    paper_count_display: String,
    citations_total: i64,
    citations_total_display: String,
    avg_score: Option<f64>,
    avg_score_display: String,
}

#[derive(Clone)]
struct DateRange {
    start: String,
    end: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DownloadTopicSettings {
    start: Option<String>,
    end: Option<String>,
    published: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DownloadSettings {
    selected_topics: Vec<String>,
    topics: HashMap<String, DownloadTopicSettings>,
}


#[derive(Debug, Clone, serde::Deserialize)]
struct UiSettings {
    show_prompt: Option<String>,
}


impl Default for DownloadSettings {
    fn default() -> Self {
        Self {
            selected_topics: Vec::new(),
            topics: HashMap::new(),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct DownloadTopicInput {
    start: Option<String>,
    end: Option<String>,
    published: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct DownloadSettingsPayload {
    selected_topics: Vec<String>,
    topics: HashMap<String, DownloadTopicInput>,
}

#[derive(Clone)]
struct ServeData {
    db: HashMap<String, Value>,
    hnsw_index: HnswIndex,
    user_sim: HashMap<i64, Vec<String>>,
    date_sorted_pids: Vec<String>,
    top_sorted_pids: Vec<String>,
    search_dict: HashMap<String, HashMap<String, f64>>,
    topic_pids: HashMap<String, Vec<String>>,
    topics: Vec<TopicInfo>,
    settings_date_range: DateRange,
    settings_topic_stats: Vec<TopicStat>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IngestJob {
    job_id: String,
    paper_id: String,
    label: String,
    percent: i64,
    messages: Vec<String>,
    done: bool,
    error: bool,
    status: String,
    warning: bool,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RecomputeStatus {
    status: String,
    updated_at: i64,
    percent: serde_json::Value,
    stdout_path: String,
    stderr_path: String,
    message: Option<String>,
    error: Option<String>,
}

#[derive(Clone)]
struct AppState {
    config: ServeConfig,
    data: Arc<RwLock<ServeData>>,
    ingest_jobs: Arc<Mutex<HashMap<String, IngestJob>>>,
    recompute_status: Arc<Mutex<Option<RecomputeStatus>>>,
    recompute_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = ServeArgs::parse();
    run_with_args(args).await
}

fn build_template_env() -> Environment<'static> {
    let mut env = Environment::new();
    env.add_filter("tojson", |value: MiniValue| {
        let json = serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string());
        MiniValue::from_safe_string(json)
    });
    env.add_filter("truncate_topic_name", |value: String, max_len: usize| {
        truncate_topic_name(&value, max_len)
    });
    env.add_function("truncate_topic_name", |value: String, max_len: usize| {
        truncate_topic_name(&value, max_len)
    });
    env.add_function("url_for", |endpoint: String, filename: Option<String>| {
        if endpoint == "static" {
            if let Some(name) = filename {
                format!("/static/{}", name)
            } else {
                "/static".to_string()
            }
        } else {
            format!("/{}", endpoint)
        }
    });
    env.set_loader(minijinja::path_loader("templates"));
    env
}

pub async fn run_with_args(args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let config = ServeConfig::new(&args);
    ensure_database(&config)?;
    let data = refresh_serving_data(&config)?;

    let state = AppState {
        config: config.clone(),
        data: Arc::new(RwLock::new(data)),
        ingest_jobs: Arc::new(Mutex::new(HashMap::new())),
        recompute_status: Arc::new(Mutex::new(None)),
        recompute_thread: Arc::new(Mutex::new(None)),
    };

    let env = build_template_env();

    let app = Router::new()
        .route("/", get(intmain))
        .route("/health", get(health_check))
        .route("/goaway", post(goaway))
        .route("/ingest/status/:job_id", get(ingest_status))
        .route("/ingest/recompute_status", get(ingest_recompute_status))
        .route("/recompute/status", get(recompute_status_endpoint))
        .route("/ingest", post(ingest_arxiv))
        .route("/settings/download", post(update_download_settings))
        .route("/search", get(search))
        .route("/topics", get(topics))
        .route("/recommend", get(recommend))
        .route("/top", get(top))
        .route("/toptwtr", get(toptwtr))
        .route("/library", get(library))
        .route("/libtoggle", post(libtoggle))
        .route("/:request_pid", get(rank))
        .nest_service(
            "/static/thumbs",
            ServeDir::new(config.thumbs_dir.clone()),
        )
        .nest_service("/static", ServeDir::new("static"))
        .with_state((state, Arc::new(env)));

    let addr = format!("0.0.0.0:{}", config.port);
    println!("starting rust server on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn truncate_topic_name(name: &str, max_len: usize) -> String {
    if name.chars().count() <= max_len {
        return name.to_string();
    }

    let truncated_candidate: String = name.chars().take(max_len).collect();
    let trimmed = match truncated_candidate.rsplit_once(' ') {
        Some((head, _)) => head,
        None => truncated_candidate.as_str(),
    };
    let trimmed = trimmed.trim_end();
    let truncated = if trimmed.is_empty() {
        truncated_candidate.as_str()
    } else {
        trimmed
    };
    format!("{truncated}...")
}


fn ensure_database(config: &ServeConfig) -> Result<(), io::Error> {
    if !config.database_path.exists() {
        println!("did not find as.db, creating an empty database from embedded schema...");
        let db = Database::open(&config.database_path)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        db.initialize_schema()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    }
    Ok(())
}

fn refresh_serving_data(config: &ServeConfig) -> Result<ServeData, Box<dyn std::error::Error>> {
    println!(
        "loading the paper database from {:?}",
        config.db_serve_path
    );
    let db_value = load_pickle_or_json_value(&config.db_serve_path, None)?;
    let mut db = value_to_map(db_value)?;
    ensure_impact_scores(&mut db);

    println!(
        "loading tfidf metadata from {:?} (json fallback {:?})",
        config.meta_path, config.tfidf_meta_json_path
    );
    let _meta = load_pickle_or_json_value(&config.meta_path, Some(&config.tfidf_meta_json_path))?;
    println!("loading HNSW index from {:?}", config.hnsw_index_path);
    let hnsw_index = load_hnsw_index(&config.hnsw_index_path)?;

    println!("loading user recommendations {:?}", config.user_sim_path);
    let user_sim_value =
        load_pickle_or_json_value_optional(&config.user_sim_path, &config.user_sim_json_path)?;
    let user_sim = value_to_user_sim(user_sim_value);

    println!("loading serve cache... {:?}", config.serve_cache_path);
    let cache_value = load_pickle_or_json_value(&config.serve_cache_path, None)?;
    let cache_map = cache_value
        .as_object()
        .ok_or("serve cache is not a map")?;
    let date_sorted_pids = cache_map
        .get("date_sorted_pids")
        .and_then(value_as_string_list)
        .unwrap_or_default();
    let search_dict = cache_map
        .get("search_dict")
        .map(value_to_search_dict)
        .unwrap_or_default();

    let top_sorted_pids = sort_by_impact(&db);
    let (topic_pids, topics) = build_topics(&db, &date_sorted_pids);
    let settings_date_range = build_date_range(&db);
    let settings_topic_stats = build_topic_stats(&db, &topic_pids);

    Ok(ServeData {
        db,
        hnsw_index,
        user_sim,
        date_sorted_pids,
        top_sorted_pids,
        search_dict,
        topic_pids,
        topics,
        settings_date_range,
        settings_topic_stats,
    })
}

fn parse_date_string(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn clean_date_value(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim().to_string();
        if trimmed.is_empty() {
            return None;
        }
        if parse_date_string(&trimmed).is_some() {
            Some(trimmed)
        } else {
            None
        }
    })
}

fn sanitize_download_settings(
    mut settings: DownloadSettings,
    valid_topics: &HashSet<String>,
) -> DownloadSettings {
    settings
        .selected_topics
        .retain(|topic| valid_topics.contains(topic));
    settings.topics.retain(|topic, value| {
        if !valid_topics.contains(topic) {
            return false;
        }
        value.start = clean_date_value(value.start.take());
        value.end = clean_date_value(value.end.take());
        if let (Some(start), Some(end)) = (&value.start, &value.end) {
            if let (Some(start_date), Some(end_date)) =
                (parse_date_string(start), parse_date_string(end))
            {
                if start_date > end_date {
                    value.end = None;
                }
            }
        }
        true
    });
    settings
}

fn load_download_settings(config: &ServeConfig, topics: &[TopicInfo]) -> DownloadSettings {
    let bytes = match fs::read(&config.download_settings_path) {
        Ok(bytes) => bytes,
        Err(_) => return DownloadSettings::default(),
    };
    let parsed: DownloadSettings = match serde_json::from_slice(&bytes) {
        Ok(settings) => settings,
        Err(_) => return DownloadSettings::default(),
    };
    let valid_topics: HashSet<String> = topics.iter().map(|topic| topic.name.clone()).collect();
    sanitize_download_settings(parsed, &valid_topics)
}

fn load_show_prompt_from_db(conn: &Connection, user_id: i64) -> Option<String> {
    let table_exists: Option<String> = conn
        .query_row(
            "select name from sqlite_master where type='table' and name='user_settings'",
            [],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten();
    if table_exists.is_none() {
        return None;
    }
    let column_exists: Option<i64> = conn
        .query_row(
            "select 1 from pragma_table_info('user_settings') where name = 'show_prompt' limit 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten();
    if column_exists.is_none() {
        return None;
    }
    conn.query_row(
        "select show_prompt from user_settings where user_id = ? limit 1",
        [user_id],
        |row| row.get(0),
    )
        .optional()
        .ok()
        .flatten()
}

fn load_show_prompt_from_config(config: &ServeConfig) -> Option<String> {
    let bytes = fs::read(&config.ui_settings_path).ok()?;
    let parsed: UiSettings = serde_json::from_slice(&bytes).ok()?;
    parsed.show_prompt
}

fn load_show_prompt_preference(
    config: &ServeConfig,
    conn: &Connection,
    user_id: i64,
) -> Option<String> {
    load_show_prompt_from_db(conn, user_id).or_else(|| load_show_prompt_from_config(config))
}

fn persist_download_settings(
    config: &ServeConfig,
    settings: &DownloadSettings,
) -> Result<(), String> {
    if let Some(parent) = config.download_settings_path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let serialized = serde_json::to_vec_pretty(settings).map_err(|err| err.to_string())?;
    utils::write_atomic_bytes(&config.download_settings_path, &serialized)
        .map_err(|err| err.to_string())?;
    Ok(())
}


fn load_hnsw_index(path: &Path) -> Result<HnswIndex, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Err(format!("Missing HNSW index at {}", path.display()).into());
    }
    let bytes = fs::read(path)
        .map_err(|err| format!("Failed to read {}: {}", path.display(), err))?;
    let index: HnswIndex = bincode::deserialize(&bytes)
        .map_err(|err| format!("Failed to decode {}: {}", path.display(), err))?;
    println!("loaded HNSW index from {}", path.display());
    Ok(index)
}

fn refresh_hnsw_in_state(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    let index = load_hnsw_index(&state.config.hnsw_index_path)?;
    let mut data = state.data.write().unwrap();
    data.hnsw_index = index;
    Ok(())
}


fn load_pickle_or_json_value(
    pickle_path: &Path,
    json_path: Option<&Path>,
) -> Result<Value, Box<dyn std::error::Error>> {
    if pickle_path.exists() {
        let pickle_result = (|| -> Result<Value, Box<dyn std::error::Error>> {
            let bytes = fs::read(pickle_path).map_err(|err| {
                format!("Failed to read {}: {}", pickle_path.display(), err)
            })?;
            let value: PickleValue =
                serde_pickle::from_slice(&bytes, DeOptions::default()).map_err(|err| {
                    format!("Failed to decode {}: {}", pickle_path.display(), err)
                })?;
            Ok(pickle_to_json(value))
        })();
        match pickle_result {
            Ok(value) => {
                println!("loaded data from pickle {:?}", pickle_path);
                Ok(value)
            }
            Err(pickle_err) => {
                if let Some(path) = json_path {
                    let json_result = (|| -> Result<Value, Box<dyn std::error::Error>> {
                        let json_bytes = fs::read(path)
                            .map_err(|err| format!("Failed to read {}: {}", path.display(), err))?;
                        Ok(serde_json::from_slice(&json_bytes).map_err(|err| {
                            format!("Failed to decode {}: {}", path.display(), err)
                        })?)
                    })();
                    match json_result {
                        Ok(value) => {
                            eprintln!(
                                "failed to load pickle {:?}: {}. Loaded JSON from {:?}.",
                                pickle_path, pickle_err, path
                            );
                            Ok(value)
                        }
                        Err(json_err) => Err(format!(
                            "Unable to load data from pickle {:?} ({}) or JSON {:?} ({}).",
                            pickle_path, pickle_err, path, json_err
                        )
                        .into()),
                    }
                } else {
                    Err(format!(
                        "Unable to load data from pickle {:?}: {}.",
                        pickle_path, pickle_err
                    )
                    .into())
                }
            }
        }
    } else if let Some(path) = json_path {
        let json_bytes = fs::read(path)
            .map_err(|err| format!("Failed to read {}: {}", path.display(), err))?;
        let value = serde_json::from_slice(&json_bytes)
            .map_err(|err| format!("Failed to decode {}: {}", path.display(), err))?;
        println!("loaded data from JSON {:?}", path);
        Ok(value)
    } else {
        Err(format!(
            "Unable to load data. Missing {:?} and {:?}.",
            pickle_path, json_path
        )
        .into())
    }
}

fn load_pickle_or_json_value_optional(
    pickle_path: &Path,
    json_path: &Path,
) -> Result<Option<Value>, Box<dyn std::error::Error>> {
    if pickle_path.exists() {
        let bytes = fs::read(pickle_path)
            .map_err(|err| format!("Failed to read {}: {}", pickle_path.display(), err))?;
        let value: PickleValue =
            serde_pickle::from_slice(&bytes, DeOptions::default()).map_err(|err| {
                format!("Failed to decode {}: {}", pickle_path.display(), err)
            })?;
        Ok(Some(pickle_to_json(value)))
    } else if json_path.exists() {
        let json_bytes = fs::read(json_path)
            .map_err(|err| format!("Failed to read {}: {}", json_path.display(), err))?;
        Ok(Some(serde_json::from_slice(&json_bytes).map_err(|err| {
            format!("Failed to decode {}: {}", json_path.display(), err)
        })?))
    } else {
        Ok(None)
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
            for (k, v) in entries {
                let key = pickle_key_to_string(k);
                map.insert(key, pickle_to_json(v));
            }
            Value::Object(map)
        }
    }
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
        PickleHashableValue::FrozenSet(values) => Value::Array(
            values
                .into_iter()
                .map(hashable_to_json)
                .collect(),
        ),
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
            format!("({})", parts.join(","))
        }
        PickleHashableValue::FrozenSet(values) => {
            let parts: Vec<String> = values.into_iter().map(pickle_key_to_string).collect();
            format!("frozenset({})", parts.join(","))
        }
        PickleHashableValue::None => "None".to_string(),
    }
}

fn value_to_map(value: Value) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    let obj = value
        .as_object()
        .ok_or("expected object in pickle")?;
    Ok(obj.clone().into_iter().collect())
}

fn value_to_user_sim(value: Option<Value>) -> HashMap<i64, Vec<String>> {
    let mut out = HashMap::new();
    let Some(value) = value else { return out };
    if let Some(map) = value.as_object() {
        for (key, value) in map {
            if let Ok(user_id) = key.parse::<i64>() {
                out.insert(user_id, value_as_string_list(value).unwrap_or_default());
            }
        }
    }
    out
}

fn value_to_search_dict(value: &Value) -> HashMap<String, HashMap<String, f64>> {
    let mut out = HashMap::new();
    if let Some(map) = value.as_object() {
        for (pid, value) in map {
            if let Some(inner) = value.as_object() {
                let mut inner_map = HashMap::new();
                for (k, v) in inner {
                    let score = v.as_f64().unwrap_or_else(|| v.as_i64().unwrap_or(0) as f64);
                    inner_map.insert(k.clone(), score);
                }
                out.insert(pid.clone(), inner_map);
            }
        }
    }
    out
}

fn value_as_string_list(value: &Value) -> Option<Vec<String>> {
    value.as_array().map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    })
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64
}

fn translate_topic_name(code: &str) -> &str {
    TOPIC_TRANSLATIONS.get(code).map(|s| *s).unwrap_or(code)
}

fn format_score(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{:.3}", v),
        None => "—".to_string(),
    }
}

fn format_with_commas<T: ToString>(value: T) -> String {
    let s = value.to_string();
    let (sign, digits) = if let Some(stripped) = s.strip_prefix('-') {
        ("-", stripped)
    } else {
        ("", s.as_str())
    };
    let mut out = String::new();
    let mut count = 0;
    for ch in digits.chars().rev() {
        if count == 3 {
            out.push(',');
            count = 0;
        }
        out.push(ch);
        count += 1;
    }
    let formatted: String = out.chars().rev().collect();
    format!("{}{}", sign, formatted)
}

fn build_date_range(paper_db: &HashMap<String, Value>) -> DateRange {
    let mut timestamps: Vec<i64> = paper_db
        .values()
        .filter_map(|paper| paper.get("time_published").and_then(|v| v.as_i64()))
        .collect();
    if timestamps.is_empty() {
        return DateRange {
            start: "—".to_string(),
            end: "—".to_string(),
        };
    }
    timestamps.sort_unstable();
    let start_ts = timestamps.first().copied().unwrap_or(0);
    let end_ts = timestamps.last().copied().unwrap_or(0);
    DateRange {
        start: format_date(start_ts),
        end: format_date(end_ts),
    }
}

fn build_topic_stats(
    paper_db: &HashMap<String, Value>,
    topic_pids: &HashMap<String, Vec<String>>,
) -> Vec<TopicStat> {
    let mut stats = Vec::new();
    for (topic, pids) in topic_pids {
        let mut citation_sum: i64 = 0;
        let mut scores: Vec<f64> = Vec::new();
        for pid in pids {
            if let Some(paper) = paper_db.get(pid) {
                if let Some(count) = paper.get("citation_count").and_then(|v| v.as_i64()) {
                    citation_sum += count;
                }
                if let Some(score) = paper.get("impact_score").and_then(|v| v.as_f64()) {
                    scores.push(score);
                }
            }
        }
        let avg_score = if scores.is_empty() {
            None
        } else {
            Some(scores.iter().sum::<f64>() / scores.len() as f64)
        };
        stats.push(TopicStat {
            display_name: translate_topic_name(topic).to_string(),
            paper_count: pids.len(),
            paper_count_display: format_with_commas(pids.len()),
            citations_total: citation_sum,
            citations_total_display: format_with_commas(citation_sum),
            avg_score,
            avg_score_display: format_score(avg_score),
        });
    }
    stats.sort_by(|a, b| {
        b.paper_count
            .cmp(&a.paper_count)
            .then_with(|| a.display_name.cmp(&b.display_name))
    });
    stats
}

fn get_storage_usage(download_dir: &Path) -> String {
    if !download_dir.is_dir() {
        return "0 B".to_string();
    }
    let output = Command::new("du")
        .arg("-sh")
        .arg(download_dir)
        .output();
    match output {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.split_whitespace().next().unwrap_or("0 B").to_string()
        }
        _ => "Unknown".to_string(),
    }
}

fn build_topics(
    paper_db: &HashMap<String, Value>,
    date_sorted_pids: &[String],
) -> (HashMap<String, Vec<String>>, Vec<TopicInfo>) {
    let mut topic_pids: HashMap<String, Vec<String>> = HashMap::new();
    for pid in date_sorted_pids {
        if let Some(topic) = paper_db
            .get(pid)
            .and_then(|p| p.get("arxiv_primary_category"))
            .and_then(|v| v.get("term"))
            .and_then(|v| v.as_str())
        {
            topic_pids
                .entry(topic.to_string())
                .or_default()
                .push(pid.clone());
        }
    }
    let mut topics: Vec<TopicInfo> = topic_pids
        .iter()
        .map(|(topic, pids)| TopicInfo {
            name: topic.clone(),
            count: pids.len(),
            display_name: translate_topic_name(topic).to_string(),
        })
        .collect();
    topics.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    (topic_pids, topics)
}

fn ensure_time_metadata(paper: &mut Value) {
    let needs_updated = paper.get("time_updated").is_none();
    let needs_published = paper.get("time_published").is_none();
    if !needs_updated && !needs_published {
        return;
    }
    if let Some(obj) = paper.as_object_mut() {
        if needs_updated {
            if let Some(updated) = obj.get("updated").and_then(|v| v.as_str()) {
                if let Some(ts) = parse_datetime(updated) {
                    obj.insert("time_updated".to_string(), json!(ts));
                }
            }
        }
        if needs_published {
            if let Some(published) = obj.get("published").and_then(|v| v.as_str()) {
                if let Some(ts) = parse_datetime(published) {
                    obj.insert("time_published".to_string(), json!(ts));
                }
            }
        }
    }
}

fn parse_datetime(value: &str) -> Option<i64> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp());
    if parsed.is_some() {
        return parsed;
    }
    let datetime_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"];
    let date_formats = ["%Y-%m-%d"];
    for fmt in datetime_formats {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, fmt) {
            return Some(dt.and_utc().timestamp());
        }
    }
    for fmt in date_formats {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(value, fmt) {
            let dt = date.and_hms_opt(0, 0, 0)?;
            return Some(dt.and_utc().timestamp());
        }
    }
    None
}

fn compute_impact_score(paper: &mut Value, now_ts: i64, alpha: f64) {
    ensure_time_metadata(paper);
    let time_published = paper
        .get("time_published")
        .and_then(|v| v.as_i64())
        .unwrap_or(now_ts);
    let years_since_pub =
        ((now_ts - time_published) as f64) / (365.25 * 24.0 * 60.0 * 60.0);
    if let Some(obj) = paper.as_object_mut() {
        obj.insert("years_since_pub".to_string(), json!(years_since_pub));
        if let Some(citations) = obj
            .get("citation_count")
            .and_then(|v| v.as_i64())
            .map(|val| val as f64)
        {
            let score = (1.0 + citations).ln() - alpha * years_since_pub.max(0.0);
            obj.insert("impact_score".to_string(), json!(score));
        } else {
            obj.insert("impact_score".to_string(), Value::Null);
        }
    }
}

fn ensure_impact_scores(db: &mut HashMap<String, Value>) {
    let now_ts = now_ts();
    for paper in db.values_mut() {
        compute_impact_score(paper, now_ts, 0.3);
    }
}

fn sort_by_impact(db: &HashMap<String, Value>) -> Vec<String> {
    let mut scored: Vec<(f64, String)> = db
        .iter()
        .filter_map(|(pid, paper)| {
            paper
                .get("impact_score")
                .and_then(|v| v.as_f64())
                .map(|score| (score, pid.clone()))
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().map(|(_, pid)| pid).collect()
}

fn normalize_topics(topic_names: &[String]) -> Vec<String> {
    topic_names.iter().filter(|t| !t.is_empty()).cloned().collect()
}

fn has_repo_metadata(paper: &Value) -> bool {
    if paper
        .get("is_opensource")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || paper
            .get("has_github")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        || paper
            .get("is_open_source")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    {
        return true;
    }
    if let Some(repo_links) = paper.get("repo_links") {
        if repo_links.is_array() {
            return repo_links.as_array().map(|a| !a.is_empty()).unwrap_or(false);
        }
        return !repo_links.is_null();
    }
    false
}

fn publication_statuses(paper: &Value) -> Vec<String> {
    let mut statuses: HashSet<String> = HashSet::new();
    let status_fields = ["publication_status", "published_status", "conference_status"];
    for field in status_fields {
        if let Some(val) = paper.get(field) {
            if let Some(s) = val.as_str() {
                statuses.insert(s.to_lowercase());
            } else if let Some(arr) = val.as_array() {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        statuses.insert(s.to_lowercase());
                    }
                }
            }
        }
    }
    if paper
        .get("accepted")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || paper
            .get("is_accepted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    {
        statuses.insert("accepted".to_string());
    }
    if paper
        .get("is_published")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        statuses.insert("published".to_string());
    }
    if paper
        .get("presented")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || paper
            .get("is_presented")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    {
        statuses.insert("presented".to_string());
    }

    let metadata_fields = [
        "comment",
        "comments",
        "journal_ref",
        "journal-ref",
        "journalref",
        "journal",
        "annotation",
        "annotations",
        "notes",
    ];
    let mut metadata_chunks: Vec<String> = Vec::new();
    for field in metadata_fields {
        if let Some(val) = paper.get(field) {
            if let Some(s) = val.as_str() {
                metadata_chunks.push(s.to_string());
            } else if let Some(arr) = val.as_array() {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        metadata_chunks.push(s.to_string());
                    }
                }
            }
        }
    }
    if !metadata_chunks.is_empty() {
        let metadata_text = metadata_chunks.join(" ");
        let metadata_lower = metadata_text.to_lowercase();
        let venue_year_pattern = Regex::new(r"\b[A-Za-z][A-Za-z0-9&.+/\-]{2,}\s?(?:20\d{2}|19\d{2}|''?\d{2})\b")
            .unwrap();
        let has_venue_year = venue_year_pattern.is_match(&metadata_text);
        if has_venue_year {
            statuses.insert("accepted".to_string());
        }
        let acceptance_patterns = [
            r"\baccepted\b",
            r"\bto appear\b",
            r"\bin press\b",
            r"\bcamera[- ]ready\b",
            r"\bappears in\b",
            r"\bin proceedings\b",
            r"\bpublished in\b",
        ];
        let presentation_patterns = [
            r"\boral\b",
            r"\bspotlight\b",
            r"\bposter\b",
            r"\bpresented at\b",
            r"\bpresentation at\b",
            r"\bwill be presented\b",
            r"\bpresenting at\b",
            r"\baccepted as an? (oral|poster|spotlight)\b",
        ];
        if acceptance_patterns
            .iter()
            .any(|pattern| Regex::new(pattern).unwrap().is_match(&metadata_lower))
        {
            statuses.insert("accepted".to_string());
        }
        if presentation_patterns
            .iter()
            .any(|pattern| Regex::new(pattern).unwrap().is_match(&metadata_lower))
        {
            statuses.insert("presented".to_string());
            statuses.insert("accepted".to_string());
        }
    }
    statuses.into_iter().collect()
}

fn matches_publication_filters(paper: &Value, filters: &[String]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let paper_statuses = publication_statuses(paper);
    if paper_statuses.is_empty() {
        return false;
    }
    paper_statuses
        .iter()
        .any(|status| filters.contains(&status.to_lowercase()))
}

fn sort_papers(papers: Vec<Value>, sort_by: &str, sort_order: &str) -> Vec<Value> {
    let reverse = sort_order != "asc";
    let mut out = papers;
    out.sort_by(|a, b| {
        let ordering = match sort_by {
            "score" => {
                let score_a = a.get("impact_score").and_then(|v| v.as_f64());
                let score_b = b.get("impact_score").and_then(|v| v.as_f64());
                let ordering = if reverse {
                    match (score_a, score_b) {
                        (Some(a), Some(b)) => b
                            .partial_cmp(&a)
                            .unwrap_or(std::cmp::Ordering::Equal),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                } else {
                    match (score_a, score_b) {
                        (Some(a), Some(b)) => a
                            .partial_cmp(&b)
                            .unwrap_or(std::cmp::Ordering::Equal),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                };
                return ordering;
            }
            "citation" => {
                let count_a = a.get("citation_count").and_then(|v| v.as_i64());
                let count_b = b.get("citation_count").and_then(|v| v.as_i64());
                let ordering = if reverse {
                    match (count_a, count_b) {
                        (Some(a), Some(b)) => b.cmp(&a),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                } else {
                    match (count_a, count_b) {
                        (Some(a), Some(b)) => a.cmp(&b),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                };
                return ordering;
            }
            _ => {
                let time_a = a.get("time_published").and_then(|v| v.as_i64()).unwrap_or(0);
                let time_b = b.get("time_published").and_then(|v| v.as_i64()).unwrap_or(0);
                time_b.cmp(&time_a)
            }
        };
        if reverse { ordering } else { ordering.reverse() }
    });
    out
}

fn filter_papers(
    papers: Vec<Value>,
    topic_names: &[String],
    min_score: Option<f64>,
    open_source: bool,
    publication_filters: &[String],
) -> Vec<Value> {
    let normalized_topics = normalize_topics(topic_names);
    let mut filtered = papers;
    if !normalized_topics.is_empty() {
        filtered = filtered
            .into_iter()
            .filter(|paper| {
                paper
                    .get("arxiv_primary_category")
                    .and_then(|v| v.get("term"))
                    .and_then(|v| v.as_str())
                    .map(|t| normalized_topics.contains(&t.to_string()))
                    .unwrap_or(false)
            })
            .collect();
    }
    if let Some(score) = min_score {
        filtered = filtered
            .into_iter()
            .filter(|paper| paper.get("impact_score").and_then(|v| v.as_f64()).unwrap_or(-1.0) >= score)
            .collect();
    }
    if open_source {
        filtered = filtered
            .into_iter()
            .filter(|paper| has_repo_metadata(paper))
            .collect();
    }
    if !publication_filters.is_empty() {
        let lowered: Vec<String> = publication_filters.iter().map(|s| s.to_lowercase()).collect();
        filtered = filtered
            .into_iter()
            .filter(|paper| matches_publication_filters(paper, &lowered))
            .collect();
    }
    filtered
}

fn papers_search(
    data: &ServeData,
    qraw: &str,
    topic_names: &[String],
    min_score: Option<f64>,
    open_source: bool,
    publication_filters: &[String],
    sort_by: Option<&str>,
    sort_order: &str,
) -> Vec<Value> {
    let qparts: Vec<String> = qraw
        .to_lowercase()
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    if qparts.is_empty() {
        let mut base: Vec<Value> = data
            .date_sorted_pids
            .iter()
            .filter_map(|pid| data.db.get(pid).cloned())
            .collect();
        base = filter_papers(base, topic_names, min_score, open_source, publication_filters);
        if let Some(sort_by) = sort_by {
            return sort_papers(base, sort_by, sort_order);
        }
        return base;
    }
    let mut scores: Vec<(f64, Value)> = Vec::new();
    for (pid, paper) in &data.db {
        let mut score = 0.0;
        if let Some(search_terms) = data.search_dict.get(pid) {
            for part in &qparts {
                score += search_terms.get(part).copied().unwrap_or(0.0);
            }
        }
        if score == 0.0 {
            continue;
        }
        score += 0.0001 * paper.get("tscore").and_then(|v| v.as_f64()).unwrap_or(0.0);
        scores.push((score, paper.clone()));
    }
    scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut out: Vec<Value> = scores.into_iter().filter(|(s, _)| *s > 0.0).map(|(_, p)| p).collect();
    out = filter_papers(out, topic_names, min_score, open_source, publication_filters);
    if let Some(sort_by) = sort_by {
        return sort_papers(out, sort_by, sort_order);
    }
    out
}

fn papers_similar(data: &ServeData, pid: &str) -> Result<Vec<Value>, String> {
    let rawpid = utils::strip_version(pid);
    if !data.db.contains_key(&rawpid) {
        return Ok(vec![]);
    }
    let similar = data
        .hnsw_index
        .find_neighbors(pid, SIMILAR_RESULTS)
        .ok_or_else(|| {
            "HNSW index is required for similarity search but is not loaded.".to_string()
        })?;
    Ok(similar
        .iter()
        .filter_map(|k| data.db.get(&utils::strip_version(k)).cloned())
        .collect())
}

fn papers_from_library(
    data: &ServeData,
    conn: &Connection,
    user_id: i64,
) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let stmt = conn
        .prepare("select paper_id from library where user_id = ?")
        .ok();
    if let Some(mut stmt) = stmt {
        let rows = stmt
            .query_map([user_id], |row| row.get::<_, String>(0))
            .ok();
        if let Some(rows) = rows {
            for pid_result in rows.flatten() {
                let pid = utils::strip_version(&pid_result);
                if let Some(paper) = data.db.get(&pid) {
                    out.push(paper.clone());
                }
            }
        }
    }
    out.sort_by(|a, b| {
        b.get("updated")
            .and_then(|v| v.as_str())
            .cmp(&a.get("updated").and_then(|v| v.as_str()))
    });
    out
}

fn papers_from_svm(
    data: &ServeData,
    conn: &Connection,
    user_id: i64,
    recent_days: Option<i64>,
) -> Vec<Value> {
    let mut out = Vec::new();
    let plist = match data.user_sim.get(&user_id) {
        Some(list) => list,
        None => return out,
    };
    let mut libids: HashSet<String> = HashSet::new();
    if let Ok(mut stmt) = conn.prepare("select paper_id from library where user_id = ?") {
        if let Ok(rows) = stmt.query_map([user_id], |row| row.get::<_, String>(0)) {
            for pid_result in rows.flatten() {
                libids.insert(utils::strip_version(&pid_result));
            }
        }
    }
    for pid in plist {
        if libids.contains(pid) {
            continue;
        }
        if let Some(paper) = data.db.get(pid) {
            out.push(paper.clone());
        }
    }
    if let Some(days) = recent_days {
        let curtime = now_ts();
        out = out
            .into_iter()
            .filter(|paper| {
                paper
                    .get("time_published")
                    .and_then(|v| v.as_i64())
                    .map(|ts| curtime - ts < days * 24 * 60 * 60)
                    .unwrap_or(false)
            })
            .collect();
    }
    out
}

fn papers_filter_version(papers: Vec<Value>, v: &str) -> Vec<Value> {
    if v != "1" {
        return papers;
    }
    papers
        .into_iter()
        .filter(|paper| paper.get("_version").and_then(|v| v.as_i64()).unwrap_or(0) == 1)
        .collect()
}

fn papers_from_topic(data: &ServeData, topic_name: &str, vfilter: &str) -> Vec<Value> {
    if topic_name.is_empty() {
        return vec![];
    }
    let pids = data.topic_pids.get(topic_name).cloned().unwrap_or_default();
    let papers: Vec<Value> = pids
        .iter()
        .filter_map(|pid| data.db.get(pid).cloned())
        .collect();
    papers_filter_version(papers, vfilter)
}

fn encode_json(
    papers: &[Value],
    n: usize,
    thumbs_dir: &Path,
    conn: &Connection,
    user_id: i64,
) -> Vec<Value> {
    let mut libids: HashSet<String> = HashSet::new();
    if let Ok(mut stmt) = conn.prepare("select paper_id from library where user_id = ?") {
        if let Ok(rows) = stmt.query_map([user_id], |row| row.get::<_, String>(0)) {
            for pid_result in rows.flatten() {
                libids.insert(utils::strip_version(&pid_result));
            }
        }
    }
    let mut ret = Vec::new();
    for paper in papers.iter().take(n) {
        let rawid = paper.get("_rawid").and_then(|v| v.as_str()).unwrap_or("");
        let version = paper.get("_version").and_then(|v| v.as_i64()).unwrap_or(0);
        let idvv = format!("{}v{}", rawid, version);
        let mut obj = Map::new();
        obj.insert("title".to_string(), json!(paper.get("title").and_then(|v| v.as_str()).unwrap_or("")));
        obj.insert("pid".to_string(), json!(idvv));
        obj.insert("rawpid".to_string(), json!(rawid));
        obj.insert(
            "category".to_string(),
            json!(
                paper
                    .get("arxiv_primary_category")
                    .and_then(|v| v.get("term"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
            ),
        );
        let authors = paper
            .get("authors")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|a| a.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()))
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        obj.insert("authors".to_string(), json!(authors));
        obj.insert(
            "link".to_string(),
            json!(paper.get("link").and_then(|v| v.as_str()).unwrap_or("")),
        );
        obj.insert(
            "in_library".to_string(),
            json!(if libids.contains(rawid) { 1 } else { 0 }),
        );
        obj.insert(
            "citation_count".to_string(),
            json!(paper.get("citation_count").and_then(|v| v.as_i64())),
        );
        obj.insert(
            "impact_score".to_string(),
            json!(paper.get("impact_score").and_then(|v| v.as_f64())),
        );
        obj.insert(
            "abstract".to_string(),
            json!(paper.get("summary").and_then(|v| v.as_str()).unwrap_or("")),
        );
        let thumb_fname = format!("{}.pdf.jpg", idvv);
        let thumb_path = thumbs_dir.join(&thumb_fname);
        let img = if thumb_path.is_file() {
            format!("/static/thumbs/{}", thumb_fname)
        } else {
            "/static/missing.svg".to_string()
        };
        obj.insert("img".to_string(), json!(img));
        let tags = paper
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.get("term").and_then(|v| v.as_str()).map(|s| s.to_string()))
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        obj.insert("tags".to_string(), json!(tags));
        let repo_links = paper
            .get("repo_links")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.as_str().map(|s| s.to_string()))
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        obj.insert("repo_links".to_string(), json!(repo_links));
        obj.insert(
            "is_opensource".to_string(),
            json!(paper.get("is_opensource").and_then(|v| v.as_bool()).unwrap_or(false)),
        );
        if let Some(updated) = paper.get("updated").and_then(|v| v.as_str()) {
            if let Some(ts) = parse_datetime(updated) {
                obj.insert("published_time".to_string(), json!(format_date_mdy(ts)));
            }
        }
        if let Some(published) = paper.get("published").and_then(|v| v.as_str()) {
            if let Some(ts) = parse_datetime(published) {
                obj.insert(
                    "originally_published_time".to_string(),
                    json!(format_date_mdy(ts)),
                );
            }
        }
        let mut comment = paper
            .get("arxiv_comment")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if comment.len() > 100 {
            comment.truncate(100);
            comment.push_str("...");
        }
        obj.insert("comment".to_string(), json!(comment));
        ret.push(Value::Object(obj));
    }
    ret
}

fn default_context(
    data: &ServeData,
    papers: &[Value],
    conn: &Connection,
    user_id: i64,
    config: &ServeConfig,
    mut kws: HashMap<String, Value>,
) -> Value {
    let top_papers = encode_json(papers, config.num_results, &config.thumbs_dir, conn, user_id);
    let download_settings = load_download_settings(config, &data.topics);
    let download_selected = download_settings.selected_topics.clone();
    let download_dir = config.pdf_dir.canonicalize().unwrap_or(config.pdf_dir.clone());
    let show_prompt = match load_user_setting(conn, user_id, SETTINGS_KEY_SHOW_PROMPT) {
        Ok(Some(value)) if value == "no" => "no".to_string(),
        Ok(Some(value)) if value == "yes" => "yes".to_string(),
        Ok(Some(value)) => value,
        Ok(None) | Err(_) => "yes".to_string(),
    };
    let settings_topics: Vec<Value> = data
        .topics
        .iter()
        .map(|topic| {
            let download_values = download_settings
                .topics
                .get(&topic.name)
                .map(|settings| {
                    json!({
                        "start": settings.start,
                        "end": settings.end,
                        "published": settings.published,
                    })
                })
                .unwrap_or(Value::Null);
            json!({
                "name": topic.name.clone(),
                "display_name": topic.display_name.clone(),
                "selected": download_selected
                    .iter()
                    .any(|t| t == &topic.name),
                "download_values": download_values,
            })
        })
        .collect();
    let settings_storage_papers = data.db.len();
    let settings_storage_papers_display = format!("{:,}", settings_storage_papers);
    let show_prompt = load_show_prompt_preference(config, conn, user_id)
        .unwrap_or_else(|| "yes".to_string());
    let mut ans = json!({
        "papers": top_papers,
        "numresults": papers.len(),
        "totpapers": data.db.len(),
        "tweets": [],
        "msg": "",
        "show_prompt": show_prompt
        "topics": data.topics.iter().map(|topic| {
            json!({
                "name": topic.name.clone(),
                "display_name": topic.display_name.clone(),
                "count": topic.count,
            })
        }).collect::<Vec<Value>>(),
        "selected_topic": "",
        "selected_topic_display": "",
        "selected_topics": [],
        "min_score": "",
        "search_query": "",
        "open_source": false,
        "publication_statuses": [],
        "sort_by": "",
        "sort_order": "desc",
        "no_results_message": "",
        "settings_download_dir": download_dir.to_string_lossy().to_string(),
        "settings_storage_used": get_storage_usage(&download_dir),
        "settings_storage_papers": settings_storage_papers,
        "settings_storage_papers_display": settings_storage_papers_display,
        "settings_download_topics": settings_topics,
        "show_prompt": show_prompt,
        "settings_date_range": {
            "start": data.settings_date_range.start,
            "end": data.settings_date_range.end,
        },
        "settings_topic_stats": data.settings_topic_stats.iter().map(|stat| {
            json!({
                "display_name": stat.display_name.clone(),
                "paper_count": stat.paper_count,
                "paper_count_display": stat.paper_count_display.clone(),
                "citations_total": stat.citations_total,
                "citations_total_display": stat.citations_total_display.clone(),
                "avg_score": stat.avg_score,
                "avg_score_display": stat.avg_score_display.clone(),
            })
        }).collect::<Vec<Value>>(),
    });
    if let Some(obj) = ans.as_object_mut() {
        for (key, value) in kws.drain() {
            obj.insert(key, value);
        }
    }
    ans
}

fn library_count(conn: &Connection, user_id: i64) -> Result<i64, rusqlite::Error> {
    conn.query_row(
        "select count(*) from library where user_id = ?",
        [user_id],
        |row| row.get(0),
    )
}

fn connect_db(path: &Path) -> Result<Connection, rusqlite::Error> {
    Connection::open(path)
}

fn ensure_single_user(conn: &Connection, username: &str) -> Result<(), rusqlite::Error> {
    let exists: Option<i64> = conn
        .query_row(
            "select user_id from user where user_id = ?",
            [SINGLE_USER_ID],
            |row| row.get(0),
        )
        .optional()?;
    if exists.is_some() {
        return Ok(());
    }
    let creation_time = now_ts();
    conn.execute(
        "insert into user (user_id, username, pw_hash, creation_time) values (?, ?, ?, ?)",
        params![SINGLE_USER_ID, username, "local-mode", creation_time],
    )?;
    Ok(())
}

fn format_date(ts: i64) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0)
        .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
    dt.format("%Y-%m-%d").to_string()
}

fn format_date_mdy(ts: i64) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0)
        .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
    format!("{}/{}/{}", dt.month(), dt.day(), dt.year())
}

async fn intmain(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    RawQuery(query): RawQuery,
) -> axum::response::Response {
    let query_map = parse_query(query);
    let vstr = query_map
        .get("vfilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "all".to_string());
    let data = state.data.read().unwrap().clone();
    let papers: Vec<Value> = data
        .date_sorted_pids
        .iter()
        .filter_map(|pid| data.db.get(pid).cloned())
        .collect();
    let papers = papers_filter_version(papers, &vstr);
    match build_context_response(
        &state,
        &env,
        &data,
        &papers,
        json!({
            "render_format": "recent",
            "msg": "Showing most recent Arxiv papers:",
        }),
    ) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn rank(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    AxumPath(request_pid): AxumPath<String>,
) -> axum::response::Response {
    if !utils::is_valid_id(&request_pid) {
        return Html("".to_string()).into_response();
    }
    let data = state.data.read().unwrap().clone();
    let papers = match papers_similar(&data, &request_pid) {
        Ok(papers) => papers,
        Err(err) => return (StatusCode::SERVICE_UNAVAILABLE, err).into_response(),
    };
    match build_context_response(
        &state,
        &env,
        &data,
        &papers,
        json!({
            "render_format": "paper",
        }),
    ) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn search(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    RawQuery(query): RawQuery,
) -> axum::response::Response {
    let query_map = parse_query(query);
    let q = query_map
        .get("q")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_default();
    let mut selected_topics: Vec<String> = Vec::new();
    if let Some(list) = query_map.get("topics") {
        selected_topics.extend(list.iter().cloned());
    }
    if let Some(list) = query_map.get("topics[]") {
        selected_topics.extend(list.iter().cloned());
    }
    let legacy_topic = query_map
        .get("topic")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_default();
    if !legacy_topic.is_empty() && !selected_topics.contains(&legacy_topic) {
        selected_topics.push(legacy_topic.clone());
    }
    let min_score_str = query_map
        .get("min_score")
        .and_then(|vals| vals.first())
        .cloned()
        .or_else(|| {
            query_map
                .get("scorebar")
                .and_then(|vals| vals.first())
                .cloned()
        })
        .unwrap_or_default();
    let open_source_filter = query_map
        .get("open_source")
        .and_then(|vals| vals.first())
        .map(|v| v == "1")
        .unwrap_or(false);
    let publication_statuses = query_map
        .get("publication_status")
        .cloned()
        .unwrap_or_default();
    let sort_by = query_map
        .get("sortby")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_default();
    let sort_order = query_map
        .get("sortorder")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "desc".to_string());
    let min_score = min_score_str.parse::<f64>().ok();

    let data = state.data.read().unwrap().clone();
    let papers = papers_search(
        &data,
        &q,
        &selected_topics,
        min_score,
        open_source_filter,
        &publication_statuses,
        if sort_by.is_empty() { None } else { Some(sort_by.as_str()) },
        &sort_order,
    );
    let mut no_results_message = "".to_string();
    if papers.is_empty() {
        let topics_display = if selected_topics.is_empty() {
            "[]".to_string()
        } else {
            selected_topics.join(", ")
        };
        no_results_message = format!(
            "No paper found for criteria in databases {}",
            topics_display
        );
    }
    let context = json!({
        "render_format": "search",
        "selected_topic": legacy_topic,
        "selected_topic_display": if legacy_topic.is_empty() { "".to_string() } else { translate_topic_name(&legacy_topic).to_string() },
        "selected_topics": selected_topics,
        "min_score": min_score_str,
        "search_query": q,
        "open_source": open_source_filter,
        "publication_statuses": publication_statuses,
        "sort_by": sort_by,
        "sort_order": sort_order,
        "no_results_message": no_results_message,
    });
    match build_context_response(&state, &env, &data, &papers, context) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn topics(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    RawQuery(query): RawQuery,
) -> axum::response::Response {
    let query_map = parse_query(query);
    let topic_name = query_map
        .get("topic")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_default();
    let vstr = query_map
        .get("vfilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "all".to_string());
    let data = state.data.read().unwrap().clone();
    let papers = papers_from_topic(&data, &topic_name, &vstr);
    let selected_topic_display = if topic_name.is_empty() {
        "".to_string()
    } else {
        translate_topic_name(&topic_name).to_string()
    };
    let msg = if topic_name.is_empty() {
        "Select an arXiv topic to browse recent papers.".to_string()
    } else if papers.is_empty() {
        format!(
            "No papers found for topic {} ({}).",
            selected_topic_display, topic_name
        )
    } else {
        format!(
            "Most recent papers in topic {} ({}):",
            selected_topic_display, topic_name
        )
    };
    let context = json!({
        "render_format": "topics",
        "msg": msg,
        "topics": data.topics.iter().map(|topic| {
            json!({
                "name": topic.name.clone(),
                "count": topic.count,
                "display_name": topic.display_name.clone(),
            })
        }).collect::<Vec<Value>>(),
        "selected_topic": topic_name,
        "selected_topic_display": selected_topic_display,
    });
    match build_context_response(&state, &env, &data, &papers, context) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn recommend(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    RawQuery(query): RawQuery,
) -> axum::response::Response {
    let query_map = parse_query(query);
    let ttstr = query_map
        .get("timefilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "week".to_string());
    let vstr = query_map
        .get("vfilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "all".to_string());
    let legend: HashMap<&str, i64> =
        HashMap::from([("day", 1), ("3days", 3), ("week", 7), ("month", 30), ("year", 365)]);
    let tt = legend.get(ttstr.as_str()).copied();

    let data = state.data.read().unwrap().clone();
    let (papers, msg) = match load_context_db(&state) {
        Ok((conn, user_id)) => {
            let papers = papers_from_svm(&data, &conn, user_id, tt);
            let papers = papers_filter_version(papers, &vstr);
            let msg = "Recommended papers: (based on SVM trained on tfidf of papers in your library, refreshed every day or so)".to_string();
            (papers, msg)
        }
        Err(_) => (vec![], "You must be logged in and have some papers saved in your library.".to_string()),
    };
    let context = json!({
        "render_format": "recommend",
        "msg": msg,
    });
    match build_context_response(&state, &env, &data, &papers, context) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn top(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    RawQuery(query): RawQuery,
) -> axum::response::Response {
    let query_map = parse_query(query);
    let ttstr = query_map
        .get("timefilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "week".to_string());
    let vstr = query_map
        .get("vfilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "all".to_string());
    let legend: HashMap<&str, i64> = HashMap::from([
        ("day", 1),
        ("3days", 3),
        ("week", 7),
        ("month", 30),
        ("year", 365),
        ("alltime", 10000),
    ]);
    let tt = legend.get(ttstr.as_str()).copied().unwrap_or(7);
    let curtime = now_ts();
    let data = state.data.read().unwrap().clone();
    let top_sorted_papers: Vec<Value> = data
        .top_sorted_pids
        .iter()
        .filter_map(|pid| data.db.get(pid).cloned())
        .collect();
    let papers = top_sorted_papers
        .into_iter()
        .filter(|paper| {
            paper
                .get("time_published")
                .and_then(|v| v.as_i64())
                .map(|ts| curtime - ts < tt * 24 * 60 * 60)
                .unwrap_or(false)
        })
        .collect::<Vec<Value>>();
    let papers = papers_filter_version(papers, &vstr);
    let context = json!({
        "render_format": "top",
        "msg": "Top papers by OpenAlex recency-adjusted score:",
    });
    match build_context_response(&state, &env, &data, &papers, context) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn toptwtr(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
    RawQuery(query): RawQuery,
) -> axum::response::Response {
    let query_map = parse_query(query);
    let ttstr = query_map
        .get("timefilter")
        .and_then(|vals| vals.first())
        .cloned()
        .unwrap_or_else(|| "day".to_string());
    let data = state.data.read().unwrap().clone();
    let context = json!({
        "render_format": "toptwtr",
        "tweets": [],
        "msg": format!("Top papers mentioned on Twitter over last {}:", ttstr),
    });
    match build_context_response(&state, &env, &data, &[], context) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn library(
    State((state, env)): State<(AppState, Arc<Environment<'static>>)>,
) -> axum::response::Response {
    let data = state.data.read().unwrap().clone();
    let (papers, msg) = match load_context_db(&state) {
        Ok((conn, user_id)) => {
            let papers = papers_from_library(&data, &conn, user_id);
            let ret = encode_json(&papers, 500, &state.config.thumbs_dir, &conn, user_id);
            let msg = format!("{} papers in your library:", ret.len());
            (papers, msg)
        }
        Err(_) => (vec![], "0 papers in your library:".to_string()),
    };
    let context = json!({
        "render_format": "library",
        "msg": msg,
    });
    match build_context_response(&state, &env, &data, &papers, context) {
        Ok(resp) => resp.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn libtoggle(
    State((state, _env)): State<(AppState, Arc<Environment<'static>>)>,
    Form(form): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let pid = match form.get("pid") {
        Some(pid) => pid.clone(),
        None => return Html("NO".to_string()).into_response(),
    };
    if !utils::is_valid_id(&pid) {
        return Html("NO".to_string()).into_response();
    }
    let rawpid = utils::strip_version(&pid);
    let data = state.data.read().unwrap();
    if !data.db.contains_key(&rawpid) {
        return Html("NO".to_string()).into_response();
    }
    let (conn, user_id) = match load_context_db(&state) {
        Ok(ctx) => ctx,
        Err(_) => return Html("NO".to_string()).into_response(),
    };
    let record: Option<i64> = conn
        .query_row(
            "select lib_id from library where user_id = ? and paper_id = ?",
            params![user_id, rawpid],
            |row| row.get(0),
        )
        .optional()
        .unwrap_or(None);
    let ret = if record.is_some() {
        let _ = conn.execute(
            "delete from library where user_id = ? and paper_id = ?",
            params![user_id, rawpid],
        );
        "OFF"
    } else {
        let _ = conn.execute(
            "insert into library (paper_id, user_id, update_time) values (?, ?, ?)",
            params![rawpid, user_id, now_ts()],
        );
        "ON"
    };
    Html(ret.to_string()).into_response()
}

async fn goaway(
    State((_state, _env)): State<(AppState, Arc<Environment<'static>>)>,
) -> impl IntoResponse {
    Html("OK".to_string())
}
async fn health_check(
    State((_state, _env)): State<(AppState, Arc<Environment<'static>>)>,
) -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
    }))
        .into_response()
}

fn normalize_date_input(value: Option<String>) -> Result<Option<String>, String> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if parse_date_string(&trimmed).is_some() {
        Ok(Some(trimmed))
    } else {
        Err(format!("Invalid date value: {}", trimmed))
    }
}

async fn update_download_settings(
    State((state, _env)): State<(AppState, Arc<Environment<'static>>)>,
    Json(payload): Json<DownloadSettingsPayload>,
) -> impl IntoResponse {
    let data = state.data.read().unwrap();
    let valid_topics: HashSet<String> = data
        .topics
        .iter()
        .map(|topic| topic.name.clone())
        .collect();
    for topic in &payload.selected_topics {
        if !valid_topics.contains(topic) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Unknown topic: {}", topic) })),
            )
                .into_response();
        }
    }
    let mut settings = DownloadSettings::default();
    settings.selected_topics = payload.selected_topics;

    for (topic, input) in payload.topics {
        if !valid_topics.contains(&topic) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Unknown topic: {}", topic) })),
            )
                .into_response();
        }
        let start = match normalize_date_input(input.start) {
            Ok(value) => value,
            Err(err) => {
                return (StatusCode::BAD_REQUEST, Json(json!({ "error": err }))).into_response();
            }
        };
        let end = match normalize_date_input(input.end) {
            Ok(value) => value,
            Err(err) => {
                return (StatusCode::BAD_REQUEST, Json(json!({ "error": err }))).into_response();
            }
        };
        if let (Some(start_date), Some(end_date)) = (
            start.as_deref().and_then(parse_date_string),
            end.as_deref().and_then(parse_date_string),
        ) {
            if start_date > end_date {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": format!("Start date cannot be after end date for {}", topic)
                    })),
                )
                    .into_response();
            }
        }
        settings.topics.insert(
            topic,
            DownloadTopicSettings {
                start,
                end,
                published: input.published.unwrap_or(false),
            },
        );
    }

    if let Err(err) = persist_download_settings(&state.config, &settings) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": err })),
        )
            .into_response();
    }

    Json(json!({ "ok": true })).into_response()
}

async fn ingest_status(
    State((state, _env)): State<(AppState, Arc<Environment<'static>>)>,
    AxumPath(job_id): AxumPath<String>,
) -> impl IntoResponse {
    let job = get_ingest_job(&state, &job_id);
    match job {
        Some(job) => Json(job).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Unknown job id" })),
        )
            .into_response(),
    }
}

async fn ingest_recompute_status(
    State((state, _env)): State<(AppState, Arc<Environment<'static>>)>,
) -> impl IntoResponse {
    Json(get_recompute_status(&state)).into_response()
}

async fn recompute_status_endpoint(
    State((state, _env)): State<(AppState, Arc<Environment<'static>>)>,
) -> impl IntoResponse {
    let status = get_recompute_status(&state);
    Json(json!({
        "status": status.status,
        "message": status.message,
        "percent": status.percent,
        "updated_at": status.updated_at,
        "error": status.error,
    }))
    .into_response()
}

async fn ingest_arxiv(
    State((state, _env)): State<(AppState, Arc<Environment<'static>>)>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let paper_id = form.get("paper_id").cloned().unwrap_or_default().trim().to_string();
    let wants_json = headers
        .get("X-Requested-With")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "XMLHttpRequest")
        .unwrap_or(false)
        || headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("application/json"))
            .unwrap_or(false);

    if paper_id.is_empty() {
        if wants_json {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Please enter an arXiv identifier to add." })),
            )
                .into_response();
        }
        return Redirect::temporary("/").into_response();
    }
    if !utils::is_valid_id(&paper_id) {
        if wants_json {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Please provide a valid arXiv identifier, e.g., 1512.08756v2." })),
            )
                .into_response();
        }
        return Redirect::temporary("/").into_response();
    }

    let job_id = init_ingest_job(&state, &paper_id);
    let job_id_clone = job_id.clone();
    let state_clone = state.clone();
    thread::spawn(move || {
        run_ingest_job(&state_clone, &job_id_clone, &paper_id);
    });

    if wants_json {
        return Json(json!({ "job_id": job_id })).into_response();
    }

    Redirect::temporary("/").into_response()
}

fn load_context_db(state: &AppState) -> Result<(Connection, i64), String> {
    let conn = connect_db(&state.config.database_path).map_err(|e| e.to_string())?;
    ensure_single_user(&conn, &state.config.single_user_name).map_err(|e| e.to_string())?;
    Ok((conn, SINGLE_USER_ID))
}

fn build_context_response(
    state: &AppState,
    env: &Environment<'static>,
    data: &ServeData,
    papers: &[Value],
    extra: Value,
) -> Result<Html<String>, String> {
    let (conn, user_id) = load_context_db(state)?;
    let mut extra_map = HashMap::new();
    if let Some(obj) = extra.as_object() {
        for (key, value) in obj {
            extra_map.insert(key.clone(), value.clone());
        }
    }
    let ctx = default_context(data, papers, &conn, user_id, &state.config, extra_map);
    let template = env.get_template("main.html").map_err(|e| e.to_string())?;
    let rendered = template.render(&ctx).map_err(|e| e.to_string())?;
    Ok(Html(rendered))
}

fn parse_query(raw: Option<String>) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    let Some(raw) = raw else { return map };
    for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
        map.entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    map
}

fn ingest_job_path(config: &ServeConfig, job_id: &str) -> PathBuf {
    config.ingest_jobs_dir.join(format!("{}.json", job_id))
}

fn persist_ingest_job(config: &ServeConfig, job: &IngestJob) {
    let _ = fs::create_dir_all(&config.ingest_jobs_dir);
    if let Ok(serialized) = serde_json::to_vec(job) {
        let path = ingest_job_path(config, &job.job_id);
        let _ = utils::write_atomic_bytes(&path, &serialized);
    }
}

fn load_ingest_job(config: &ServeConfig, job_id: &str) -> Option<IngestJob> {
    let path = ingest_job_path(config, job_id);
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn init_ingest_job(state: &AppState, paper_id: &str) -> String {
    let job_id = uuid::Uuid::new_v4().to_string().replace('-', "");
    let now = now_ts();
    let job = IngestJob {
        job_id: job_id.clone(),
        paper_id: paper_id.to_string(),
        label: "Preparing to start the ingest process...".to_string(),
        percent: 0,
        messages: vec![],
        done: false,
        error: false,
        status: "running".to_string(),
        warning: false,
        created_at: now,
        updated_at: now,
    };
    {
        let mut jobs = state.ingest_jobs.lock().unwrap();
        jobs.insert(job_id.clone(), job.clone());
    }
    persist_ingest_job(&state.config, &job);
    job_id
}

fn update_ingest_job(
    state: &AppState,
    job_id: &str,
    label: &str,
    percent: i64,
    message: Option<String>,
    done: bool,
    error: bool,
    warning: bool,
) {
    let mut jobs = state.ingest_jobs.lock().unwrap();
    let mut job = jobs
        .get(job_id)
        .cloned()
        .or_else(|| load_ingest_job(&state.config, job_id))
        .unwrap_or_else(|| IngestJob {
            job_id: job_id.to_string(),
            paper_id: "".to_string(),
            label: label.to_string(),
            percent,
            messages: vec![],
            done: false,
            error: false,
            status: "running".to_string(),
            warning: false,
            created_at: now_ts(),
            updated_at: now_ts(),
        });
    job.label = label.to_string();
    job.percent = percent;
    if let Some(msg) = message {
        job.messages.push(msg);
    }
    if done {
        job.done = true;
    }
    if error {
        job.error = true;
        job.done = true;
    }
    if warning {
        job.warning = true;
    }
    job.status = if job.error {
        "error".to_string()
    } else if job.warning {
        "warning".to_string()
    } else if job.done {
        "done".to_string()
    } else {
        "running".to_string()
    };
    job.updated_at = now_ts();
    jobs.insert(job_id.to_string(), job.clone());
    persist_ingest_job(&state.config, &job);
}

fn get_ingest_job(state: &AppState, job_id: &str) -> Option<IngestJob> {
    let jobs = state.ingest_jobs.lock().unwrap();
    jobs.get(job_id)
        .cloned()
        .or_else(|| load_ingest_job(&state.config, job_id))
}

fn recompute_status_paths(config: &ServeConfig) -> (PathBuf, PathBuf, PathBuf) {
    (
        config
            .ingest_jobs_dir
            .join("recompute_status.json"),
        config.ingest_jobs_dir.join("recompute_stdout.log"),
        config.ingest_jobs_dir.join("recompute_stderr.log"),
    )
}

fn persist_recompute_status(config: &ServeConfig, status: &RecomputeStatus) {
    let _ = fs::create_dir_all(&config.ingest_jobs_dir);
    if let Ok(serialized) = serde_json::to_vec(status) {
        let path = recompute_status_paths(config).0;
        let _ = utils::write_atomic_bytes(&path, &serialized);
    }
}

fn load_recompute_status(config: &ServeConfig) -> Option<RecomputeStatus> {
    let path = recompute_status_paths(config).0;
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn get_recompute_status(state: &AppState) -> RecomputeStatus {
    let mut status_lock = state.recompute_status.lock().unwrap();
    let mut status = status_lock
        .clone()
        .or_else(|| load_recompute_status(&state.config))
        .unwrap_or_else(|| {
            let (_status_path, stdout_path, stderr_path) = recompute_status_paths(&state.config);
            RecomputeStatus {
                status: "idle".to_string(),
                updated_at: now_ts(),
                percent: json!(0),
                stdout_path: stdout_path.to_string_lossy().to_string(),
                stderr_path: stderr_path.to_string_lossy().to_string(),
                message: None,
                error: None,
            }
        });
    let now = now_ts();
    let thread_active = state
        .recompute_thread
        .lock()
        .unwrap()
        .as_ref()
        .map(|h| !h.is_finished())
        .unwrap_or(false);
    if status.status == "running" || status.status == "queued" {
        let stale = now - status.updated_at > RECOMPUTE_STALE_SECONDS;
        if stale && !thread_active {
            status.status = "idle".to_string();
            status.updated_at = now;
            status.message = None;
            status.percent = json!(0);
            status.error = None;
        }
    }
    *status_lock = Some(status.clone());
    persist_recompute_status(&state.config, &status);
    status
}

fn update_recompute_status(
    state: &AppState,
    status_value: &str,
    message: Option<String>,
    error: Option<String>,
    percent: Option<Value>,
) {
    let mut status_lock = state.recompute_status.lock().unwrap();
    let (_status_path, stdout_path, stderr_path) = recompute_status_paths(&state.config);
    let mut status = status_lock
        .clone()
        .unwrap_or(RecomputeStatus {
            status: "idle".to_string(),
            updated_at: now_ts(),
            percent: json!(0),
            stdout_path: stdout_path.to_string_lossy().to_string(),
            stderr_path: stderr_path.to_string_lossy().to_string(),
            message: None,
            error: None,
        });
    status.status = status_value.to_string();
    status.updated_at = now_ts();
    if ["idle", "skipped", "disabled"].contains(&status_value) {
        status.message = None;
        status.percent = json!(0);
    } else {
        if let Some(msg) = message {
            status.message = Some(msg);
        }
        if let Some(pct) = percent {
            status.percent = pct;
        }
    }
    if let Some(err) = error {
        status.error = Some(err);
    }
    *status_lock = Some(status.clone());
    persist_recompute_status(&state.config, &status);
}

fn run_recompute_job(state: &AppState) {
    let (_status_path, stdout_path, stderr_path) = recompute_status_paths(&state.config);
    let _ = fs::create_dir_all(&state.config.ingest_jobs_dir);
    update_recompute_status(
        state,
        "running",
        Some("Recomputing caches".to_string()),
        None,
        Some(json!("—")),
    );
    let mut stdout_handle = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stdout_path)
        .ok();
    let mut stderr_handle = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_path)
        .ok();
    let now = chrono::Local::now().to_rfc2822();
    if let Some(ref mut handle) = stdout_handle {
        let _ = writeln!(handle, "[{}] Starting recompute", now);
    }
    if let Some(ref mut handle) = stderr_handle {
        let _ = writeln!(handle, "[{}] Starting recompute", now);
    }
    let steps = [("analyze.py", true), ("buildsvm.py", false), ("make_cache.py", true)];
    for (script_name, required) in steps {
        update_recompute_status(
            state,
            "running",
            Some(format!("Running {}", script_name)),
            None,
            Some(json!("—")),
        );
        if let Some(ref mut handle) = stdout_handle {
            let _ = writeln!(handle, "[{}] Running {}", now, script_name);
        }
        if let Some(ref mut handle) = stderr_handle {
            let _ = writeln!(handle, "[{}] Running {}", now, script_name);
        }
        let mut returncode = 0;
        if script_name == "make_cache.py" {
            let mut cmd = Command::new(python_executable());
            cmd.arg(script_name)
                .stdout(Stdio::piped())
                .stderr(
                    stderr_handle
                        .as_ref()
                        .and_then(|h| h.try_clone().ok())
                        .map(Stdio::from)
                        .unwrap_or(Stdio::null()),
                );
            if let Ok(mut child) = cmd.spawn() {
                if let Some(stdout) = child.stdout.take() {
                    let reader = BufReader::new(stdout);
                    let percent_pattern = Regex::new(r"(\d{1,3})%").unwrap();
                    for line in reader.lines().flatten() {
                        if let Some(ref mut handle) = stdout_handle {
                            let _ = writeln!(handle, "{}", line);
                        }
                        if let Some(caps) = percent_pattern.captures(&line) {
                            if let Ok(percent) = caps[1].parse::<i64>() {
                                if (0..=100).contains(&percent) {
                                    update_recompute_status(
                                        state,
                                        "running",
                                        Some(format!("Running {}", script_name)),
                                        None,
                                        Some(json!(percent)),
                                    );
                                }
                            }
                        }
                    }
                }
                if let Ok(status) = child.wait() {
                    returncode = status.code().unwrap_or(1);
                }
            } else {
                returncode = 1;
            }
        } else {
            let status = Command::new(python_executable())
                .arg(script_name)
                .stdout(
                    stdout_handle
                        .as_ref()
                        .and_then(|h| h.try_clone().ok())
                        .map(Stdio::from)
                        .unwrap_or(Stdio::null()),
                )
                .stderr(
                    stderr_handle
                        .as_ref()
                        .and_then(|h| h.try_clone().ok())
                        .map(Stdio::from)
                        .unwrap_or(Stdio::null()),
                )
                .status();
            returncode = status.ok().and_then(|s| s.code()).unwrap_or(1);
        }
        if returncode != 0 && required {
            update_recompute_status(
                state,
                "failed",
                Some("Recompute failed".to_string()),
                Some(format!("{} failed", script_name)),
                None,
            );
            let mut handle = state.recompute_thread.lock().unwrap();
            *handle = None;
            return;
        }
        update_recompute_status(
            state,
            "running",
            Some(format!("Finished {}", script_name)),
            None,
            Some(json!("—")),
        );
    }
    if let Some(ref mut handle) = stdout_handle {
        let _ = writeln!(handle, "[{}] Recompute finished", now);
    }
    if let Some(ref mut handle) = stderr_handle {
        let _ = writeln!(handle, "[{}] Recompute finished", now);
    }
    if let Ok(new_data) = refresh_serving_data(&state.config) {
        let mut data = state.data.write().unwrap();
        *data = new_data;
    }
    update_recompute_status(
        state,
        "finished",
        Some("Recompute finished successfully".to_string()),
        None,
        Some(json!(100)),
    );
    let mut handle = state.recompute_thread.lock().unwrap();
    *handle = None;
}

fn run_ingest_job(state: &AppState, job_id: &str, paper_id: &str) {
    update_ingest_job(
        state,
        job_id,
        "Starting ingest...",
        1,
        Some("Starting ingest".to_string()),
        false,
        false,
        false,
    );
    let mut cmd = Command::new(python_executable());
    cmd.arg("ingest_single_paper.py")
        .arg("--no-recompute")
        .arg(paper_id)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut error = false;
    let warning_flag = Arc::new(AtomicBool::new(false));
    if let Ok(mut child) = cmd.spawn() {
        let mut stdout_thread = None;
        let mut stderr_thread = None;
        if let Some(stdout) = child.stdout.take() {
            let state = state.clone();
            let job_id = job_id.to_string();
            let warning_flag = Arc::clone(&warning_flag);
            stdout_thread = Some(thread::spawn(move || {
                let reader = BufReader::new(stdout);
                for line in reader.lines().flatten() {
                    let warning = warning_flag.load(Ordering::Relaxed);
                    update_ingest_job(
                        &state,
                        &job_id,
                        "Ingest running...",
                        50,
                        Some(line),
                        false,
                        false,
                        warning,
                    );
                }
            }));
        }
        if let Some(stderr) = child.stderr.take() {
            let state = state.clone();
            let job_id = job_id.to_string();
            let warning_flag = Arc::clone(&warning_flag);
            stderr_thread = Some(thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().flatten() {
                    warning_flag.store(true, Ordering::Relaxed);
                    update_ingest_job(
                        &state,
                        &job_id,
                        "Ingest running...",
                        50,
                        Some(line),
                        false,
                        false,
                        true,
                    );
                }
            }));
        }
        if let Ok(status) = child.wait() {
            if !status.success() {
                error = true;
            }
        } else {
            error = true;
        }
        if let Some(handle) = stdout_thread {
            let _ = handle.join();
        }
        if let Some(handle) = stderr_thread {
            let _ = handle.join();
        }
    } else {
        error = true;
    }
    let warning = warning_flag.load(Ordering::Relaxed);
    if error {
        update_ingest_job(
            state,
            job_id,
            "Ingest failed",
            100,
            Some("Ingest failed".to_string()),
            true,
            true,
            warning,
        );
        return;
    }
    update_ingest_job(
        state,
        job_id,
        "Ingest complete",
        100,
        Some("Ingest complete".to_string()),
        true,
        false,
        warning,
    );
    if let Err(err) = refresh_hnsw_in_state(state) {
        eprintln!("Failed to refresh HNSW index: {}", err);
    }
    let recompute_state = get_recompute_status(state);
    if recompute_state.status == "running" {
        update_ingest_job(
            state,
            job_id,
            "Recompute already running...",
            90,
            Some("Existing cache recompute is still running.".to_string()),
            true,
            false,
            warning,
        );
        return;
    }
    update_ingest_job(
        state,
        job_id,
        "Queued recompute...",
        90,
        Some("Cache recompute will run in the background.".to_string()),
        true,
        false,
        warning,
    );
    update_recompute_status(
        state,
        "queued",
        Some("Recompute queued".to_string()),
        None,
        Some(json!("—")),
    );
    let state_clone = state.clone();
    let mut handle = state.recompute_thread.lock().unwrap();
    *handle = Some(thread::spawn(move || {
        run_recompute_job(&state_clone);
    }));
}

fn python_executable() -> String {
    std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string())
}

static TOPIC_TRANSLATIONS: once_cell::sync::Lazy<HashMap<&'static str, &'static str>> =
    once_cell::sync::Lazy::new(|| {
        HashMap::from([
            ("astro-ph.CO", "Cosmology and Nongalactic Astrophysics"),
            ("astro-ph.GA", "Astrophysics of Galaxies"),
            ("astro-ph.HE", "High Energy Astrophysical Phenomena"),
            ("astro-ph.IM", "Instrumentation and Methods for Astrophysics"),
            ("cond-mat.dis-nn", "Disordered Systems and Neural Networks"),
            ("cond-mat.mes-hall", "Mesoscale and Nanoscale Physics"),
            ("cond-mat.mtrl-sci", "Materials Science"),
            ("cond-mat.quant-gas", "Quantum Gases"),
            ("cond-mat.soft", "Soft Condensed Matter"),
            ("cond-mat.stat-mech", "Statistical Mechanics"),
            ("cond-mat.str-el", "Strongly Correlated Electrons"),
            ("cs.AI", "Artificial Intelligence"),
            ("cs.AR", "Hardware Architecture"),
            ("cs.CC", "Computational Complexity"),
            ("cs.CE", "Computational Engineering, Finance, and Science"),
            ("cs.CG", "Computational Geometry"),
            ("cs.CL", "Computation and Language"),
            ("cs.CR", "Cryptography and Security"),
            ("cs.CV", "Computer Vision and Pattern Recognition"),
            ("cs.CY", "Computers and Society"),
            ("cs.DB", "Databases"),
            ("cs.DC", "Distributed, Parallel, and Cluster Computing"),
            ("cs.DL", "Digital Libraries"),
            ("cs.DM", "Discrete Mathematics"),
            ("cs.DS", "Data Structures and Algorithms"),
            ("cs.ET", "Emerging Technologies"),
            ("cs.FL", "Formal Languages and Automata Theory"),
            ("cs.GR", "Graphics"),
            ("cs.GT", "Computer Science and Game Theory"),
            ("cs.HC", "Human-Computer Interaction"),
            ("cs.IR", "Information Retrieval"),
            ("cs.IT", "Information Theory"),
            ("cs.LG", "Machine Learning"),
            ("cs.LO", "Logic in Computer Science"),
            ("cs.MA", "Multiagent Systems"),
            ("cs.MM", "Multimedia"),
            ("cs.MS", "Mathematical Software"),
            ("cs.NE", "Neural and Evolutionary Computing"),
            ("cs.NI", "Networking and Internet Architecture"),
            ("cs.OH", "Other Computer Science"),
            ("cs.OS", "Operating Systems"),
            ("cs.PF", "Performance"),
            ("cs.PL", "Programming Languages"),
            ("cs.RO", "Robotics"),
            ("cs.SD", "Sound"),
            ("cs.SE", "Software Engineering"),
            ("cs.SI", "Social and Information Networks"),
            ("econ.EM", "Econometrics"),
            ("econ.GN", "General Economics"),
            ("econ.TH", "Theoretical Economics"),
            ("eess.AS", "Audio and Speech Processing"),
            ("eess.IV", "Image and Video Processing"),
            ("eess.SP", "Signal Processing"),
            ("eess.SY", "Systems and Control"),
            ("gr-qc", "General Relativity and Quantum Cosmology"),
            ("hep-ex", "High Energy Physics - Experiment"),
            ("hep-lat", "High Energy Physics - Lattice"),
            ("hep-ph", "High Energy Physics - Phenomenology"),
            ("hep-th", "High Energy Physics - Theory"),
            ("math-ph", "Mathematical Physics"),
            ("math.AC", "Commutative Algebra"),
            ("math.AG", "Algebraic Geometry"),
            ("math.AP", "Analysis of PDEs"),
            ("math.CA", "Classical Analysis and ODEs"),
            ("math.CO", "Combinatorics"),
            ("math.CT", "Category Theory"),
            ("math.DG", "Differential Geometry"),
            ("math.DS", "Dynamical Systems"),
            ("math.FA", "Functional Analysis"),
            ("math.GM", "General Mathematics"),
            ("math.GT", "Geometric Topology"),
            ("math.HO", "History and Overview"),
            ("math.MG", "Metric Geometry"),
            ("math.NA", "Numerical Analysis"),
            ("math.OC", "Optimization and Control"),
            ("math.PR", "Probability"),
            ("math.QA", "Quantum Algebra"),
            ("math.RA", "Rings and Algebras"),
            ("math.ST", "Statistics Theory"),
            ("nlin.AO", "Adaptation and Self-Organizing Systems"),
            ("nlin.CD", "Chaotic Dynamics"),
            ("nlin.CG", "Cellular Automata and Lattice Gases"),
            ("nlin.PS", "Pattern Formation and Solitons"),
            ("nucl-th", "Nuclear Theory"),
            ("physics.acc-ph", "Accelerator Physics"),
            ("physics.ao-ph", "Atmospheric and Oceanic Physics"),
            ("physics.bio-ph", "Biological Physics"),
            ("physics.chem-ph", "Chemical Physics"),
            ("physics.comp-ph", "Computational Physics"),
            ("physics.data-an", "Data Analysis, Statistics and Probability"),
            ("physics.ed-ph", "Physics Education"),
            ("physics.flu-dyn", "Fluid Dynamics"),
            ("physics.geo-ph", "Geophysics"),
            ("physics.hist-ph", "History and Philosophy of Physics"),
            ("physics.ins-det", "Instrumentation and Detectors"),
            ("physics.med-ph", "Medical Physics"),
            ("physics.optics", "Optics"),
            ("physics.plasm-ph", "Plasma Physics"),
            ("physics.soc-ph", "Physics and Society"),
            ("physics.space-ph", "Space Physics"),
            ("q-bio.BM", "Biomolecules"),
            ("q-bio.CB", "Cell Behavior"),
            ("q-bio.GN", "Genomics"),
            ("q-bio.MN", "Molecular Networks"),
            ("q-bio.NC", "Neurons and Cognition"),
            ("q-bio.PE", "Populations and Evolution"),
            ("q-bio.QM", "Quantitative Methods"),
            ("q-bio.SC", "Subcellular Processes"),
            ("q-bio.TO", "Tissues and Organs"),
            ("q-fin.CP", "Computational Finance"),
            ("q-fin.GN", "General Finance"),
            ("q-fin.MF", "Mathematical Finance"),
            ("q-fin.PM", "Portfolio Management"),
            ("q-fin.PR", "Pricing of Securities"),
            ("q-fin.RM", "Risk Management"),
            ("q-fin.ST", "Statistical Finance"),
            ("q-fin.TR", "Trading and Market Microstructure"),
            ("quant-ph", "Quantum Physics"),
            ("stat.AP", "Applications"),
            ("stat.CO", "Computation"),
            ("stat.ME", "Methodology"),
            ("stat.ML", "Machine Learning"),
        ])
    });
