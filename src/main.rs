use clap::{Args, Parser, Subcommand};
use feed_rs::parser;
use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use regex::Regex;
use reqwest::{StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smartcore::linalg::basic::matrix::DenseMatrix;
use smartcore::svm::svc::{SVCParameters, SVC};
use smartcore::svm::Kernels;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use stop_words::{get, LANGUAGE};
use wait_timeout::ChildExt;

use arxiv_sanity_pipeline::cache;
use arxiv_sanity_pipeline::utils;
use crate::hnsw_index::HnswIndex;

mod db;
mod download;
mod hnsw_index;
mod ingest;
mod migrate;
mod serve;
mod twitter;

const DEFAULT_SEARCH_QUERY: &str = "cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML+OR+cat:cs.IT+OR+cat:eess.SP+OR+cat:q-fin.TR+OR+cat:q-fin.RM+OR+cat:q-fin.ST+OR+cat:cond-mat.stat-mech";
const DEFAULT_CATEGORY_COUNTS: &str = "cs.AI=2000,cs.LG=2000,stat.ML=2000,cs.IT=1500,eess.SP=1500,cs.NE=1000,cs.CL=1000,cs.CV=1500,cond-mat.stat-mech=500,q-fin.TR=2000,q-fin.RM=2000,q-fin.ST=2000";
const DEFAULT_MISSING_THUMB: &str = "static/missing.svg";
const DEFAULT_OUTPUT_DIR: &str = ".pipeline";
const DEFAULT_TFIDF_META_OUT: &str = ".pipeline/tfidf_meta.json";
const DEFAULT_USER_SIM_OUT: &str = ".pipeline/user_sim.json";
const DEFAULT_DB_JSONL_OUT: &str = ".pipeline/db.jsonl";
const OPENALEX_WORKS_ENDPOINT: &str = "https://api.openalex.org/works";
const MAX_TRAIN_DOCS: usize = 5000;
const MAX_FEATURES: usize = 5000;
const MIN_TEXT_LEN: usize = 1000;
const MAX_TEXT_LEN: usize = 500_000;
const NUM_RECOMMENDATIONS: usize = 1000;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PipelineConfig {
    output_dir: String,
    db_path: String,
    pdf_dir: String,
    txt_dir: String,
    thumb_dir: String,
    tmp_dir: String,
    database_path: String,
    tfidf_path: String,
    tfidf_meta_path: String,
    hnsw_index_path: String,
    user_sim_path: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        let output_dir = default_output_dir();
        Self {
            output_dir: output_dir.clone(),
            db_path: output_path(&output_dir, "db.jsonl"),
            pdf_dir: output_path(&output_dir, "pdf"),
            txt_dir: output_path(&output_dir, "txt"),
            thumb_dir: output_path(&output_dir, "thumb"),
            tmp_dir: output_path(&output_dir, "tmp"),
            database_path: output_path(&output_dir, "as.db"),
            tfidf_path: output_path(&output_dir, "tfidf.bin"),
            tfidf_meta_path: output_path(&output_dir, "tfidf_meta.json"),
            hnsw_index_path: output_path(&output_dir, "hnsw_index.bin"),
            user_sim_path: output_path(&output_dir, "user_sim.bin"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PipelineConfigFile {
    #[serde(default)]
    output_dir: Option<String>,
    #[serde(default)]
    db_path: Option<String>,
    #[serde(default)]
    pdf_dir: Option<String>,
    #[serde(default)]
    txt_dir: Option<String>,
    #[serde(default)]
    thumb_dir: Option<String>,
    #[serde(default)]
    tmp_dir: Option<String>,
    #[serde(default)]
    database_path: Option<String>,
    #[serde(default)]
    tfidf_path: Option<String>,
    #[serde(default)]
    tfidf_meta_path: Option<String>,
    #[serde(default)]
    hnsw_index_path: Option<String>,
    #[serde(default)]
    user_sim_path: Option<String>,
}

fn default_output_dir() -> String {
    DEFAULT_OUTPUT_DIR.to_string()
}

fn output_path(output_dir: &str, entry: &str) -> String {
    Path::new(output_dir)
        .join(entry)
        .to_string_lossy()
        .to_string()
}

impl PipelineConfig {
    fn from_file(config: PipelineConfigFile) -> Self {
        let output_dir = config.output_dir.unwrap_or_else(default_output_dir);
        Self {
            output_dir: output_dir.clone(),
            db_path: config
                .db_path
                .unwrap_or_else(|| output_path(&output_dir, "db.jsonl")),
            pdf_dir: config
                .pdf_dir
                .unwrap_or_else(|| output_path(&output_dir, "pdf")),
            txt_dir: config
                .txt_dir
                .unwrap_or_else(|| output_path(&output_dir, "txt")),
            thumb_dir: config
                .thumb_dir
                .unwrap_or_else(|| output_path(&output_dir, "thumb")),
            tmp_dir: config
                .tmp_dir
                .unwrap_or_else(|| output_path(&output_dir, "tmp")),
            database_path: config
                .database_path
                .unwrap_or_else(|| output_path(&output_dir, "as.db")),
            tfidf_path: config
                .tfidf_path
                .unwrap_or_else(|| output_path(&output_dir, "tfidf.bin")),
            tfidf_meta_path: config
                .tfidf_meta_path
                .unwrap_or_else(|| output_path(&output_dir, "tfidf_meta.json")),
            hnsw_index_path: config
                .hnsw_index_path
                .unwrap_or_else(|| output_path(&output_dir, "hnsw_index.bin")),
            user_sim_path: config
                .user_sim_path
                .unwrap_or_else(|| output_path(&output_dir, "user_sim.bin")),
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "arxiv_sanity_pipeline",
    version,
    about = "Rust CLI wrapper for the arxiv-sanity processing pipeline"
)]
struct Cli {
    #[arg(long, global = true, default_value = "pipeline_config.json")]
    config: PathBuf,
    #[arg(
        long,
        global = true,
        help = "Write the resolved pipeline config to disk before running"
    )]
    write_config: bool,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(name = "fetch-papers", alias = "fetch_papers")]
    FetchPapers(FetchArgs),
    #[command(name = "download-pdfs", alias = "download_pdfs")]
    DownloadPdfs(DownloadArgs),
    #[command(name = "parse-pdf-to-text", alias = "parse_pdf_to_text")]
    ParsePdfToText(ParsePdfArgs),
    #[command(name = "thumb-pdf", alias = "thumb_pdf")]
    ThumbPdf(ThumbArgs),
    #[command(name = "analyze")]
    Analyze,
    #[command(name = "buildsvm")]
    BuildSvm,
    #[command(name = "make-cache", alias = "make_cache")]
    MakeCache,
    #[command(name = "ingest-single-paper", alias = "ingest_single_paper")]
    IngestSinglePaper(IngestSinglePaperArgs),
    #[command(name = "migrate-analysis", alias = "migrate_analysis")]
    MigrateAnalysis(MigrateAnalysisArgs),
    #[command(name = "migrate-db", alias = "migrate_db")]
    MigrateDb(MigrateDbArgs),
    #[command(name = "serve")]
    Serve(ServeCommandArgs),
    #[command(name = "twitter-daemon", alias = "twitter_daemon")]
    TwitterDaemon(TwitterDaemonArgs),
    #[command(name = "run-all", alias = "run_all")]
    RunAll(RunAllArgs),
    #[command(name = "reset-pipeline", alias = "reset_pipeline")]
    ResetPipeline(ResetPipelineArgs),
}

#[derive(Args, Debug, Clone)]
struct FetchArgs {
    #[arg(long, default_value = DEFAULT_SEARCH_QUERY)]
    search_query: String,
    #[arg(long, default_value_t = 0)]
    start_index: i32,
    #[arg(long, default_value_t = 10000)]
    max_index: i32,
    #[arg(long, default_value_t = 500)] //arXiv api max slice 2k; be gentle with it, API & Database are free, must respect
    results_per_iteration: i32,
    #[arg(long, default_value_t = 5.0)]
    wait_time: f64,
    #[arg(long, default_value_t = 1)]
    break_on_no_added: i32,
    #[arg(long, default_value = DEFAULT_CATEGORY_COUNTS)]
    category_counts: String,
}

#[derive(Args, Debug, Clone)]
struct RunAllArgs {
    #[command(flatten)]
    fetch: FetchArgs,
}

#[derive(Args, Debug, Clone)]
struct ResetPipelineArgs {
    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Allow deletion of pipeline outputs and cached data."
    )]
    force: bool,
}

#[derive(Args, Debug, Clone)]
struct ParsePdfArgs {
    #[arg(long, help = "Directory containing input PDFs")]
    pdf_dir: Option<PathBuf>,
    #[arg(long, help = "Directory for output text files")]
    txt_dir: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
struct ThumbArgs {
    #[arg(long, help = "Directory containing input PDFs")]
    pdf_dir: Option<PathBuf>,
    #[arg(long, help = "Directory for output thumbnails")]
    thumb_dir: Option<PathBuf>,
    #[arg(long, help = "Directory for temporary thumbnail files")]
    tmp_dir: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
struct DownloadArgs {}

#[derive(Args, Debug, Clone)]
struct IngestSinglePaperArgs {
    #[arg(value_name = "paper_id", help = "arXiv identifier, e.g., 1512.08756v2")]
    paper_id: Option<String>,
    #[arg(
        long = "no-recompute",
        action = clap::ArgAction::SetTrue,
        help = "Skip recomputing caches (useful when triggering recompute separately)."
    )]
    no_recompute: bool,
}

#[derive(Args, Debug, Clone)]
struct MigrateAnalysisArgs {
    #[arg(long = "tfidf-meta-in", default_value = "tfidf_meta.p")]
    tfidf_meta_in: PathBuf,
    #[arg(long = "tfidf-meta-out", default_value = DEFAULT_TFIDF_META_OUT)]
    tfidf_meta_out: PathBuf,
    #[arg(long = "user-sim-in", default_value = "user_sim.p")]
    user_sim_in: PathBuf,
    #[arg(long = "user-sim-out", default_value = DEFAULT_USER_SIM_OUT)]
    user_sim_out: PathBuf,
    #[arg(long = "allow-missing", action = clap::ArgAction::SetTrue)]
    allow_missing: bool,
}

#[derive(Args, Debug, Clone)]
struct MigrateDbArgs {
    #[arg(long, default_value = "db.p")]
    input: PathBuf,
    #[arg(long, default_value = DEFAULT_DB_JSONL_OUT)]
    output: PathBuf,
}

#[derive(Args, Debug, Clone)]
struct ServeCommandArgs {
    #[arg(
        short = 'p',
        long = "prod",
        action = clap::ArgAction::SetTrue,
        help = "run in prod?"
    )]
    prod: bool,
    #[arg(
        short = 'r',
        long = "num_results",
        default_value_t = 200,
        help = "number of results to return per query"
    )]
    num_results: usize,
    #[arg(
        long = "port",
        default_value_t = 5000,
        help = "port to serve on"
    )]
    port: u16,
}

#[derive(Args, Debug, Clone)]
struct TwitterDaemonArgs {
    #[arg(
        long = "query",
        default_value = "arxiv.org",
        help = "Search query to use for the Twitter API"
    )]
    query: String,
    #[arg(
        long = "sleep-seconds",
        default_value_t = 600,
        help = "Time to wait between polling Twitter"
    )]
    sleep_seconds: u64,
    #[arg(
        long = "max-results",
        default_value_t = 100,
        help = "Maximum results to request per Twitter API call"
    )]
    max_results: u32,
    #[arg(
        long = "output",
        default_value = "twitter_recent.jsonl",
        help = "Path to write tweet metadata"
    )]
    output: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Paper {
    id: String,
    version: i32,
    title: String,
    authors: Vec<String>,
    #[serde(rename = "abstract")]
    abstract_text: String,
    updated: String,
    categories: Vec<String>,
    #[serde(default)]
    citation_count: Option<i64>,
    #[serde(default)]
    is_accepted: Option<bool>,
    #[serde(default)]
    is_published: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TfidfMatrix {
    vectors: Vec<Vec<f32>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TfidfMeta {
    vocab: HashMap<String, usize>,
    idf: Vec<f32>,
    pids: Vec<String>,
    ptoi: HashMap<String, usize>,
}

fn load_config(path: &Path) -> Result<PipelineConfig, String> {
    if path.exists() {
        let contents = fs::read_to_string(path)
            .map_err(|err| format!("Failed to read config {path:?}: {err}"))?;
        let config = serde_json::from_str::<PipelineConfigFile>(&contents)
            .map_err(|err| format!("Failed to parse config {path:?}: {err}"))?;
        Ok(PipelineConfig::from_file(config))
    } else {
        Ok(PipelineConfig::default())
    }
}

fn write_config(path: &Path, config: &PipelineConfig) -> Result<(), String> {
    let _ = utils::ensure_parent_dir(path)?;
    let contents = serde_json::to_string_pretty(config)
        .map_err(|err| format!("Failed to serialize config {path:?}: {err}"))?;
    fs::write(path, contents).map_err(|err| format!("Failed to write config {path:?}: {err}"))
}

fn parse_arxiv_url(url: &str) -> Result<(String, i32), String> {
    let idversion = url
        .rsplit('/')
        .next()
        .ok_or_else(|| format!("Invalid arXiv URL {url}"))?;
    let mut parts = idversion.split('v');
    let raw = parts
        .next()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("Invalid arXiv URL {url}"))?;
    let version = parts
        .next()
        .ok_or_else(|| format!("Invalid arXiv URL {url}"))?
        .parse::<i32>()
        .map_err(|err| format!("Invalid arXiv version in {url}: {err}"))?;
    Ok((raw.to_string(), version))
}

fn parse_category_counts(category_arg: &str) -> Result<HashMap<String, i32>, String> {
    let mut mapping = HashMap::new();
    if category_arg.trim().is_empty() {
        return Ok(mapping);
    }
    for part in category_arg.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (category, count_str) = trimmed.split_once('=').ok_or_else(|| {
            format!("Invalid category specification \"{trimmed}\" (expected category=count)")
        })?;
        let count = count_str
            .trim()
            .parse::<i32>()
            .map_err(|err| format!("Invalid count for category \"{category}\": {err}"))?;
        mapping.insert(category.trim().to_string(), count);
    }
    Ok(mapping)
}

static TOKEN_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?u)\b[a-zA-Z_][a-zA-Z0-9_]+\b").expect("valid token regex"));
static STOP_WORDS: Lazy<HashSet<String>> =
    Lazy::new(|| get(LANGUAGE::English).into_iter().collect());
static OPENALEX_PUNCTUATION: Lazy<HashSet<char>> = Lazy::new(|| {
    let chars = "'!\"#$%&'()*+,./:;<=>?@[\\]^_`{|}~";
    chars.chars().collect()
});

fn tokenize(text: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(text)
        .map(|mat| mat.as_str().to_lowercase())
        .filter(|token| !STOP_WORDS.contains(token))
        .collect()
}

#[derive(Debug, Default, Clone)]
struct OpenAlexMetadata {
    citation_count: Option<i64>,
    is_accepted: Option<bool>,
    is_published: Option<bool>,
}

fn fetch_openalex_metadata(
    client: &reqwest::blocking::Client,
    title: &str,
) -> OpenAlexMetadata {
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
    let response = match resp {
        Ok(response) => response,
        Err(err) => {
            println!("Unexpected error fetching OpenAlex metadata for {}: {}", title, err);
            return metadata;
        }
    };
    if response.status() == StatusCode::NOT_FOUND {
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
            println!("Unexpected error reading OpenAlex response for {}: {}", title, err);
            return metadata;
        }
    };
    let payload: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(err) => {
            println!("Unexpected error parsing OpenAlex response for {}: {}", title, err);
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

fn normalize_title(title: &str) -> String {
    title
        .to_lowercase()
        .chars()
        .filter(|ch| !OPENALEX_PUNCTUATION.contains(ch))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn write_bincode<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let encoded =
        bincode::serialize(value).map_err(|err| format!("Failed to encode {path:?}: {err}"))?;
    utils::write_atomic_bytes(path, &encoded)
}

fn read_bincode<T: DeserializeOwned>(path: &Path) -> Result<T, String> {
    let data = fs::read(path).map_err(|err| format!("Failed to read {path:?}: {err}"))?;
    bincode::deserialize(&data).map_err(|err| format!("Failed to decode {path:?}: {err}"))
}

fn load_db_jsonl(path: &Path) -> Result<HashMap<String, Paper>, String> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let file = File::open(path).map_err(|err| format!("Failed to open db file {path:?}: {err}"))?;
    let reader = BufReader::new(file);
    let mut db = HashMap::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|err| format!("Failed to read db line {}: {err}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let paper: Paper = serde_json::from_str(&line)
            .map_err(|err| format!("Failed to parse db line {}: {err}", idx + 1))?;
        db.insert(paper.id.clone(), paper);
    }
    Ok(db)
}

fn write_db_jsonl(path: &Path, db: &HashMap<String, Paper>) -> Result<(), String> {
    utils::write_atomic(path, |temp| {
        for paper in db.values() {
            let line = serde_json::to_string(paper)
                .map_err(|err| format!("Failed to serialize paper: {err}"))?;
            writeln!(temp, "{line}").map_err(|err| format!("Failed to write db: {err}"))?;
        }
        Ok(())
    })
}

fn ensure_command_exists(command: &str) -> Result<(), String> {
    which::which(command)
        .map_err(|_| format!("Missing dependency: {command} is not available on PATH"))?;
    Ok(())
}

fn arxiv_id_from_filename(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("unknown")
        .to_string()
}

fn run_pdftotext_for_file(pdf_path: &Path, txt_path: &Path) -> Result<(), String> {
    let status = Command::new("pdftotext")
        .arg(pdf_path)
        .arg(txt_path)
        .status()
        .map_err(|err| format!("Failed to launch pdftotext: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("pdftotext exited with status {status}"))
    }
}

fn run_parse_pdf_to_text(args: &ParsePdfArgs, config: &PipelineConfig) -> Result<(), String> {
    ensure_command_exists("pdftotext")?;
    let pdf_dir = args
        .pdf_dir
        .as_deref()
        .unwrap_or_else(|| Path::new(&config.pdf_dir));
    let txt_dir = args
        .txt_dir
        .as_deref()
        .unwrap_or_else(|| Path::new(&config.txt_dir));

    if !txt_dir.exists() {
        println!("creating {:?}", txt_dir);
        fs::create_dir_all(txt_dir)
            .map_err(|err| format!("Failed to create txt dir {txt_dir:?}: {err}"))?;
    }

    let existing_txt: HashSet<String> = fs::read_dir(txt_dir)
        .map_err(|err| format!("Failed to read txt dir {txt_dir:?}: {err}"))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();

    let pdf_entries: Vec<_> = fs::read_dir(pdf_dir)
        .map_err(|err| format!("Failed to read pdf dir {pdf_dir:?}: {err}"))?
        .filter_map(|entry| entry.ok())
        .collect();

    for (idx, entry) in pdf_entries.iter().enumerate() {
        let file_name = entry.file_name();
        let file_name = match file_name.into_string() {
            Ok(name) => name,
            Err(_) => {
                eprintln!("Skipping non-utf8 filename in {:?}", pdf_dir);
                continue;
            }
        };
        let txt_basename = format!("{file_name}.txt");
        if existing_txt.contains(&txt_basename) {
            println!(
                "{}/{} skipping {}, already exists.",
                idx,
                pdf_entries.len(),
                txt_basename
            );
            continue;
        }

        let pdf_path = pdf_dir.join(&file_name);
        let txt_path = txt_dir.join(&txt_basename);
        let cmd_desc = format!("pdftotext {:?} {:?}", pdf_path, txt_path);
        println!("{}/{} {}", idx, pdf_entries.len(), cmd_desc);

        if let Err(err) = run_pdftotext_for_file(&pdf_path, &txt_path) {
            let arxiv_id = arxiv_id_from_filename(&pdf_path);
            eprintln!("pdftotext failed for {arxiv_id}: {err}");
        }

        if !txt_path.is_file() {
            let arxiv_id = arxiv_id_from_filename(&pdf_path);
            eprintln!(
                "problem parsing {:?} to text for {arxiv_id}, creating empty text file",
                pdf_path
            );
            if let Err(err) = File::create(&txt_path) {
                eprintln!("failed to create empty txt file for {arxiv_id}: {err}");
            }
        }

        sleep(Duration::from_millis(10));
    }

    Ok(())
}

fn is_imagemagick_policy_denial(stderr_output: &str) -> bool {
    let lowered = stderr_output.to_lowercase();
    lowered.contains("security policy")
        || lowered.contains("not allowed by the security policy 'pdf'")
}

fn collect_thumbnail_files(tmp_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let pattern = tmp_dir.join("thumb-*.png");
    let pattern = pattern.to_string_lossy().to_string();
    let mut files: Vec<PathBuf> = glob::glob(&pattern)
        .map_err(|err| format!("Failed to read glob {pattern}: {err}"))?
        .filter_map(Result::ok)
        .collect();
    files.sort();
    Ok(files)
}

fn render_thumbnail_for_pdf(
    pdf_path: &Path,
    thumb_dir: &Path,
    tmp_dir: &Path,
) -> Result<PathBuf, String> {
    let arxiv_id = arxiv_id_from_filename(pdf_path);
    let file_name = pdf_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Invalid filename in {:?}", pdf_path))?;
    let thumb_path = thumb_dir.join(format!("{file_name}.jpg"));
    if thumb_path.is_file() {
        return Ok(thumb_path);
    }

    let pdf_tmp_dir = tmp_dir.join(&arxiv_id);
    if !pdf_tmp_dir.exists() {
        fs::create_dir_all(&pdf_tmp_dir)
            .map_err(|err| format!("Failed to create temp dir {pdf_tmp_dir:?}: {err}"))?;
    }

    if pdf_tmp_dir.join("thumb-0.png").is_file() {
        for index in 0..3 {
            let from = pdf_tmp_dir.join(format!("thumb-{index}.png"));
            let to = pdf_tmp_dir.join(format!("thumbbuf-{index}.png"));
            if from.is_file() {
                if let Err(err) = fs::rename(&from, &to) {
                    eprintln!("failed to move {:?} to {:?}: {err}", from, to);
                }
            }
        }
    }

    let mut child = Command::new("convert")
        .arg("-density")
        .arg("200")
        .arg(format!("{}[0-3]", pdf_path.display()))
        .arg("-thumbnail")
        .arg("x1000")
        .arg(pdf_tmp_dir.join("thumb-%d.png"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Failed to spawn convert: {err}"))?;

    let timed_out = match child.wait_timeout(Duration::from_secs(20)) {
        Ok(Some(_)) => false,
        Ok(None) => {
            let _ = child.kill();
            true
        }
        Err(err) => return Err(format!("Failed to wait for convert: {err}")),
    };

    let output = child
        .wait_with_output()
        .map_err(|err| format!("Failed to collect convert output: {err}"))?;
    let status = output.status;
    let stderr_output = String::from_utf8_lossy(&output.stderr);
    let policy_denial = is_imagemagick_policy_denial(&stderr_output);

    if timed_out {
        let arxiv_id = arxiv_id_from_filename(pdf_path);
        eprintln!("convert command timed out for {arxiv_id}, terminating");
    }

    if !status.success() || policy_denial {
        let arxiv_id = arxiv_id_from_filename(pdf_path);
        if policy_denial {
            eprintln!(
                "ImageMagick PDF policy blocked conversion for {arxiv_id}; update policy.xml to allow PDF."
            );
        } else {
            eprintln!(
                "convert failed for {arxiv_id} with exit code {:?}. stderr: {}",
                status.code(),
                stderr_output.trim()
            );
        }
    }

    if !pdf_tmp_dir.join("thumb-0.png").is_file() {
        eprintln!("could not render pdf for {arxiv_id}, creating missing placeholder");
        if let Err(err) = fs::copy(DEFAULT_MISSING_THUMB, &thumb_path) {
            eprintln!("failed to copy missing thumb for {arxiv_id}: {err}");
        }
    } else {
        let thumbs = collect_thumbnail_files(&pdf_tmp_dir)?;
        if thumbs.is_empty() {
            eprintln!("no thumbnails found for {arxiv_id}, creating placeholder");
            if let Err(err) = fs::copy(DEFAULT_MISSING_THUMB, &thumb_path) {
                eprintln!("failed to copy missing thumb for {arxiv_id}: {err}");
            }
        } else {
            let mut cmd = Command::new("montage");
            cmd.arg("-mode")
                .arg("concatenate")
                .arg("-quality")
                .arg("80")
                .arg("-tile")
                .arg("x1")
                .args(&thumbs)
                .arg(&thumb_path);
            println!("{:?}", cmd);
            if let Err(err) = cmd.status() {
                let arxiv_id = arxiv_id_from_filename(pdf_path);
                eprintln!("montage failed for {arxiv_id}: {err}");
            }
        }
    }

    Ok(thumb_path)
}

fn run_thumb_pdf(args: &ThumbArgs, config: &PipelineConfig) -> Result<(), String> {
    ensure_command_exists("convert")?;
    ensure_command_exists("montage")?;
    let pdf_dir = args
        .pdf_dir
        .as_deref()
        .unwrap_or_else(|| Path::new(&config.pdf_dir));
    let thumb_dir = args
        .thumb_dir
        .as_deref()
        .unwrap_or_else(|| Path::new(&config.thumb_dir));
    let tmp_dir = args
        .tmp_dir
        .as_deref()
        .unwrap_or_else(|| Path::new(&config.tmp_dir));

    if !thumb_dir.exists() {
        fs::create_dir_all(thumb_dir)
            .map_err(|err| format!("Failed to create thumb dir {thumb_dir:?}: {err}"))?;
    }
    if !tmp_dir.exists() {
        fs::create_dir_all(tmp_dir)
            .map_err(|err| format!("Failed to create tmp dir {tmp_dir:?}: {err}"))?;
    }

    let pdf_entries: Vec<_> = fs::read_dir(pdf_dir)
        .map_err(|err| format!("Failed to read pdf dir {pdf_dir:?}: {err}"))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("pdf"))
        .collect();

    let total = pdf_entries.len();
    if total == 0 {
        return Ok(());
    }

    let worker_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4)
        .min(total)
        .max(1);
    let mut queue = VecDeque::with_capacity(total);
    for entry in pdf_entries {
        queue.push_back(entry.path());
    }

    let queue = Arc::new(Mutex::new(queue));
    let thumb_dir = Arc::new(thumb_dir.to_path_buf());
    let tmp_dir = Arc::new(tmp_dir.to_path_buf());
    let progress = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let queue = Arc::clone(&queue);
        let thumb_dir = Arc::clone(&thumb_dir);
        let tmp_dir = Arc::clone(&tmp_dir);
        let progress = Arc::clone(&progress);
        handles.push(std::thread::spawn(move || {
            loop {
                let pdf_path = {
                    let mut guard = queue.lock().expect("queue mutex poisoned");
                    guard.pop_front()
                };
                let pdf_path = match pdf_path {
                    Some(path) => path,
                    None => break,
                };

                let idx = progress.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                let file_name = pdf_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("<unknown>");
                println!("{}/{} processing {}", idx, total, file_name);
                if let Err(err) = render_thumbnail_for_pdf(&pdf_path, &thumb_dir, &tmp_dir) {
                    let arxiv_id = arxiv_id_from_filename(&pdf_path);
                    eprintln!("failed to render thumbnail for {arxiv_id}: {err}");
                }
                sleep(Duration::from_millis(10));
            }
        }));
    }

    for handle in handles {
        if let Err(err) = handle.join() {
            eprintln!("thumbnail worker thread failed: {err:?}");
        }
    }

    Ok(())
}

fn extract_terms(text: &str) -> Vec<String> {
    let tokens = tokenize(text);
    if tokens.is_empty() {
        return Vec::new();
    }
    let mut terms = Vec::with_capacity(tokens.len() * 2);
    for token in &tokens {
        terms.push(token.clone());
    }
    for window in tokens.windows(2) {
        if let [first, second] = window {
            terms.push(format!("{first} {second}"));
        }
    }
    terms
}

fn build_vocab_and_idf(paths: &[PathBuf]) -> Result<(HashMap<String, usize>, Vec<f32>), String> {
    let mut term_counts: HashMap<String, usize> = HashMap::new();
    let mut doc_freq: HashMap<String, usize> = HashMap::new();

    for path in paths {
        let text =
            fs::read_to_string(path).map_err(|err| format!("Failed to read {path:?}: {err}"))?;
        let terms = extract_terms(&text);
        let mut seen: HashSet<String> = HashSet::new();
        for term in terms {
            *term_counts.entry(term.clone()).or_insert(0) += 1;
            if seen.insert(term.clone()) {
                *doc_freq.entry(term).or_insert(0) += 1;
            }
        }
    }

    let mut term_pairs: Vec<(String, usize)> = term_counts.into_iter().collect();
    term_pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    term_pairs.truncate(MAX_FEATURES);

    let mut vocab = HashMap::new();
    for (idx, (term, _count)) in term_pairs.iter().enumerate() {
        vocab.insert(term.clone(), idx);
    }

    let doc_count = paths.len() as f32;
    let mut idf = vec![0.0; vocab.len()];
    for (term, idx) in &vocab {
        let df = *doc_freq.get(term).unwrap_or(&0) as f32;
        idf[*idx] = ((1.0 + doc_count) / (1.0 + df)).ln() + 1.0;
    }

    Ok((vocab, idf))
}

fn vectorize_documents(
    paths: &[PathBuf],
    vocab: &HashMap<String, usize>,
    idf: &[f32],
) -> Result<Vec<Vec<f32>>, String> {
    let mut vectors = Vec::with_capacity(paths.len());

    for path in paths {
        let text =
            fs::read_to_string(path).map_err(|err| format!("Failed to read {path:?}: {err}"))?;
        let terms = extract_terms(&text);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for term in terms {
            *counts.entry(term).or_insert(0) += 1;
        }

        let mut vector = vec![0.0; vocab.len()];
        for (term, count) in counts {
            if let Some(&idx) = vocab.get(&term) {
                let tf = 1.0 + (count as f32).ln();
                vector[idx] = tf * idf[idx];
            }
        }

        let norm: f32 = vector.iter().map(|val| val * val).sum::<f32>().sqrt();
        if norm > 0.0 {
            for val in &mut vector {
                *val /= norm;
            }
        }
        vectors.push(vector);
    }

    Ok(vectors)
}

fn vectorize_document_text(text: &str, meta: &TfidfMeta) -> Vec<f32> {
    let terms = extract_terms(text);
    let mut counts: HashMap<String, usize> = HashMap::new();
    for term in terms {
        *counts.entry(term).or_insert(0) += 1;
    }

    let mut vector = vec![0.0; meta.vocab.len()];
    for (term, count) in counts {
        if let Some(&idx) = meta.vocab.get(&term) {
            let tf = 1.0 + (count as f32).ln();
            vector[idx] = tf * meta.idf[idx];
        }
    }

    let norm: f32 = vector.iter().map(|val| val * val).sum::<f32>().sqrt();
    if norm > 0.0 {
        for val in &mut vector {
            *val /= norm;
        }
    }

    vector
}


fn run_analyze(config: &PipelineConfig) -> Result<(), String> {
    let db_path = Path::new(&config.db_path);
    let db = load_db_jsonl(db_path)?;
    let mut papers: Vec<&Paper> = db.values().collect();
    papers.sort_by(|a, b| a.id.cmp(&b.id).then_with(|| a.version.cmp(&b.version)));

    let txt_dir = Path::new(&config.txt_dir);
    let mut txt_paths = Vec::new();
    let mut pids = Vec::new();
    let mut n = 0;

    for paper in papers {
        n += 1;
        let idvv = format!("{}v{}", paper.id, paper.version);
        let txt_path = txt_dir.join(format!("{idvv}.pdf.txt"));
        if txt_path.is_file() {
            let txt = fs::read_to_string(&txt_path)
                .map_err(|err| format!("Failed to read {txt_path:?}: {err}"))?;
            let len = txt.len();
            if len > MIN_TEXT_LEN && len < MAX_TEXT_LEN {
                txt_paths.push(txt_path);
                pids.push(idvv.clone());
                println!("read {}/{} ({idvv}) with {} chars", n, db.len(), len);
            } else {
                println!(
                    "skipped {}/{} ({idvv}) with {} chars: suspicious!",
                    n,
                    db.len(),
                    len
                );
            }
        } else {
            println!("could not find {:?} in txt folder.", txt_path);
        }
    }
    println!(
        "in total read in {} text files out of {} db entries.",
        txt_paths.len(),
        db.len()
    );

    let mut train_txt_paths = txt_paths.clone();
    let mut rng = rand::rngs::StdRng::seed_from_u64(1337);
    train_txt_paths.shuffle(&mut rng);
    train_txt_paths.truncate(std::cmp::min(train_txt_paths.len(), MAX_TRAIN_DOCS));
    println!("training on {} documents...", train_txt_paths.len());

    let (vocab, idf) = build_vocab_and_idf(&train_txt_paths)?;
    println!("transforming {} documents...", txt_paths.len());
    let vectors = vectorize_documents(&txt_paths, &vocab, &idf)?;
    println!("vocab size {}, vectors {}", vocab.len(), vectors.len());

    let tfidf = TfidfMatrix { vectors };
    println!("writing {}", config.tfidf_path);
    write_bincode(Path::new(&config.tfidf_path), &tfidf)?;

    let ptoi: HashMap<String, usize> = pids
        .iter()
        .enumerate()
        .map(|(idx, pid)| (pid.clone(), idx))
        .collect();
    let meta = TfidfMeta {
        vocab,
        idf,
        pids: pids.clone(),
        ptoi,
    };
    println!("writing {}", config.tfidf_meta_path);
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|err| format!("Failed to serialize meta: {err}"))?;
    fs::write(&config.tfidf_meta_path, meta_json)
        .map_err(|err| format!("Failed to write {}: {err}", config.tfidf_meta_path))?;

    println!("building HNSW index...");
    let hnsw_index = HnswIndex::build(&tfidf.vectors, &pids)?;
    println!("writing {}", config.hnsw_index_path);
    write_bincode(Path::new(&config.hnsw_index_path), &hnsw_index)?;

    Ok(())
}

fn run_buildsvm(config: &PipelineConfig) -> Result<(), String> {
    let db_path = Path::new(&config.database_path);
    if !db_path.is_file() {
        return Err(format!(
            "the database file {} should exist. You can create an empty database with sqlite3 {} < schema.sql",
            config.database_path, config.database_path
        ));
    }

    let db = db::Database::open(db_path)?;

    let meta_contents = fs::read_to_string(&config.tfidf_meta_path)
        .map_err(|err| format!("Failed to read {}: {err}", config.tfidf_meta_path))?;
    let meta: TfidfMeta = serde_json::from_str(&meta_contents)
        .map_err(|err| format!("Failed to parse tfidf meta: {err}"))?;
    let tfidf: TfidfMatrix = read_bincode(Path::new(&config.tfidf_path))?;

    let mut xtoi = HashMap::new();
    for (pid, idx) in &meta.ptoi {
        xtoi.insert(utils::strip_version(pid), *idx);
    }

    let features: Vec<Vec<f64>> = tfidf
        .vectors
        .iter()
        .map(|row| row.iter().map(|val| *val as f64).collect())
        .collect();
    let x_full = DenseMatrix::from_2d_vec(&features);

    let users = db.list_users()?;
    println!("number of users: {}", users.len());

    let mut user_sim: HashMap<i64, Vec<String>> = HashMap::new();

    for (idx, user) in users.iter().enumerate() {
        println!(
            "{}/{} building an SVM for {}",
            idx,
            users.len(),
            user.username
        );
        let library_pids = db.list_library_paper_ids_for_user(user.user_id)?;
        let mut posix = Vec::new();
        for pid in library_pids {
            if let Some(ix) = xtoi.get(&pid) {
                posix.push(*ix);
            }
        }

        if posix.is_empty() {
            continue;
        }

        let pos_set: HashSet<usize> = posix.iter().copied().collect();
        let pos_count = pos_set.len();
        let neg_count = features.len().saturating_sub(pos_count);
        let pos_repeat = ((neg_count as f64 / pos_count as f64).round() as usize).max(1);

        let mut train_features = Vec::new();
        let mut train_labels: Vec<i32> = Vec::new();
        for (row_idx, row) in features.iter().enumerate() {
            if pos_set.contains(&row_idx) {
                for _ in 0..pos_repeat {
                    train_features.push(row.clone());
                    train_labels.push(1);
                }
            } else {
                train_features.push(row.clone());
                train_labels.push(-1);
            }
        }

        let train_matrix = DenseMatrix::from_2d_vec(&train_features);
        let params = SVCParameters::default()
            .with_c(0.1)
            .with_kernel(Kernels::linear());
        let svc = SVC::fit(&train_matrix, &train_labels, &params)
            .map_err(|err| format!("Failed to train SVM for {}: {err}", user.username))?;
        let scores = svc
            .decision_function(&x_full)
            .map_err(|err| format!("Failed to score SVM for {}: {err}", user.username))?;

        let mut score_pairs: Vec<(usize, f64)> = scores.into_iter().enumerate().collect();
        score_pairs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        let recommendations = score_pairs
            .iter()
            .take(std::cmp::min(NUM_RECOMMENDATIONS, score_pairs.len()))
            .map(|(ix, _)| utils::strip_version(&meta.pids[*ix]))
            .collect::<Vec<String>>();
        user_sim.insert(user.user_id, recommendations);
    }

    println!("writing {}", config.user_sim_path);
    write_bincode(Path::new(&config.user_sim_path), &user_sim)?;
    let user_json_path = Path::new(&config.user_sim_path).with_extension("json");
    let user_json = serde_json::to_string_pretty(&user_sim)
        .map_err(|err| format!("Failed to serialize {user_json_path:?}: {err}"))?;
    fs::write(&user_json_path, user_json)
        .map_err(|err| format!("Failed to write {user_json_path:?}: {err}"))?;

    Ok(())
}

fn fetch_for_query(
    client: &reqwest::blocking::Client,
    search_query: &str,
    max_results: Option<i32>,
    args: &FetchArgs,
    db: &mut HashMap<String, Paper>,
) -> Result<i32, String> {
    let base_url = "http://export.arxiv.org/api/query?";
    let max_fetch = max_results.unwrap_or(args.max_index);
    println!("Searching arXiv for {search_query}");
    println!("database has {} entries at start", db.len());

    let mut num_added_total = 0;
    let mut index = args.start_index;
    while index < max_fetch {
        let results_this_iter = std::cmp::min(args.results_per_iteration, max_fetch - index);
        println!("Results {index} - {}", index + results_this_iter);
        let query = format!(
            "search_query={search_query}&sortBy=lastUpdatedDate&start={index}&max_results={results_this_iter}"
        );
        let response = client
            .get(format!("{base_url}{query}"))
            .send()
            .map_err(|err| format!("Failed to query arXiv: {err}"))?
            .bytes()
            .map_err(|err| format!("Failed to read response bytes: {err}"))?;

        let feed = parser::parse(&response[..])
            .map_err(|err| format!("Failed to parse Atom feed: {err}"))?;
        let mut num_added = 0;
        let mut num_skipped = 0;

        if feed.entries.is_empty() {
            println!(
                "Received no results from arxiv. Rate limiting? Exiting. Restart later maybe."
            );
            break;
        }

        for entry in feed.entries {
            let (raw_id, version) = parse_arxiv_url(&entry.id)?;
            let title = entry
                .title
                .as_ref()
                .map(|t| t.content.clone())
                .unwrap_or_default();
            let openalex_metadata = fetch_openalex_metadata(client, &title);
            let abstract_text = entry
                .summary
                .as_ref()
                .map(|s| s.content.clone())
                .unwrap_or_default();
            let authors = entry
                .authors
                .iter()
                .map(|author| author.name.clone())
                .collect::<Vec<_>>();
            let updated = entry.updated.map(|dt| dt.to_string()).unwrap_or_default();
            let categories = entry
                .categories
                .iter()
                .map(|c| c.term.clone())
                .collect::<Vec<_>>();

            let paper = Paper {
                id: raw_id.clone(),
                version,
                title,
                authors,
                abstract_text,
                updated,
                categories,
                citation_count: openalex_metadata.citation_count,
                is_accepted: openalex_metadata.is_accepted,
                is_published: openalex_metadata.is_published,
            };

            if db.get(&raw_id).map(|p| p.version).unwrap_or(0) < version {
                db.insert(raw_id.clone(), paper.clone());
                println!("Updated {} added {}", paper.updated, paper.title);
                num_added += 1;
                num_added_total += 1;
            } else {
                num_skipped += 1;
            }
        }

        println!("Added {num_added} papers, already had {num_skipped}.");

        if num_added == 0 && args.break_on_no_added == 1 {
            println!("No new papers were added. Assuming no new papers exist. Exiting.");
            break;
        }

        index += results_this_iter;
        if index < max_fetch {
            let jitter: f64 = rand::thread_rng().gen_range(0.0..3.0);
            println!("Sleeping for {} seconds", args.wait_time);
            sleep(Duration::from_secs_f64(args.wait_time + jitter));
        }
    }

    Ok(num_added_total)
}

fn run_fetch(args: &FetchArgs, config: &PipelineConfig) -> Result<(), String> {
    let db_path = Path::new(&config.db_path);
    let mut db = load_db_jsonl(db_path)?;
    let category_counts = parse_category_counts(&args.category_counts)?;
    let client = reqwest::blocking::Client::builder()
        .user_agent("arxiv-sanity-rust-fetcher")
        .build()
        .map_err(|err| format!("Failed to build HTTP client: {err}"))?;

    let mut num_added_total = 0;
    if !category_counts.is_empty() {
        for (category, max_results) in category_counts {
            let query = format!("cat:{category}");
            println!("--- Fetching up to {max_results} papers for category {category} ---");
            num_added_total += fetch_for_query(&client, &query, Some(max_results), args, &mut db)?;
        }
    } else {
        num_added_total += fetch_for_query(&client, &args.search_query, None, args, &mut db)?;
    }

    if num_added_total > 0 {
        println!(
            "Saving database with {} papers to {}",
            db.len(),
            config.db_path
        );
        write_db_jsonl(db_path, &db)?;
    }

    Ok(())
}

struct ResetPipelineTargets {
    output_dir: PathBuf,
    ingest_jobs_dir: PathBuf,
    recompute_stdout: PathBuf,
    recompute_stderr: PathBuf,
    legacy_files: Vec<PathBuf>,
}

impl ResetPipelineTargets {
    fn all_paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![
            self.output_dir.clone(),
            self.ingest_jobs_dir.clone(),
            self.recompute_stdout.clone(),
            self.recompute_stderr.clone(),
        ];
        paths.extend(self.legacy_files.iter().cloned());
        paths
    }
}

fn resolve_reset_targets(config: &PipelineConfig) -> Result<ResetPipelineTargets, String> {
    let cwd = std::env::current_dir()
        .map_err(|err| format!("Failed to resolve current directory: {err}"))?;
    let output_dir = resolve_path(&cwd, Path::new(&config.output_dir));
    let ingest_jobs_dir = cwd.join("data/ingest_jobs");
    let recompute_stdout = ingest_jobs_dir.join("recompute_stdout.log");
    let recompute_stderr = ingest_jobs_dir.join("recompute_stderr.log");
    let legacy_files = vec![
        cwd.join("db.p"),
        cwd.join("tfidf_meta.p"),
        cwd.join("user_sim.p"),
        cwd.join("db2.p"),
        cwd.join("serve_cache.p"),
        cwd.join("as.db"),
    ];
    Ok(ResetPipelineTargets {
        output_dir,
        ingest_jobs_dir,
        recompute_stdout,
        recompute_stderr,
        legacy_files,
    })
}

fn resolve_path(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn reset_pipeline_warning(config: &PipelineConfig) -> Result<String, String> {
    let targets = resolve_reset_targets(config)?;
    let mut message = String::from(
        "WARNING: reset-pipeline will delete the following resolved paths:\n",
    );
    for path in targets.all_paths() {
        message.push_str(&format!("  - {}\n", path.display()));
    }
    message.push_str("Re-run with --force to proceed.");
    Ok(message)
}

fn run_reset_pipeline(config: &PipelineConfig) -> Result<(), String> {
    let targets = resolve_reset_targets(config)?;
    for path in targets.all_paths() {
        if !path.exists() {
            continue;
        }
        if path.is_dir() {
            fs::remove_dir_all(&path)
                .map_err(|err| format!("Failed to remove directory {}: {err}", path.display()))?;
        } else {
            fs::remove_file(&path)
                .map_err(|err| format!("Failed to remove file {}: {err}", path.display()))?;
        }
    }
    Ok(())
}

fn run_all_rust(
    run_all: &RunAllArgs,
    config: &PipelineConfig,
    config_path: &Path,
) -> Result<(), String> {
    run_fetch(&run_all.fetch, config)
        .and_then(|_| download::run_download_pdfs(&DownloadArgs {}, config))
        .and_then(|_| {
            run_parse_pdf_to_text(
                &ParsePdfArgs {
                    pdf_dir: None,
                    txt_dir: None,
                },
                config,
            )
        })
        .and_then(|_| {
            run_thumb_pdf(
                &ThumbArgs {
                    pdf_dir: None,
                    thumb_dir: None,
                    tmp_dir: None,
                },
                config,
            )
        })
        .and_then(|_| run_analyze(config))
        .and_then(|_| run_buildsvm(config))
        .and_then(|_| cache::run_make_cache(config_path))
}

fn run_rust_command(
    command: Commands,
    config: &PipelineConfig,
    config_path: &Path,
) -> Result<(), String> {
    match command {
        Commands::FetchPapers(args) => run_fetch(&args, config),
        Commands::DownloadPdfs(args) => download::run_download_pdfs(&args, config),
        Commands::ParsePdfToText(args) => run_parse_pdf_to_text(&args, config),
        Commands::ThumbPdf(args) => run_thumb_pdf(&args, config),
        Commands::Analyze => run_analyze(config),
        Commands::BuildSvm => run_buildsvm(config),
        Commands::MakeCache => cache::run_make_cache(config_path),
        Commands::IngestSinglePaper(args) => {
            ingest::run_ingest_single_paper(&args, config, config_path)
        }
        Commands::MigrateAnalysis(args) => migrate::run_migrate_analysis(&args),
        Commands::MigrateDb(args) => migrate::run_migrate_db(&args),
        Commands::Serve(args) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|err| format!("Failed to create runtime: {err}"))?;
            let serve_args = serve::ServeArgs::new(args.prod, args.num_results, args.port);
            rt.block_on(serve::run_with_args(serve_args))
                .map_err(|err| format!("serve failed: {err}"))
        }
        Commands::TwitterDaemon(args) => twitter::run_twitter_daemon(&args, config),
        Commands::RunAll(run_all) => run_all_rust(&run_all, config, config_path),
        Commands::ResetPipeline(args) => {
            if !args.force {
                let warning = reset_pipeline_warning(config)?;
                eprintln!("{warning}");
                return Err("reset-pipeline aborted; re-run with --force to proceed.".to_string());
            }
            run_reset_pipeline(config)
        }
    }
}

fn dispatch_command(
    command: Commands,
    config: &PipelineConfig,
    config_path: &Path,
) -> Result<(), String> {
    run_rust_command(command, config, config_path)
}

fn main() {
    let cli = Cli::parse();
    let config = match load_config(&cli.config) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = fs::create_dir_all(&config.output_dir) {
        eprintln!(
            "Failed to create output directory {:?}: {err}",
            config.output_dir
        );
        std::process::exit(1);
    }

    if cli.write_config {
        if let Err(err) = write_config(&cli.config, &config) {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }

    let Some(command) = cli.command else {
        if !cli.write_config {
            eprintln!("No subcommand supplied. Use --help for usage details.");
            std::process::exit(2);
        }
        return;
    };

    let result = dispatch_command(command, &config, &cli.config);

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
