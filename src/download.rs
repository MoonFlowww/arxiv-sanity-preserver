use rand::Rng;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use crate::{load_db_jsonl, utils, DownloadArgs, Paper, PipelineConfig};

#[derive(Debug, Deserialize)]
struct DownloadTopicSettings {
    #[serde(default)]
    published: bool,
}

#[derive(Debug, Deserialize)]
struct DownloadSettings {
    #[serde(default)]
    topics: HashMap<String, DownloadTopicSettings>,
}

fn load_published_topic_filter(config: &PipelineConfig) -> HashSet<String> {
    let settings_path = Path::new(&config.output_dir).join("download_settings.json");
    let bytes = match fs::read(&settings_path) {
        Ok(bytes) => bytes,
        Err(_) => return HashSet::new(),
    };
    let settings: DownloadSettings = match serde_json::from_slice(&bytes) {
        Ok(settings) => settings,
        Err(_) => return HashSet::new(),
    };
    settings
        .topics
        .into_iter()
        .filter_map(|(topic, settings)| settings.published.then_some(topic))
        .collect()
}

fn build_pdf_filename(paper: &Paper) -> String {
    format!("{}v{}.pdf", paper.id, paper.version)
}

fn build_pdf_url(paper: &Paper) -> String {
    format!(
        "https://export.arxiv.org/pdf/{}v{}.pdf",
        paper.id, paper.version
    )
}

fn is_html_response(content_type: Option<&str>, body: &[u8]) -> bool {
    content_type
        .map(|value| value.to_lowercase().contains("text/html"))
        .unwrap_or(false)
        || body
            .iter()
            .skip_while(|byte| byte.is_ascii_whitespace())
            .take(6)
            .map(|byte| byte.to_ascii_lowercase())
            .eq(b"<html>".iter().copied())
}

pub fn download_pdf_for_paper(
    client: &Client,
    paper: &Paper,
    pdf_dir: &Path,
) -> Result<PathBuf, String> {
    let filename = build_pdf_filename(paper);
    let pdf_path = pdf_dir.join(&filename);
    if pdf_path.is_file() {
        return Ok(pdf_path);
    }

    let pdf_url = build_pdf_url(paper);
    let response = client
        .get(&pdf_url)
        .send()
        .map_err(|err| format!("Failed to download {pdf_url}: {err}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to download {pdf_url}: HTTP {}",
            response.status()
        ));
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok().map(str::to_owned));
    let bytes = response
        .bytes()
        .map_err(|err| format!("Failed to read {pdf_url}: {err}"))?;
    if is_html_response(content_type.as_deref(), &bytes) {
        return Err(format!(
            "Got HTML instead of PDF for {pdf_url} (content-type={content_type:?})"
        ));
    }

    utils::ensure_parent_dir(&pdf_path)?;
    utils::write_atomic_bytes(&pdf_path, &bytes)?;
    Ok(pdf_path)
}

pub fn run_download_pdfs(_args: &DownloadArgs, config: &PipelineConfig) -> Result<(), String> {
    let db = load_db_jsonl(Path::new(&config.db_path))?;
    let pdf_dir = Path::new(&config.pdf_dir);
    let published_topics = load_published_topic_filter(config);
    if !pdf_dir.exists() {
        fs::create_dir_all(pdf_dir)
            .map_err(|err| format!("Failed to create pdf dir {pdf_dir:?}: {err}"))?;
    }

    let mut have: HashSet<String> = fs::read_dir(pdf_dir)
        .map_err(|err| format!("Failed to read pdf dir {pdf_dir:?}: {err}"))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();

    let client = Client::builder()
        .user_agent("arxiv-sanity-preserver (contact: you@example.com)")
        .build()
        .map_err(|err| format!("Failed to build HTTP client: {err}"))?;

    let mut numok = 0;
    let mut numtot = 0;
    let mut rng = rand::thread_rng();

    for paper in db.values() {
        if !published_topics.is_empty() {
            let matches_published_topic = paper
                .categories
                .iter()
                .any(|category| published_topics.contains(category));
            if matches_published_topic
                && !(paper.is_accepted.unwrap_or(false) || paper.is_published.unwrap_or(false))
            {
                continue;
            }
        }
        let filename = build_pdf_filename(paper);
        numtot += 1;

        if !have.contains(&filename) {
            let pdf_url = build_pdf_url(paper);
            println!("fetching {pdf_url} into {}", pdf_dir.join(&filename).display());
            match download_pdf_for_paper(&client, paper, pdf_dir) {
                Ok(_) => {
                    have.insert(filename);
                    numok += 1;
                    let jitter = rng.gen_range(0.0..0.5);
                    sleep(Duration::from_secs_f64(0.5 + jitter));
                }
                Err(err) => {
                    eprintln!("error downloading: {pdf_url}");
                    eprintln!("{err}");
                }
            }
        } else {
            println!(
                "{} exists, skipping",
                pdf_dir.join(&filename).display()
            );
            numok += 1;
        }

        println!("{numok}/{numtot} of {} downloaded ok.", db.len());
    }

    println!("final number of papers downloaded okay: {numok}/{}", db.len());
    Ok(())
}
