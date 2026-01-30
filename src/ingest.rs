use feed_rs::parser;
use reqwest::blocking::Client;
use std::fs;
use std::path::{Path, PathBuf};

use crate::download;
use crate::{
    ensure_command_exists, load_db_jsonl, parse_arxiv_url, read_bincode, run_analyze,
    run_buildsvm, run_pdftotext_for_file, render_thumbnail_for_pdf, utils,
    vectorize_document_text, write_bincode, write_db_jsonl, IngestSinglePaperArgs, Paper,
    PipelineConfig, TfidfMatrix, TfidfMeta,
};
use crate::hnsw_index::HnswIndex;

fn fetch_paper_metadata(paper_id: &str) -> Result<Paper, String> {
    let base_url = "http://export.arxiv.org/api/query?id_list=";
    let client = Client::builder()
        .user_agent("arxiv-sanity-preserver (contact: you@example.com)")
        .build()
        .map_err(|err| format!("Failed to build HTTP client: {err}"))?;
    let response = client
        .get(format!("{base_url}{paper_id}"))
        .send()
        .map_err(|err| format!("Failed to query arXiv: {err}"))?
        .bytes()
        .map_err(|err| format!("Failed to read response bytes: {err}"))?;
    let feed = parser::parse(&response[..])
        .map_err(|err| format!("Failed to parse Atom feed: {err}"))?;
    let entry = feed
        .entries
        .into_iter()
        .next()
        .ok_or_else(|| format!("No paper found for id {paper_id}"))?;
    let (raw_id, version) = parse_arxiv_url(&entry.id)?;
    let title = entry
        .title
        .as_ref()
        .map(|t| t.content.clone())
        .unwrap_or_default();
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

    Ok(Paper {
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
    })
}

fn ensure_text_for_pdf(pdf_path: &Path, txt_dir: &Path) -> Result<PathBuf, String> {
    if !txt_dir.exists() {
        fs::create_dir_all(txt_dir)
            .map_err(|err| format!("Failed to create txt dir {txt_dir:?}: {err}"))?;
    }
    let file_name = pdf_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Invalid PDF filename {pdf_path:?}"))?;
    let txt_path = txt_dir.join(format!("{file_name}.txt"));
    if txt_path.is_file() {
        println!("Text already exists at {}", txt_path.display());
        return Ok(txt_path);
    }
    if let Err(err) = run_pdftotext_for_file(pdf_path, &txt_path) {
        eprintln!("pdftotext failed for {}: {err}", pdf_path.display());
    }
    if !txt_path.is_file() {
        println!(
            "Could not parse {} to text; creating empty placeholder",
            pdf_path.display()
        );
        fs::write(&txt_path, "")
            .map_err(|err| format!("Failed to create placeholder {txt_path:?}: {err}"))?;
    }
    Ok(txt_path)
}

fn ensure_thumbnail_for_pdf(pdf_path: &Path, thumb_dir: &Path, tmp_dir: &Path) -> Result<(), String> {
    if !thumb_dir.exists() {
        fs::create_dir_all(thumb_dir)
            .map_err(|err| format!("Failed to create thumb dir {thumb_dir:?}: {err}"))?;
    }
    if !tmp_dir.exists() {
        fs::create_dir_all(tmp_dir)
            .map_err(|err| format!("Failed to create tmp dir {tmp_dir:?}: {err}"))?;
    }
    let thumb_path = render_thumbnail_for_pdf(pdf_path, thumb_dir, tmp_dir)?;
    println!("Thumbnail stored at {}", thumb_path.display());
    Ok(())
}

fn update_incremental_tfidf(
    config: &PipelineConfig,
    txt_path: &Path,
    paper: &Paper,
) -> Result<(), String> {
    // Incremental TF-IDF policy: keep the existing IDF fixed for single-paper ingest.
    // Full recomputation (including IDF refresh) is handled by run_analyze.
    let meta_contents = fs::read_to_string(&config.tfidf_meta_path)
        .map_err(|err| format!("Failed to read {}: {err}", config.tfidf_meta_path))?;
    let mut meta: TfidfMeta = serde_json::from_str(&meta_contents)
        .map_err(|err| format!("Failed to parse tfidf meta: {err}"))?;
    let mut tfidf: TfidfMatrix = read_bincode(Path::new(&config.tfidf_path))?;

    let text =
        fs::read_to_string(txt_path).map_err(|err| format!("Failed to read {txt_path:?}: {err}"))?;
    let vector = vectorize_document_text(&text, &meta);
    let pid = format!("{}v{}", paper.id, paper.version);

    let pid_exists = meta.ptoi.contains_key(&pid);

    if let Some(&idx) = meta.ptoi.get(&pid) {
        if let Some(slot) = tfidf.vectors.get_mut(idx) {
            *slot = vector.clone();
        } else {
            return Err(format!(
                "TF-IDF index mismatch: {} points to missing vector",
                pid
            ));
        }
    } else {
        let idx = tfidf.vectors.len();
        tfidf.vectors.push(vector.clone());
        meta.pids.push(pid.clone());
        meta.ptoi.insert(pid.clone(), idx);
    }

    println!("writing {}", config.tfidf_path);
    write_bincode(Path::new(&config.tfidf_path), &tfidf)?;
    println!("writing {}", config.tfidf_meta_path);
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|err| format!("Failed to serialize meta: {err}"))?;
    fs::write(&config.tfidf_meta_path, meta_json)
        .map_err(|err| format!("Failed to write {}: {err}", config.tfidf_meta_path))?;

    let hnsw_index_path = Path::new(&config.hnsw_index_path);
    let mut hnsw_index = if hnsw_index_path.exists() {
        match read_bincode::<HnswIndex>(hnsw_index_path) {
            Ok(index) => Some(index),
            Err(err) => {
                eprintln!(
                    "Failed to read existing HNSW index at {}: {err}",
                    hnsw_index_path.display()
                );
                None
            }
        }
    } else {
        None
    };

    let mut rebuild_hnsw = !pid_exists;
    if let Some(index) = hnsw_index.as_ref() {
        rebuild_hnsw = index.len() != tfidf.vectors.len() - if pid_exists { 0 } else { 1 };
    }
    if pid_exists || rebuild_hnsw {
        println!("building HNSW index...");
        hnsw_index = Some(HnswIndex::build(&tfidf.vectors, &meta.pids)?);
    } else if let Some(index) = hnsw_index.as_mut() {
        println!("updating HNSW index with new paper...");
        index.insert(pid.clone(), vector.clone())?;
    } else {
        println!("building HNSW index...");
        hnsw_index = Some(HnswIndex::build(&tfidf.vectors, &meta.pids)?);
    }
    println!("writing {}", config.hnsw_index_path);
    if let Some(index) = hnsw_index {
        write_bincode(Path::new(&config.hnsw_index_path), &index)?;
    }

    Ok(())
}

pub fn run_ingest_single_paper(
    args: &IngestSinglePaperArgs,
    config: &PipelineConfig,
    config_path: &Path,
) -> Result<(), String> {
    let paper_id = args
        .paper_id
        .as_deref()
        .ok_or_else(|| "paper_id is required for ingest-single-paper".to_string())?;
    if !utils::is_valid_id(paper_id) {
        return Err(format!("{paper_id} is not a valid arXiv identifier"));
    }

    println!("Fetching arXiv metadata for {paper_id}");
    let paper = fetch_paper_metadata(paper_id)?;
    let mut db = load_db_jsonl(Path::new(&config.db_path))?;

    let existing_version = db.get(&paper.id).map(|p| p.version).unwrap_or(0);
    if existing_version >= paper.version {
        println!(
            "Paper {} already present with version {}, refreshing assets only",
            paper.id, existing_version
        );
    } else {
        println!("Saving metadata for {}v{}", paper.id, paper.version);
        db.insert(paper.id.clone(), paper.clone());
        write_db_jsonl(Path::new(&config.db_path), &db)?;
    }

    let pdf_dir = Path::new(&config.pdf_dir);
    let client = Client::builder()
        .user_agent("arxiv-sanity-preserver (contact: you@example.com)")
        .build()
        .map_err(|err| format!("Failed to build HTTP client: {err}"))?;
    let pdf_path = download::download_pdf_for_paper(&client, &paper, pdf_dir)?;
    println!("PDF stored at {}", pdf_path.display());

    let txt_dir = Path::new(&config.txt_dir);
    ensure_command_exists("pdftotext")?;
    let txt_path = ensure_text_for_pdf(&pdf_path, txt_dir)?;
    println!("Text stored at {}", txt_path.display());

    ensure_command_exists("convert")?;
    ensure_command_exists("montage")?;
    ensure_thumbnail_for_pdf(
        &pdf_path,
        Path::new(&config.thumb_dir),
        Path::new(&config.tmp_dir),
    )?;

    if !args.no_recompute {
        println!("Recomputing tfidf and similarity caches...");
        run_analyze(config)?;
        run_buildsvm(config)?;
        crate::cache::run_make_cache(config_path)?;
    } else {
        println!("Updating TF-IDF vector for single-paper ingest...");
        update_incremental_tfidf(config, &txt_path, &paper)?;
        println!("Skipping full cache recompute as requested.");
    }

    Ok(())
}
