use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_OUTPUT_DIR: &str = ".pipeline";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub output_dir: PathBuf,
    pub db_path: PathBuf,
    pub pdf_dir: PathBuf,
    pub txt_dir: PathBuf,
    pub thumb_dir: PathBuf,
    pub tmp_dir: PathBuf,
    pub database_path: PathBuf,
    pub tfidf_path: PathBuf,
    pub tfidf_meta_path: PathBuf,
    pub tfidf_meta_pickle_path: PathBuf,
    pub hnsw_index_path: PathBuf,
    pub user_sim_path: PathBuf,
    pub user_sim_json_path: PathBuf,
    pub db_pickle_path: PathBuf,
    pub db_serve_path: PathBuf,
    pub serve_cache_path: PathBuf,
    pub ingest_jobs_dir: PathBuf,
    pub download_settings_path: PathBuf,
    pub ui_settings_path: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
pub struct AppConfigFile {
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
    #[serde(default)]
    pub db_path: Option<PathBuf>,
    #[serde(default)]
    pub pdf_dir: Option<PathBuf>,
    #[serde(default)]
    pub txt_dir: Option<PathBuf>,
    #[serde(default)]
    pub thumb_dir: Option<PathBuf>,
    #[serde(default)]
    pub tmp_dir: Option<PathBuf>,
    #[serde(default)]
    pub database_path: Option<PathBuf>,
    #[serde(default)]
    pub tfidf_path: Option<PathBuf>,
    #[serde(default)]
    pub tfidf_meta_path: Option<PathBuf>,
    #[serde(default)]
    pub tfidf_meta_pickle_path: Option<PathBuf>,
    #[serde(default)]
    pub hnsw_index_path: Option<PathBuf>,
    #[serde(default)]
    pub user_sim_path: Option<PathBuf>,
    #[serde(default)]
    pub user_sim_json_path: Option<PathBuf>,
    #[serde(default)]
    pub db_pickle_path: Option<PathBuf>,
    #[serde(default)]
    pub db_serve_path: Option<PathBuf>,
    #[serde(default)]
    pub serve_cache_path: Option<PathBuf>,
    #[serde(default)]
    pub ingest_jobs_dir: Option<PathBuf>,
    #[serde(default)]
    pub download_settings_path: Option<PathBuf>,
    #[serde(default)]
    pub ui_settings_path: Option<PathBuf>,
}

fn output_path(output_dir: &Path, entry: &str) -> PathBuf {
    output_dir.join(entry)
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::from_file(AppConfigFile::default()).expect("default config must be valid")
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        if path.exists() {
            let contents = fs::read_to_string(path)
                .map_err(|err| format!("Failed to read config {path:?}: {err}"))?;
            let config_file = serde_json::from_str::<AppConfigFile>(&contents)
                .map_err(|err| format!("Failed to parse config {path:?}: {err}"))?;
            Self::from_file(config_file)
        } else {
            Ok(Self::default())
        }
    }

    pub fn from_file(config: AppConfigFile) -> Result<Self, String> {
        let output_dir = config
            .output_dir
            .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_DIR));

        let resolved = Self {
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
            tfidf_meta_pickle_path: config
                .tfidf_meta_pickle_path
                .unwrap_or_else(|| output_path(&output_dir, "tfidf_meta.p")),
            hnsw_index_path: config
                .hnsw_index_path
                .unwrap_or_else(|| output_path(&output_dir, "hnsw_index.bin")),
            user_sim_path: config
                .user_sim_path
                .unwrap_or_else(|| output_path(&output_dir, "user_sim.bin")),
            user_sim_json_path: config
                .user_sim_json_path
                .unwrap_or_else(|| output_path(&output_dir, "user_sim.json")),
            db_pickle_path: config
                .db_pickle_path
                .unwrap_or_else(|| output_path(&output_dir, "db.p")),
            db_serve_path: config
                .db_serve_path
                .unwrap_or_else(|| output_path(&output_dir, "db2.p")),
            serve_cache_path: config
                .serve_cache_path
                .unwrap_or_else(|| output_path(&output_dir, "serve_cache.p")),
            ingest_jobs_dir: config
                .ingest_jobs_dir
                .unwrap_or_else(|| output_path(&output_dir, "ingest_jobs")),
            download_settings_path: config
                .download_settings_path
                .unwrap_or_else(|| output_path(&output_dir, "download_settings.json")),
            ui_settings_path: config
                .ui_settings_path
                .unwrap_or_else(|| output_path(&output_dir, "ui_settings.json")),
        };

        resolved.validate()?;
        Ok(resolved)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.output_dir.as_os_str().is_empty() {
            return Err("Config invariant failed: output_dir cannot be empty".to_string());
        }
        if self.db_path.as_os_str().is_empty()
            || self.database_path.as_os_str().is_empty()
            || self.tfidf_meta_path.as_os_str().is_empty()
        {
            return Err("Config invariant failed: key file paths cannot be empty".to_string());
        }
        Ok(())
    }
}
