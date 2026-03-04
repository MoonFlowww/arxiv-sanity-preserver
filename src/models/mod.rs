use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadTopicInput {
    pub start: Option<String>,
    pub end: Option<String>,
    pub published: Option<bool>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DownloadSettingsPayload {
    pub selected_topics: Vec<String>,
    pub topics: HashMap<String, DownloadTopicInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestArxivForm {
    pub paper_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorResponse {
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OkResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OkMessageResponse {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub version: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestCreatedResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecomputeStatusApiResponse {
    pub status: String,
    pub message: Option<String>,
    pub percent: Value,
    pub updated_at: i64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeBinCount {
    pub bin: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PageBinCount {
    pub pages: i64,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NoteCoverage {
    pub with_note: i64,
    pub without_note: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetadataCoverage {
    pub pages_with_metadata: i64,
    pub pages_missing_metadata: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PapersMetricsResponse {
    pub total: usize,
    pub time_bin: String,
    pub time_series: Vec<TimeBinCount>,
    pub page_histogram: Vec<PageBinCount>,
    pub note_coverage: NoteCoverage,
    pub metadata_coverage: MetadataCoverage,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaperAuthor {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaperTerm {
    #[serde(default)]
    pub term: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaperRecord {
    #[serde(default)]
    pub _rawid: String,
    #[serde(default)]
    pub _version: i64,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub arxiv_primary_category: Option<PaperTerm>,
    #[serde(default)]
    pub authors: Vec<PaperAuthor>,
    #[serde(default)]
    pub link: String,
    #[serde(default)]
    pub citation_count: Option<i64>,
    #[serde(default)]
    pub impact_score: Option<f64>,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub tags: Vec<PaperTerm>,
    #[serde(default)]
    pub repo_links: Vec<String>,
    #[serde(default)]
    pub is_opensource: bool,
    #[serde(default)]
    pub updated: Option<String>,
    #[serde(default)]
    pub published: Option<String>,
    #[serde(default)]
    pub arxiv_comment: Option<String>,
    #[allow(dead_code)]
    #[serde(flatten)]
    pub legacy: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperApiRecord {
    pub title: String,
    pub pid: String,
    pub rawpid: String,
    pub category: String,
    pub authors: Vec<String>,
    pub link: String,
    pub in_library: i32,
    pub citation_count: Option<i64>,
    pub impact_score: Option<f64>,
    #[serde(rename = "abstract")]
    pub abstract_text: String,
    pub img: String,
    pub tags: Vec<String>,
    pub repo_links: Vec<String>,
    pub is_opensource: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub originally_published_time: Option<String>,
    pub comment: String,
}
