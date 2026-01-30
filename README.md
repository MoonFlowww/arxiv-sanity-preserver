# arxiv sanity preserver

A Rust-first rebuild of arxiv-sanity that bundles the data pipeline and the web UI in one CLI. It fetches arXiv
metadata, downloads PDFs, extracts text, builds TF‑IDF vectors + an HNSW similarity index, and serves the Axum web app.
Everything is stored in a local pipeline directory (default: `.pipeline`).

![user interface](https://raw.github.com/karpathy/arxiv-sanity-preserver/master/ui.jpeg)

## What’s in this repo

- **Rust CLI + server** (`arxiv_sanity_pipeline`): one binary that runs the pipeline and serves the UI.
- **Axum web app**: search, filter, similarity browsing, personal library, and recommendations.
- **SQLite user DB**: local database for users + saved papers.
- **Config-driven pipeline**: all paths live in `pipeline_config.json`.

## Key features
- **ArXiv ingestion pipeline**: metadata fetch, PDF download, text extraction, thumbnails, and analysis.
- **TF‑IDF + HNSW similarity**: fast nearest-neighbor lookups for “similar papers”.
- **Personalized recommendations**: SVMs trained from your library selections.
- **OpenAlex enrichment**: citations / publication metadata used to compute impact scores.
- **Single-paper ingest**: add a paper on demand without a full crawl.
- **Twitter daemon (optional)**: track tweets that mention arxiv.org.

## Requirements
- Rust toolchain (install via [rustup](https://rustup.rs/)).
- System tools used by the pipeline:
    - **pdftotext** (`poppler-utils`) for PDF text extraction.
    - **ImageMagick** (`convert` + `montage`) for thumbnails.
    - **sqlite3** for the user database.

On Ubuntu:

```bash
sudo apt-get install poppler-utils imagemagick sqlite3
```
## Quick start

1. **Build the CLI**
   ```bash
   cargo build --release
   ```
2. **Initialize the SQLite database** (required before `buildsvm` / `run-all`)

   ```bash
   sqlite3 .pipeline/as.db < schema.sql
   ```

3. **Run the full pipeline** (fetch → download → parse → thumbnails → analyze → SVM → cache)

   ```bash
   ./target/release/arxiv_sanity_pipeline run-all
   ```

4. **Start the server**

   ```bash
   ./target/release/arxiv_sanity_pipeline serve --port 5000
   ```

Then open <http://localhost:5000>.

## Pipeline commands

You can run any step independently:

```bash
arxiv_sanity_pipeline fetch-papers
arxiv_sanity_pipeline download-pdfs
arxiv_sanity_pipeline parse-pdf-to-text
arxiv_sanity_pipeline thumb-pdf
arxiv_sanity_pipeline analyze
arxiv_sanity_pipeline buildsvm
arxiv_sanity_pipeline make-cache
```

### Ingest a single paper
```bash
arxiv_sanity_pipeline ingest-single-paper 1512.08756v2
```

By default this refreshes the TF‑IDF vectors, HNSW index, and caches. Use `--no-recompute` to skip cache rebuild.

### Resetting pipeline artifacts

If you need to wipe generated data:

```bash
arxiv_sanity_pipeline reset-pipeline --force
```

## Configuration

The CLI reads `pipeline_config.json` by default. It controls output paths and the pipeline directory.
You can point to a custom config with `--config` or emit the resolved config with `--write-config`.

Default output layout (see `pipeline_config.json`):

```
.pipeline/
  db.jsonl
  pdf/
  txt/
  thumb/
  tfidf.bin
  tfidf_meta.json
  hnsw_index.bin
  user_sim.bin
  as.db
  serve_cache.p
  db2.p
  download_settings.json
  ingest_jobs/
```

## Artifacts produced by the pipeline

| Artifact | Produced by | Purpose |
| --- | --- | --- |
| `db.jsonl` | `fetch-papers` | arXiv metadata (one JSON record per paper). |
| `pdf/` | `download-pdfs` | Downloaded PDFs. |
| `txt/` | `parse-pdf-to-text` | Extracted text for analysis. |
| `thumb/` | `thumb-pdf` | Thumbnail images. |
| `tfidf.bin` | `analyze` | TF‑IDF vectors (bincode). |
| `tfidf_meta.json` | `analyze` | Vocabulary + IDF + PID mappings. |
| `hnsw_index.bin` | `analyze` | HNSW index for fast similarity search (the sole similarity mechanism). |
| `user_sim.bin` | `buildsvm` | Per-user recommendation lists. |
| `serve_cache.p` / `db2.p` | `make-cache` | Server-ready caches + enriched paper metadata. |
| `download_settings.json` | server | Topic/date filters used for PDF download behavior. |
| `.pipeline/ingest_jobs/*.json` | server | Background ingest + recompute job status. |

For a complete CLI flag reference, see [`CLI_FLAGS.md`](CLI_FLAGS.md).

## Optional: Twitter daemon

Collect tweets mentioning arxiv.org (requires a Twitter API bearer token):

```bash
export TWITTER_BEARER_TOKEN=... 
arxiv_sanity_pipeline twitter-daemon --query "arxiv.org"
```

Outputs JSONL by default to `twitter_recent.jsonl`.

## Docker

Build and run the bundled container:

```bash
docker build -t arxiv-sanity .
docker run --rm -p 5000:5000 arxiv-sanity
```

## Notes

- The server runs in “single-user” mode by default. You can override the default username with
  `ASP_SINGLE_USER_NAME`.
- `make-cache` fetches OpenAlex metadata (citations / publication status) for papers missing those fields, so expect
  network access and a short delay between requests.
- For additional CLI documentation, see [`docs/cli.md`](docs/cli.md).