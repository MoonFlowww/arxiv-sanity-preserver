# Rust CLI guide

The Rust CLI lives at `arxiv_sanity_pipeline` and powers both the pipeline and the server. Build it with:

```bash
cargo build --release
```

## Subcommands

The pipeline is composed of subcommands that can be run independently or via the `run-all` convenience wrapper (there are no standalone binaries for each step).

* `fetch-papers` — Query arXiv and produce `db.jsonl` metadata.
* `download-pdfs` — Download PDFs for the papers in `db.jsonl`.
* `parse-pdf-to-text` — Extract text into `txt/`.
* `thumb-pdf` — Generate thumbnails in `thumb/`.
* `analyze` — Build TF-IDF vectors + similarity artifacts.
* `buildsvm` — Train per-user SVM recommendations.
* `make-cache` — Build server cache artifacts.
* `ingest-single-paper` — End-to-end ingest for a single paper.
* `migrate-analysis` — Convert legacy analysis pickles into JSON artifacts.
* `migrate-db` — Convert `db.p` pickle into `db.jsonl`.
* `serve` — Run the Axum server.
* `twitter-daemon` — Collect recent tweets mentioning arxiv.org.
* `run-all` — Run the full pipeline in sequence.

For the exhaustive flag reference (defaults + descriptions), see [`CLI_FLAGS.md`](../CLI_FLAGS.md).

## External dependencies

The CLI shells out to a few system tools:

* **pdftotext** (from `poppler-utils`) for extracting text from PDFs.
* **ImageMagick** for rendering thumbnails (`convert` and `montage` commands).
  * If you see ImageMagick PDF policy errors, update `policy.xml` to allow PDF rendering.
* **sqlite3** for the user database.

Install them on Ubuntu with:

```bash
sudo apt-get install poppler-utils imagemagick sqlite3
```

## Rust analysis artifacts

The `analyze` and `buildsvm` subcommands produce the following artifacts:

* `tfidf.bin` (`PipelineConfig.tfidf_path`): `bincode`-serialized `TfidfMatrix { vectors: Vec<Vec<f32>> }` (L2-normalized TF-IDF vectors).
* `tfidf_meta.json` (`PipelineConfig.tfidf_meta_path`): `TfidfMeta` serialized with `serde_json` containing `vocab`, `idf`, `pids`, and `ptoi`.
* `sim_dict.bin` (`PipelineConfig.sim_dict_path`): `bincode`-serialized `HashMap<String, Vec<String>>` mapping each paper id+version to its nearest neighbors.
* `user_sim.bin` (`PipelineConfig.user_sim_path`): `bincode`-serialized `HashMap<i64, Vec<String>>` mapping user IDs to recommended paper IDs.

Tokenization matches the historical regex (`\b[a-zA-Z_][a-zA-Z0-9_]+\b`), lowercases, filters English stop words, builds unigrams + bigrams, applies sublinear TF with smoothed IDF, and L2-normalizes the vectors.

## Migrating legacy pickle artifacts

If you need to migrate legacy pickles without recomputation, use:

```bash
arxiv_sanity_pipeline migrate-analysis --allow-missing
arxiv_sanity_pipeline migrate-db --input db.p --output db.jsonl
```

This emits JSON artifacts (`tfidf_meta.json`, `sim_dict.json`, `user_sim.json`) that the Rust server can read.
