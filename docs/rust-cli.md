# arxiv_sanity_pipeline (Rust)

This crate provides the Rust CLI for the arxiv-sanity pipeline steps, including PDF text extraction, thumbnail generation,
and the Axum server.

## Documentation

The top-level CLI guide lives in [`cli.md`](cli.md) and the flag reference lives in
[`CLI_FLAGS.md`](../CLI_FLAGS.md).

## External dependencies

The Rust CLI shells out to a few system tools:

* **pdftotext** (from `poppler-utils`) for extracting text from PDFs.
* **ImageMagick** for rendering thumbnails (`convert` and `montage` commands).
  * If you see ImageMagick PDF policy errors, update `policy.xml` to allow PDF rendering.
* **sqlite3** for the user database.

Ensure these executables are available on your `PATH` before running `parse-pdf-to-text` or `thumb-pdf`.

## Rust analysis artifacts

The Rust `analyze` and `buildsvm` subcommands produce the following artifacts:

* `tfidf.bin` (`PipelineConfig.tfidf_path`): `bincode`-serialized `TfidfMatrix { vectors: Vec<Vec<f32>> }` (L2-normalized TF-IDF vectors).
* `tfidf_meta.json` (`PipelineConfig.tfidf_meta_path`): `TfidfMeta` serialized with `serde_json` containing `vocab`, `idf`, `pids`, and `ptoi`.
* `sim_dict.bin` (`PipelineConfig.sim_dict_path`): `bincode`-serialized `HashMap<String, Vec<String>>` mapping each paper id+version to its nearest neighbors.
* `user_sim.bin` (`PipelineConfig.user_sim_path`): `bincode`-serialized `HashMap<i64, Vec<String>>` mapping user IDs to recommended paper IDs.

Tokenization matches the historical regex (`\b[a-zA-Z_][a-zA-Z0-9_]+\b`), lowercases, filters English stop words, builds
unigrams + bigrams, applies sublinear TF with smoothed IDF, and L2-normalizes the vectors.

## Migrating legacy pickle artifacts

If you need to migrate legacy pickles without recomputation, use:

```bash
arxiv_sanity_pipeline migrate-analysis --allow-missing
arxiv_sanity_pipeline migrate-db --input db.p --output db.jsonl
```

This emits JSON artifacts (`tfidf_meta.json`, `sim_dict.json`, `user_sim.json`) that the Rust server can read.
