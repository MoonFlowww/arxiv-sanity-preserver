# CLI flags and defaults (Rust CLI)

This document summarizes the CLI flags, defaults, and help text for the Rust `arxiv_sanity_pipeline` CLI in this repo.

## Global flags

These flags apply to all subcommands.

| Flag | Default | Help |
| --- | --- | --- |
| `--config` | `pipeline_config.json` | `Path to pipeline config.` |
| `--write-config` | `false` | `Write the resolved pipeline config to disk before running.` |

## `fetch-papers`

| Flag | Default | Help |
| --- | --- | --- |
| `--search-query` | `cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML+OR+cat:cs.IT+OR+cat:eess.SP+OR+cat:q-fin.TR+OR+cat:q-fin.RM+OR+cat:q-fin.ST+OR+cat:cond-mat.stat-mech` | `query used for arxiv API. See http://arxiv.org/help/api/user-manual#detailed_examples` |
| `--start-index` | `0` | `0 = most recent API result` |
| `--max-index` | `10000` | `upper bound on paper index we will fetch` |
| `--results-per-iteration` | `100` | `passed to arxiv API` |
| `--wait-time` | `5.0` | `lets be gentle to arxiv API (in number of seconds)` |
| `--break-on-no-added` | `1` | `break out early if all returned query papers are already in db? 1=yes, 0=no` |
| `--category-counts` | `cs.AI=2000,cs.LG=2000,stat.ML=2000,cs.IT=1500,eess.SP=1500,cs.NE=1000,cs.CL=1000,cs.CV=1500,cond-mat.stat-mech=500,q-fin.TR=2000,q-fin.RM=2000,q-fin.ST=2000` | `Comma-separated list like "cs.AI=50,cs.CL=20" to fetch latest papers per category. Overrides --search-query.` |

## `download-pdfs`

No flags.

## `parse-pdf-to-text`

| Flag | Default | Help |
| --- | --- | --- |
| `--pdf-dir` | _none_ | `Directory containing input PDFs` |
| `--txt-dir` | _none_ | `Directory for output text files` |

## `thumb-pdf`

| Flag | Default | Help |
| --- | --- | --- |
| `--pdf-dir` | _none_ | `Directory containing input PDFs` |
| `--thumb-dir` | _none_ | `Directory for output thumbnails` |
| `--tmp-dir` | _none_ | `Directory for temporary thumbnail files` |

## `analyze`

No flags.

## `buildsvm`

No flags.

## `make-cache`

No flags.

## `ingest-single-paper`

| Flag | Default | Help |
| --- | --- | --- |
| `paper_id` (positional) | _none_ (prompts if omitted) | `arXiv identifier, e.g., 1512.08756v2` |
| `--no-recompute` | `false` | `Skip recomputing caches (useful when triggering recompute separately).` |

## `migrate-analysis`

| Flag | Default | Help |
| --- | --- | --- |
| `--tfidf-meta-in` | `tfidf_meta.p` | `Path to tfidf_meta.p` |
| `--tfidf-meta-out` | `tfidf_meta.json` | `Path to tfidf_meta.json` |
| `--sim-in` | `sim_dict.p` | `Path to sim_dict.p` |
| `--sim-out` | `sim_dict.json` | `Path to sim_dict.json` |
| `--user-sim-in` | `user_sim.p` | `Path to user_sim.p` |
| `--user-sim-out` | `user_sim.json` | `Path to user_sim.json` |
| `--allow-missing` | `false` | `Skip missing input files instead of failing` |

## `migrate-db`

| Flag | Default | Help |
| --- | --- | --- |
| `--input` | `db.p` | `Path to db.p pickle file` |
| `--output` | `db.jsonl` | `Path to output JSONL file` |

## `serve`

| Flag | Default | Help |
| --- | --- | --- |
| `-p`, `--prod` | `false` | `run in prod?` |
| `-r`, `--num_results` | `200` | `number of results to return per query` |
| `--port` | `5000` | `port to serve on` |

## `twitter-daemon`

| Flag | Default | Help |
| --- | --- | --- |
| `--query` | `arxiv.org` | `Search query to use for the Twitter API` |
| `--sleep-seconds` | `600` | `Time to wait between polling Twitter` |
| `--max-results` | `100` | `Maximum results to request per Twitter API call` |
| `--output` | `twitter_recent.jsonl` | `Path to write tweet metadata` |

## `run-all`

Convenience wrapper that runs the full pipeline (fetch + download + parse + thumb + analyze + buildsvm + make-cache).
Accepts the same flags as `fetch-papers` to control the ingest stage.

## `reset-pipeline`

| Flag | Default | Help |
| --- | --- | --- |
| `--force` | `false` | `Allow deletion of pipeline outputs and cached data.` |
