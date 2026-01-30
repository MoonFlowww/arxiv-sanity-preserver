# arxiv sanity preserver

**Update Nov 27, 2021**: you may wish to look at my from-scratch re-write of
arxiv-sanity: [arxiv-sanity-lite](https://github.com/karpathy/arxiv-sanity-lite). It is a smaller version of
arxiv-sanity that focuses on the core value proposition, is significantly less likely to ever go down, scales better,
and has a few additional goodies such as multiple possible tags per account, regular emails of new papers of interest,
etc. It is also running live on [arxiv-sanity-lite.com](https://arxiv-sanity-lite.com).

This project is a web interface that attempts to tame the overwhelming flood of papers on Arxiv. It allows researchers
to keep track of recent papers, search for papers, sort papers by similarity to any paper, see recent popular papers, to
add papers to a personal library, and to get personalized recommendations of (new or old) Arxiv papers. This code is
currently running live at [www.arxiv-sanity.com/](http://www.arxiv-sanity.com/), where it's serving 25,000+ Arxiv papers
from Machine Learning (cs.[CV|AI|CL|LG|NE]/stat.ML) over the last ~3 years. With this code base you could replicate the
website to any of your favorite subsets of Arxiv by simply changing the categories in `pipeline_config.json` or by
passing `--category-counts` to the Rust CLI.

![user interface](https://raw.github.com/karpathy/arxiv-sanity-preserver/master/ui.jpeg)

### Code layout

There are two large parts of the code:

**Rust pipeline + server**. The Rust CLI (`arxiv_sanity_pipeline`) handles the entire indexing pipeline and the Axum
server. It downloads papers, extracts text, builds TF-IDF vectors, computes similarity, generates thumbnails, and
serves the web UI. See the Rust CLI docs for subcommands and flags.

**User interface**. Then there is a web server (based on Rust/Axum/sqlite) that allows searching through the
database and filtering papers by similarity, etc.

### Rust CLI documentation

For full CLI flags and subcommand details, see:

* [`docs/cli.md`](docs/cli.md)
* [`CLI_FLAGS.md`](CLI_FLAGS.md) (quick reference)

### Dependencies

You will need the Rust toolchain to build the CLI and server. Install Rust via
[rustup](https://rustup.rs/) or your package manager.

The pipeline also shells out to a few system tools:

* **pdftotext** (from `poppler-utils`) for extracting text from PDFs.
* **ImageMagick** (`convert` + `montage`) for rendering thumbnails.
* **sqlite3** for the user database.

On Ubuntu, install them with:

```bash
sudo apt-get install poppler-utils imagemagick sqlite3
```

### Processing pipeline

The processing pipeline requires you to run a series of CLI commands. In order, the processing pipeline is:

1. Run `arxiv_sanity_pipeline fetch-papers` to query the arxiv API and create a file
   containing metadata for each paper. The fetcher writes `db.jsonl` (one JSON record per paper). This is the step where
   you would modify the **query**, indicating which parts of arxiv you'd like to use. Note that if
   you're trying to pull too many papers arxiv will start to rate limit you. You may have to run the command multiple
   times, and I recommend using the arg `--start-index` to restart where you left off when you were last interrupted by
   arxiv.

    * **Per-category limits (defaulted)**: By default, the command fetches per-category using
      `--category-counts "cs.AI=2000,cs.LG=2000,stat.ML=2000,cs.IT=1500,eess.SP=1500,cs.NE=1000,cs.CL=1000,cs.CV=1500,cond-mat.stat-mech=500,q-fin.TR=2000,q-fin.RM=2000,q-fin.ST=2000"`,
      which totals roughly 19k papers focused on Quantitative Finance and Computer Vision. Override the defaults with
      your own comma-separated `category=count` list (e.g., `cs.AI=50,cs.CL=20`), or set `--category-counts ""` to fall
      back to the combined `--search-query`.

    * **Example (20k target with Quantitative Finance + Computer Vision focus)**:

      ```bash
      arxiv_sanity_pipeline fetch-papers --category-counts \
      "cs.AI=2000,cs.LG=2000,stat.ML=2000,cs.IT=1500,eess.SP=1500,cs.NE=1000,cs.CL=1000,cs.CV=1500,cond-mat.stat-mech=500,q-fin.TR=2000,q-fin.RM=2000,q-fin.ST=2000"
      ```

    * **Deduplication**: Downloads are keyed by arXiv ID, so a paper listed in multiple categories is saved once (the
      command updates only if a newer version appears).
    * **Migrating db.p to db.jsonl**: If you have an existing pickle database, convert it with
      `arxiv_sanity_pipeline migrate-db --input db.p --output db.jsonl`.
    * **Migrating analysis pickles to JSON**: If you need JSON versions of `tfidf_meta.p`,
      `sim_dict.p`, or `user_sim.p`, run `arxiv_sanity_pipeline migrate-analysis --allow-missing` to emit
      `tfidf_meta.json`, `sim_dict.json`, and `user_sim.json`.
2. Run `arxiv_sanity_pipeline download-pdfs`, which iterates over all papers in parsed JSONL and downloads the papers
   into folder `pdf`.
3. Run `arxiv_sanity_pipeline parse-pdf-to-text` to export all text from pdfs to files in `txt`.
4. Run `arxiv_sanity_pipeline thumb-pdf` to export thumbnails of all pdfs to `thumb`.
5. Run `arxiv_sanity_pipeline analyze` to compute tfidf vectors for all documents based on bigrams. Saves
   `tfidf.bin`, `tfidf_meta.json`, and `sim_dict.json`.
6. Run `arxiv_sanity_pipeline buildsvm` to train SVMs for all users (if any), exports `user_sim.json`.
7. Run `arxiv_sanity_pipeline make-cache` for various preprocessing so that server starts faster (and make sure to run
   `sqlite3 as.db < schema.sql` if this is the very first time ever you're starting arxiv-sanity, which initializes an
   empty database).
8. (Optional) Run `arxiv_sanity_pipeline reset-pipeline --force` to remove generated pipeline outputs and cached serve
   artifacts before starting fresh.
9. Start the mongodb daemon in the background. Mongodb can be installed by following the instructions
   here - https://docs.mongodb.com/tutorials/install-mongodb-on-ubuntu/.

* Start the mongodb server with - `sudo service mongod start`.
* Verify if the server is running in the background : The last line of /var/log/mongodb/mongod.log file must be -
  `[initandlisten] waiting for connections on port <port> `

10. Run the server with `arxiv_sanity_pipeline serve`. Visit localhost:5000 and enjoy sane viewing of papers!

I have a simple shell script that runs these commands one by one, and every day I run this script to fetch new papers,
incorporate them into the database, and recompute all tfidf vectors/classifiers. More details on this process below.

### Artifact formats

The Rust server and cache builder consume a small set of artifacts. The table below inventories the consumers and the
formats/fields they require.

| Artifact | Format | Produced by | Consumer | Required fields/keys |
| --- | --- | --- | --- | --- |
| `db.jsonl` | JSONL | `arxiv_sanity_pipeline fetch-papers` | `arxiv_sanity_pipeline make-cache` | Paper dicts must include `_rawid`, `_version`, `title`, `authors` (list of `{name}`), `summary`, `tags` (list of `{term}`), `updated`, `published`, `link`, and `arxiv_primary_category.term`. Missing citation-related fields are filled later. |
| `tfidf_meta.json` | JSON | `arxiv_sanity_pipeline analyze` | `arxiv_sanity_pipeline make-cache`, `arxiv_sanity_pipeline serve` | `vocab` (term → index) and `idf` (list of floats). |
| `sim_dict.json` | JSON | `arxiv_sanity_pipeline analyze` | `arxiv_sanity_pipeline serve` | Mapping of `paper_id+version` → list of nearest-neighbor ids. |
| `user_sim.json` | JSON | `arxiv_sanity_pipeline buildsvm` | `arxiv_sanity_pipeline serve` | Mapping of `user_id` → list of recommended paper ids. |
| `serve_cache.p` | Pickle | `arxiv_sanity_pipeline make-cache` | `arxiv_sanity_pipeline serve` | `date_sorted_pids`, `search_dict`, `library_sorted_pids`, `top_sorted_pids`. |
| `db2.p` | Pickle | `arxiv_sanity_pipeline make-cache` | `arxiv_sanity_pipeline serve` | Enriched paper dicts with `time_updated`, `time_published`, `citation_count`, `impact_score`, `years_since_pub`, and `tscore` added to the original schema. |
| `data/ingest_jobs/*.json` | JSON | `arxiv_sanity_pipeline serve` | `arxiv_sanity_pipeline serve` | Ingest/recompute job status and progress metadata. |

### Running online

If you'd like to run the server online (e.g. AWS) run it as `arxiv_sanity_pipeline serve --prod`.

You also want to create a `secret_key.txt` file and fill it with random text.

### Current workflow

Running the site live is not currently set up for a fully automatic plug and play operation. Instead it's a bit of a
manual process and I thought I should document how I'm keeping this code alive right now. I have a script that performs
the following update early morning after arxiv papers come out (~midnight PST):

```bash
arxiv_sanity_pipeline fetch-papers
arxiv_sanity_pipeline download-pdfs
arxiv_sanity_pipeline parse-pdf-to-text
arxiv_sanity_pipeline thumb-pdf
arxiv_sanity_pipeline analyze
arxiv_sanity_pipeline buildsvm
arxiv_sanity_pipeline make-cache
```

### Adding a single paper by arXiv id

If you want to ingest one paper outside of the regular daily crawl, you can
run the helper command and type (or pass) the arXiv identifier:

```bash
arxiv_sanity_pipeline ingest-single-paper 1512.08756v2
```

The command pulls the metadata from arXiv, downloads the PDF, extracts text,
creates thumbnails, and then reruns the analysis/cache steps so the new paper
is immediately incorporated into the database.

I run the server in a screen session, so `screen -S serve` to create it (or `-r` to reattach to it) and run:

```bash
arxiv_sanity_pipeline serve --prod --port 80
```

The server will load the new files and begin hosting the site. Note that on some systems you can't use port 80 without
`sudo`. Your two options are to use `iptables` to reroute ports or you can
use [setcap](http://stackoverflow.com/questions/413807/is-there-a-way-for-non-root-processes-to-bind-to-privileged-ports-1024-on-l)
to elavate the permissions of your `arxiv_sanity_pipeline` binary. In this case I'd recommend careful
permissions and a dedicated service user.
