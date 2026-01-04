"""
Ingest a single arXiv paper end-to-end through the existing pipeline.

The script fetches the paper metadata, downloads its PDF, extracts text,
creates a thumbnail, and finally recomputes the caches so the paper is
immediately available to the web server.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Optional, Tuple

import feedparser

from fetch_papers import encode_feedparser_dict, parse_arxiv_url
from repo_metadata import build_repo_metadata
from utils import Config, isvalidid, safe_pickle_dump
import pickle


def _load_database():
    try:
        return pickle.load(open(Config.db_path, "rb"))
    except Exception:
        return {}


def _fetch_metadata(paper_id: str) -> Tuple[dict, dict]:
    """Fetch metadata for a single paper via the arXiv API."""
    base_url = "http://export.arxiv.org/api/query?id_list=%s" % paper_id
    with urllib.request.urlopen(base_url) as url:
        response = url.read()
    parsed = feedparser.parse(response)
    if not parsed.entries:
        raise ValueError(f"No paper found for id {paper_id}")

    entry = encode_feedparser_dict(parsed.entries[0])
    rawid, version = parse_arxiv_url(entry["id"])
    entry["_rawid"] = rawid
    entry["_version"] = version
    return entry, parsed


def _ensure_pdf(entry: dict) -> str:
    os.makedirs(Config.pdf_dir, exist_ok=True)
    pdfs = [x["href"] for x in entry.get("links", []) if x.get("type") == "application/pdf"]
    if not pdfs:
        raise ValueError("No PDF link found in entry")

    pdf_url = (pdfs[0] + ".pdf").replace("://arxiv.org", "://export.arxiv.org")
    basename = pdf_url.split("/")[-1]
    fname = os.path.join(Config.pdf_dir, basename)
    if os.path.isfile(fname):
        print(f"PDF already present at {fname}")
        return fname

    print(f"Fetching PDF {pdf_url}")
    req = urllib.request.Request(
        pdf_url, headers={"User-Agent": "arxiv-sanity-preserver (contact: you@example.com)"}
    )
    with urllib.request.urlopen(req, None, 15) as resp:
        data = resp.read()
        content_type = resp.headers.get("Content-Type", "").lower()
        if "text/html" in content_type or data.lstrip().lower().startswith(b"<html"):
            raise RuntimeError(f"Received HTML instead of PDF for {pdf_url}")
        with open(fname, "wb") as fp:
            fp.write(data)
    time.sleep(0.5)
    return fname


def _ensure_text(pdf_path: str, entry: dict) -> str:
    if not shutil.which("pdftotext"):
        raise RuntimeError("pdftotext is required to extract text")
    os.makedirs(Config.txt_dir, exist_ok=True)

    pdf_basename = os.path.basename(pdf_path)
    txt_path = os.path.join(Config.txt_dir, pdf_basename + ".txt")
    if os.path.isfile(txt_path):
        print(f"Text already exists at {txt_path}")
        return txt_path

    subprocess.run(["pdftotext", pdf_path, txt_path], check=False)
    if not os.path.isfile(txt_path):
        print(f"Could not parse {pdf_basename} to text; creating empty placeholder")
        open(txt_path, "w").close()
    return txt_path


class ThumbnailPolicyError(RuntimeError):
    def __init__(self, message: str, thumb_path: str):
        super().__init__(message)
        self.thumb_path = thumb_path


def _is_imagemagick_policy_denial(stderr_output: str) -> bool:
    lowered = stderr_output.lower()
    return "security policy" in lowered or "not allowed by the security policy 'pdf'" in lowered

def _render_thumbnail_with_pdftoppm(pdf_path: str, thumb_path: str) -> bool:
    if not shutil.which("pdftoppm"):
        print("Warning: pdftoppm is unavailable; cannot render thumbnail without ImageMagick PDF support.")
        return False

    pdftoppm_cmd = [
        "pdftoppm",
        "-f",
        "1",
        "-l",
        "8",
        "-png",
        pdf_path,
        os.path.join(Config.tmp_dir, "thumb"),
    ]
    pdftoppm_proc = subprocess.run(pdftoppm_cmd, check=False, capture_output=True, text=True)
    if pdftoppm_proc.returncode != 0:
        print(
            "Warning: pdftoppm failed to render thumbnail; "
            "cannot render thumbnail without ImageMagick PDF support."
        )
        return False

    for i in range(1, 9):
        source = os.path.join(Config.tmp_dir, f"thumb-{i}.png")
        target = os.path.join(Config.tmp_dir, f"thumb-{i - 1}.png")
        if os.path.isfile(source):
            os.replace(source, target)

    first_thumb = os.path.join(Config.tmp_dir, "thumb-0.png")
    if not os.path.isfile(first_thumb):
        return False

    montage_cmd = [
        "montage",
        "-mode",
        "concatenate",
        "-quality",
        "80",
        "-tile",
        "x1",
        os.path.join(Config.tmp_dir, "thumb-*.png"),
        thumb_path,
    ]
    subprocess.run(montage_cmd, check=False)
    return os.path.isfile(thumb_path)

def _ensure_thumbnail(pdf_path: str) -> str:
    if not shutil.which("convert"):
        raise RuntimeError("ImageMagick's convert is required for thumbnails")

    os.makedirs(Config.thumbs_dir, exist_ok=True)
    os.makedirs(Config.tmp_dir, exist_ok=True)

    pdf_basename = os.path.basename(pdf_path)
    thumb_path = os.path.join(Config.thumbs_dir, pdf_basename + ".jpg")
    if os.path.isfile(thumb_path):
        print(f"Thumbnail already exists at {thumb_path}")
        return thumb_path

    # Clear temporary files from previous runs
    for i in range(8):
        for prefix in ("thumb", "thumbbuf"):
            tmp_file = os.path.join(Config.tmp_dir, f"{prefix}-{i}.png")
            if os.path.isfile(tmp_file):
                os.remove(tmp_file)

    convert_proc = subprocess.Popen(
        [
            "convert",
            f"{pdf_path}[0-7]",
            "-thumbnail",
            "x156",
            os.path.join(Config.tmp_dir, "thumb-%d.png"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout_output, stderr_output = convert_proc.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        convert_proc.terminate()
        stdout_output, stderr_output = convert_proc.communicate()
        raise RuntimeError("Thumbnail generation timed out")

    if convert_proc.returncode != 0 or _is_imagemagick_policy_denial(stderr_output):
        policy_message = (
            "ImageMagick PDF policy blocked conversion; update policy.xml to allow PDF or use an alternate renderer."
        )
        if _is_imagemagick_policy_denial(stderr_output):
            if _render_thumbnail_with_pdftoppm(pdf_path, thumb_path):
                return thumb_path
            missing_thumb_path = os.path.join("static", "missing.svg")
            shutil.copy(missing_thumb_path, thumb_path)
            raise ThumbnailPolicyError(policy_message, thumb_path)
        raise RuntimeError(
            "Thumbnail generation failed with exit code %s.\nstdout: %s\nstderr: %s"
            % (convert_proc.returncode, stdout_output.strip(), stderr_output.strip())
        )

    first_thumb = os.path.join(Config.tmp_dir, "thumb-0.png")
    if not os.path.isfile(first_thumb):
        missing_thumb_path = os.path.join("static", "missing.svg")
        shutil.copy(missing_thumb_path, thumb_path)
    else:
        montage_cmd = [
            "montage",
            "-mode",
            "concatenate",
            "-quality",
            "80",
            "-tile",
            "x1",
            os.path.join(Config.tmp_dir, "thumb-*.png"),
            thumb_path,
        ]
        subprocess.run(montage_cmd, check=False)
    return thumb_path


def _recompute_caches():
    print("Recomputing tfidf and similarity caches... this may take a while")
    subprocess.run([sys.executable, "analyze.py"], check=True)
    subprocess.run([sys.executable, "buildsvm.py"], check=False)
    subprocess.run([sys.executable, "make_cache.py"], check=True)


def ingest_paper(paper_id: str, progress_callback=None, recompute_caches: bool = True):
    """Ingest a paper and emit progress updates when a callback is provided."""

    def emit(label: str, percent: int, message: Optional[str] = None, warning: bool = False):
        if progress_callback:
            progress_callback(label, percent, message, warning=warning)

    emit("Validating identifier...", 5)
    if not isvalidid(paper_id):
        emit("Invalid arXiv identifier", 100, f"{paper_id} is not valid")
        raise ValueError(f"{paper_id} is not a valid arXiv identifier")

    emit("Loading local database...", 10)
    db = _load_database()

    emit("Fetching arXiv metadata...", 20)
    entry, _ = _fetch_metadata(paper_id)
    repo_metadata = build_repo_metadata(entry)
    entry.update(repo_metadata)
    rawid = entry["_rawid"]
    existing = db.get(rawid)
    if existing and existing.get("_version", 0) >= entry["_version"]:
        msg = f"Paper {paper_id} already present with version {existing['_version']}, refreshing assets only"
        print(msg)
        emit("Refreshing existing assets...", 25, msg)
        updated = False
        for key, value in repo_metadata.items():
            if existing.get(key) != value:
                existing[key] = value
                updated = True
        if updated:
            db[rawid] = existing
            safe_pickle_dump(db, Config.db_path)
            emit("Updated repository metadata", 28, "Persisted repository metadata")
    else:
        db[rawid] = entry
        safe_pickle_dump(db, Config.db_path)
        msg = f"Saved metadata for {rawid}v{entry['_version']}"
        print(msg)
        emit("Saved metadata", 30, msg)

    emit("Downloading PDF...", 40)
    pdf_path = _ensure_pdf(entry)
    emit("Extracting text from PDF...", 55)
    txt_path = _ensure_text(pdf_path, entry)
    emit("Generating thumbnail...", 70)
    try:
        thumb_path = _ensure_thumbnail(pdf_path)
    except ThumbnailPolicyError as exc:
        emit("Generating thumbnail...", 70, f"Thumbnail warning: {exc}", warning=True)
        thumb_path = exc.thumb_path

    print(f"PDF stored at: {pdf_path}")
    print(f"Text stored at: {txt_path}")
    print(f"Thumbnail stored at: {thumb_path}")

    if recompute_caches:
        emit("Recomputing caches...", 85)
        _recompute_caches()
        emit("Finished", 100, "Ingest complete")
    else:
        emit("Skipping cache recompute...", 90, "Recompute queued to run in background.")
        emit("Finished", 100, "Ingest complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest a single arXiv paper end-to-end")
    parser.add_argument("paper_id", nargs="?", help="arXiv identifier, e.g., 1512.08756v2")
    parser.add_argument(
        "--no-recompute",
        action="store_true",
        help="Skip recomputing caches (useful when triggering recompute separately).",
    )
    args = parser.parse_args()

    paper_id = args.paper_id
    if paper_id is None:
        paper_id = input("Enter arXiv id (e.g., 1512.08756v2): ").strip()

    ingest_paper(paper_id, recompute_caches=not args.no_recompute)
