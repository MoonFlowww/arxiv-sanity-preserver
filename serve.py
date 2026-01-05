import argparse
import dateutil.parser
import json
import math
import numpy as np
import os
import pickle
import subprocess
import sys
import pymongo
import re
import threading
import time
import uuid
from flask import Flask, request, url_for, redirect, render_template, abort, g, flash, jsonify
from hashlib import md5
from random import shuffle, randrange, uniform
from sqlite3 import dbapi2 as sqlite3
from typing import Optional, Union

from flask_limiter import Limiter
from ingest_single_paper import ingest_paper
from utils import strip_version, isvalidid, Config, open_atomic

# various globals
# -----------------------------------------------------------------------------

# database configuration
# single-user mode constants
SINGLE_USER_ID = 1
SINGLE_USER_USERNAME = os.environ.get('ASP_SINGLE_USER_NAME', 'localuser')

if os.path.isfile('secret_key.txt'):
    SECRET_KEY = open('secret_key.txt', 'r').read()
else:
    SECRET_KEY = 'devkey, should be in a file'
app = Flask(__name__)
app.config.from_object(__name__)
limiter = Limiter(app, global_limits=["100 per hour", "20 per minute"])

ingest_jobs = {}
ingest_lock = threading.Lock()
ingest_jobs_dir = Config.ingest_jobs_dir
data_lock = threading.Lock()
recompute_lock = threading.Lock()
recompute_status = {}
recompute_status_path = os.path.join(ingest_jobs_dir, 'recompute_status.json')
recompute_stdout_path = os.path.join(ingest_jobs_dir, 'recompute_stdout.log')
recompute_stderr_path = os.path.join(ingest_jobs_dir, 'recompute_stderr.log')
recompute_thread = None
RECOMPUTE_STALE_SECONDS = 30 * 60

# topic browsing globals
TOPIC_PIDS = {}
TOPICS = []

SECONDS_PER_YEAR = 365.25 * 24 * 60 * 60

# mapping of arXiv category identifiers to human-readable labels
TOPIC_TRANSLATIONS = {
    'astro-ph.CO': 'Cosmology and Nongalactic Astrophysics',
    'astro-ph.GA': 'Astrophysics of Galaxies',
    'astro-ph.HE': 'High Energy Astrophysical Phenomena',
    'astro-ph.IM': 'Instrumentation and Methods for Astrophysics',
    'cond-mat.dis-nn': 'Disordered Systems and Neural Networks',
    'cond-mat.mes-hall': 'Mesoscale and Nanoscale Physics',
    'cond-mat.mtrl-sci': 'Materials Science',
    'cond-mat.quant-gas': 'Quantum Gases',
    'cond-mat.soft': 'Soft Condensed Matter',
    'cond-mat.stat-mech': 'Statistical Mechanics',
    'cond-mat.str-el': 'Strongly Correlated Electrons',
    'cs.AI': 'Artificial Intelligence',
    'cs.AR': 'Hardware Architecture',
    'cs.CC': 'Computational Complexity',
    'cs.CE': 'Computational Engineering, Finance, and Science',
    'cs.CG': 'Computational Geometry',
    'cs.CL': 'Computation and Language',
    'cs.CR': 'Cryptography and Security',
    'cs.CV': 'Computer Vision and Pattern Recognition',
    'cs.CY': 'Computers and Society',
    'cs.DB': 'Databases',
    'cs.DC': 'Distributed, Parallel, and Cluster Computing',
    'cs.DL': 'Digital Libraries',
    'cs.DM': 'Discrete Mathematics',
    'cs.DS': 'Data Structures and Algorithms',
    'cs.ET': 'Emerging Technologies',
    'cs.FL': 'Formal Languages and Automata Theory',
    'cs.GR': 'Graphics',
    'cs.GT': 'Computer Science and Game Theory',
    'cs.HC': 'Human-Computer Interaction',
    'cs.IR': 'Information Retrieval',
    'cs.IT': 'Information Theory',
    'cs.LG': 'Machine Learning',
    'cs.LO': 'Logic in Computer Science',
    'cs.MA': 'Multiagent Systems',
    'cs.MM': 'Multimedia',
    'cs.MS': 'Mathematical Software',
    'cs.NE': 'Neural and Evolutionary Computing',
    'cs.NI': 'Networking and Internet Architecture',
    'cs.OH': 'Other Computer Science',
    'cs.OS': 'Operating Systems',
    'cs.PF': 'Performance',
    'cs.PL': 'Programming Languages',
    'cs.RO': 'Robotics',
    'cs.SD': 'Sound',
    'cs.SE': 'Software Engineering',
    'cs.SI': 'Social and Information Networks',
    'econ.EM': 'Econometrics',
    'econ.GN': 'General Economics',
    'econ.TH': 'Theoretical Economics',
    'eess.AS': 'Audio and Speech Processing',
    'eess.IV': 'Image and Video Processing',
    'eess.SP': 'Signal Processing',
    'eess.SY': 'Systems and Control',
    'gr-qc': 'General Relativity and Quantum Cosmology',
    'hep-ex': 'High Energy Physics - Experiment',
    'hep-lat': 'High Energy Physics - Lattice',
    'hep-ph': 'High Energy Physics - Phenomenology',
    'hep-th': 'High Energy Physics - Theory',
    'math-ph': 'Mathematical Physics',
    'math.AC': 'Commutative Algebra',
    'math.AG': 'Algebraic Geometry',
    'math.AP': 'Analysis of PDEs',
    'math.CA': 'Classical Analysis and ODEs',
    'math.CO': 'Combinatorics',
    'math.CT': 'Category Theory',
    'math.DG': 'Differential Geometry',
    'math.DS': 'Dynamical Systems',
    'math.FA': 'Functional Analysis',
    'math.GM': 'General Mathematics',
    'math.GT': 'Geometric Topology',
    'math.HO': 'History and Overview',
    'math.MG': 'Metric Geometry',
    'math.NA': 'Numerical Analysis',
    'math.OC': 'Optimization and Control',
    'math.PR': 'Probability',
    'math.QA': 'Quantum Algebra',
    'math.RA': 'Rings and Algebras',
    'math.ST': 'Statistics Theory',
    'nlin.AO': 'Adaptation and Self-Organizing Systems',
    'nlin.CD': 'Chaotic Dynamics',
    'nlin.CG': 'Cellular Automata and Lattice Gases',
    'nlin.PS': 'Pattern Formation and Solitons',
    'nucl-th': 'Nuclear Theory',
    'physics.acc-ph': 'Accelerator Physics',
    'physics.ao-ph': 'Atmospheric and Oceanic Physics',
    'physics.bio-ph': 'Biological Physics',
    'physics.chem-ph': 'Chemical Physics',
    'physics.comp-ph': 'Computational Physics',
    'physics.data-an': 'Data Analysis, Statistics and Probability',
    'physics.ed-ph': 'Physics Education',
    'physics.flu-dyn': 'Fluid Dynamics',
    'physics.geo-ph': 'Geophysics',
    'physics.hist-ph': 'History and Philosophy of Physics',
    'physics.ins-det': 'Instrumentation and Detectors',
    'physics.med-ph': 'Medical Physics',
    'physics.optics': 'Optics',
    'physics.plasm-ph': 'Plasma Physics',
    'physics.soc-ph': 'Physics and Society',
    'physics.space-ph': 'Space Physics',
    'q-bio.BM': 'Biomolecules',
    'q-bio.CB': 'Cell Behavior',
    'q-bio.GN': 'Genomics',
    'q-bio.MN': 'Molecular Networks',
    'q-bio.NC': 'Neurons and Cognition',
    'q-bio.PE': 'Populations and Evolution',
    'q-bio.QM': 'Quantitative Methods',
    'q-bio.SC': 'Subcellular Processes',
    'q-bio.TO': 'Tissues and Organs',
    'q-fin.CP': 'Computational Finance',
    'q-fin.GN': 'General Finance',
    'q-fin.MF': 'Mathematical Finance',
    'q-fin.PM': 'Portfolio Management',
    'q-fin.PR': 'Pricing of Securities',
    'q-fin.RM': 'Risk Management',
    'q-fin.ST': 'Statistical Finance',
    'q-fin.TR': 'Trading and Market Microstructure',
    'quant-ph': 'Quantum Physics',
    'stat.AP': 'Applications',
    'stat.CO': 'Computation',
    'stat.ME': 'Methodology',
    'stat.ML': 'Machine Learning',
}


def translate_topic_name(topic_code):
    """Return a human-readable label for an arXiv topic identifier."""

    return TOPIC_TRANSLATIONS.get(topic_code, topic_code)


def _build_topics(paper_db, date_sorted_pids):
    topic_pids = {}
    for pid in date_sorted_pids:
        topic = paper_db[pid].get('arxiv_primary_category', {}).get('term')
        if not topic:
            continue
        topic_pids.setdefault(topic, []).append(pid)
    topics = sorted([
        {
            'name': topic,
            'count': len(pids),
            'display_name': translate_topic_name(topic),
        } for topic, pids in topic_pids.items()
    ], key=lambda x: x['display_name'])
    return topic_pids, topics


def _refresh_serving_data():
    global db, vocab, idf, sim_dict, user_sim
    global DATE_SORTED_PIDS, TOP_SORTED_PIDS, SEARCH_DICT
    global TOPIC_PIDS, TOPICS

    print('loading the paper database', Config.db_serve_path)
    new_db = pickle.load(open(Config.db_serve_path, 'rb'))
    ensure_impact_scores(new_db)

    print('loading tfidf_meta', Config.meta_path)
    meta = pickle.load(open(Config.meta_path, "rb"))
    new_vocab = meta['vocab']
    new_idf = meta['idf']

    print('loading paper similarities', Config.sim_path)
    new_sim_dict = pickle.load(open(Config.sim_path, "rb"))

    print('loading user recommendations', Config.user_sim_path)
    new_user_sim = {}
    if os.path.isfile(Config.user_sim_path):
        new_user_sim = pickle.load(open(Config.user_sim_path, 'rb'))

    print('loading serve cache...', Config.serve_cache_path)
    cache = pickle.load(open(Config.serve_cache_path, "rb"))
    new_date_sorted_pids = cache['date_sorted_pids']
    new_top_sorted_pids = sort_by_impact(new_db)
    new_search_dict = cache['search_dict']

    print('building topic index')
    new_topic_pids, new_topics = _build_topics(new_db, new_date_sorted_pids)

    with data_lock:
        db = new_db
        vocab = new_vocab
        idf = new_idf
        sim_dict = new_sim_dict
        user_sim = new_user_sim
        DATE_SORTED_PIDS = new_date_sorted_pids
        TOP_SORTED_PIDS = new_top_sorted_pids
        SEARCH_DICT = new_search_dict
        TOPIC_PIDS = new_topic_pids
        TOPICS = new_topics


# -----------------------------------------------------------------------------
# utilities for database interactions 
# -----------------------------------------------------------------------------
# to initialize the database: sqlite3 as.db < schema.sql
def connect_db():
    sqlite_db = sqlite3.connect(Config.database_path)
    sqlite_db.row_factory = sqlite3.Row  # to return dicts rather than tuples
    return sqlite_db


def query_db(query, args=(), one=False):
    """Queries the database and returns a list of dictionaries."""
    cur = g.db.execute(query, args)
    rv = cur.fetchall()
    return (rv[0] if rv else None) if one else rv


def get_user_id(username):
    """Convenience method to look up the id for a username."""
    rv = query_db('select user_id from user where username = ?',
                  [username], one=True)
    return rv[0] if rv else None


def get_username(user_id):
    """Convenience method to look up the username for a user."""
    rv = query_db('select username from user where user_id = ?',
                  [user_id], one=True)
    return rv[0] if rv else None


def ensure_single_user():
    """Ensure the single local user exists and return its row."""
    user = query_db('select * from user where user_id = ?', [SINGLE_USER_ID], one=True)
    if user:
        return user

    creation_time = int(time.time())
    g.db.execute('''insert into user (user_id, username, pw_hash, creation_time)
                    values (?, ?, ?, ?)''',
                 [SINGLE_USER_ID, SINGLE_USER_USERNAME, 'local-mode', creation_time])
    g.db.commit()
    return query_db('select * from user where user_id = ?', [SINGLE_USER_ID], one=True)


def current_user_id():
    """Return the identifier for the single local user."""
    return SINGLE_USER_ID


# -----------------------------------------------------------------------------
# connection handlers
# -----------------------------------------------------------------------------

@app.before_request
def before_request():
    # this will always request database connection, even if we dont end up using it ;\
    g.db = connect_db()
    # single-user mode: always ensure a default user is present
    g.user = ensure_single_user()


@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()


# -----------------------------------------------------------------------------
# search/sort functionality
# -----------------------------------------------------------------------------

def _normalize_topics(topic_names):
    if not topic_names:
        return []
    if isinstance(topic_names, str):
        topic_names = [topic_names]
    return [t for t in topic_names if t]


def ensure_time_metadata(paper):
    if 'time_updated' not in paper:
        timestruct = dateutil.parser.parse(paper['updated'])
        paper['time_updated'] = int(timestruct.strftime("%s"))

    if 'time_published' not in paper:
        timestruct = dateutil.parser.parse(paper['published'])
        paper['time_published'] = int(timestruct.strftime("%s"))


def compute_impact_score(paper, now_ts=None, alpha=0.3):
    ensure_time_metadata(paper)

    citations = paper.get('citation_count', 0) or 0
    years_since_pub = max(((now_ts or time.time()) - paper['time_published']) / SECONDS_PER_YEAR, 0)
    paper['impact_score'] = math.log(1 + citations) - alpha * years_since_pub
    paper['years_since_pub'] = years_since_pub


def ensure_impact_scores(paper_db):
    now_ts = time.time()
    for paper in paper_db.values():
        compute_impact_score(paper, now_ts=now_ts)


def sort_by_impact(paper_db):
    scored_papers = []
    for pid, paper in paper_db.items():
        if paper.get('impact_score') is None:
            continue
        scored_papers.append((paper['impact_score'], pid))

    scored_papers.sort(reverse=True, key=lambda x: x[0])
    return [pid for _, pid in scored_papers]


def _has_repo_metadata(paper):
    if paper.get('is_opensource') or paper.get('has_github') or paper.get('is_open_source'):
        return True

    repo_links = paper.get('repo_links')
    if isinstance(repo_links, (list, tuple, set)):
        return bool(repo_links)
    return bool(repo_links)


def _publication_statuses(paper):
    status_fields = [
        'publication_status',
        'published_status',
        'conference_status',
    ]

    statuses = set()
    for field in status_fields:
        val = paper.get(field)
        if isinstance(val, str):
            statuses.add(val.lower())
        elif isinstance(val, (list, tuple)):
            statuses.update([str(v).lower() for v in val])

    if paper.get('accepted') or paper.get('is_accepted'):
        statuses.add('accepted')
    if paper.get('presented') or paper.get('is_presented'):
        statuses.add('presented')

    metadata_fields = [
        'comment',
        'comments',
        'journal_ref',
        'journal-ref',
        'journalref',
        'journal',
        'annotation',
        'annotations',
        'notes',
    ]

    metadata_chunks = []
    for field in metadata_fields:
        val = paper.get(field)
        if isinstance(val, str):
            metadata_chunks.append(val)
        elif isinstance(val, (list, tuple)):
            metadata_chunks.extend([str(v) for v in val])

    if metadata_chunks:
        metadata_text = " ".join(metadata_chunks)
        metadata_lower = metadata_text.lower()

        venue_year_pattern = re.compile(r'\b[A-Za-z][A-Za-z0-9&.+/\-]{2,}\s?(?:20\d{2}|19\d{2}|\'\'?\d{2})\b')
        has_venue_year = bool(venue_year_pattern.search(metadata_text))

        acceptance_patterns = [
            r'\baccepted\b',
            r'\bto appear\b',
            r'\bin press\b',
            r'\bcamera[- ]ready\b',
            r'\bappears in\b',
            r'\bin proceedings\b',
            r'\bpublished in\b',
        ]

        presentation_patterns = [
            r'\boral\b',
            r'\bspotlight\b',
            r'\bposter\b',
            r'\bpresented at\b',
            r'\bpresentation at\b',
            r'\bwill be presented\b',
            r'\bpresenting at\b',
            r'\baccepted as an? (oral|poster|spotlight)\b',
        ]

        if has_venue_year:
            statuses.add('accepted')

        if any(re.search(pattern, metadata_lower) for pattern in acceptance_patterns):
            statuses.add('accepted')

        if any(re.search(pattern, metadata_lower) for pattern in presentation_patterns):
            statuses.add('presented')
            statuses.add('accepted')

    return list(statuses)


def _matches_publication_filters(paper, publication_filters):
    if not publication_filters:
        return True

    paper_statuses = set(_publication_statuses(paper))
    if not paper_statuses:
        return False

    return any(status in paper_statuses for status in publication_filters)


def _sort_papers(papers, sort_by='date', sort_order='desc'):
    reverse = sort_order != 'asc'

    if sort_by == 'score':
        key_fn = lambda p: p.get('impact_score', float('-inf'))
    elif sort_by == 'citation':
        key_fn = lambda p: p.get('citation_count', 0)
    else:
        key_fn = lambda p: p.get('time_published', 0)

    for paper in papers:
        ensure_time_metadata(paper)

    return sorted(papers, key=key_fn, reverse=reverse)


def filter_papers(papers, topic_names=None, min_score=None, open_source=False, publication_filters=None):
    filtered = papers
    normalized_topics = _normalize_topics(topic_names)

    if normalized_topics:
        filtered = [p for p in filtered if p.get('arxiv_primary_category', {}).get('term') in normalized_topics]

    if min_score is not None:
        filtered = [p for p in filtered if p.get('impact_score') is not None and p['impact_score'] >= min_score]

    if open_source:
        filtered = [p for p in filtered if _has_repo_metadata(p)]

    if publication_filters:
        publication_filters = [f.lower() for f in publication_filters]
        filtered = [p for p in filtered if _matches_publication_filters(p, publication_filters)]

    return filtered


def papers_search(qraw, topic_names=None, min_score=None, open_source=False, publication_filters=None, sort_by=None,
                  sort_order='desc'):
    qparts = qraw.lower().strip().split() if qraw else []
    scores = []

    if not qparts:
        base = [db[pid] for pid in DATE_SORTED_PIDS]
        filtered = filter_papers(base, topic_names=topic_names, min_score=min_score, open_source=open_source,
                                 publication_filters=publication_filters)
        if sort_by:
            return _sort_papers(filtered, sort_by=sort_by, sort_order=sort_order)
        return filtered

    # use reverse index and accumulate scores
    for pid, p in db.items():
        score = sum(SEARCH_DICT[pid].get(q, 0) for q in qparts)
        if score == 0:
            continue  # no match whatsoever, dont include
        # give a small boost to more recent papers
        score += 0.0001 * p['tscore']
        scores.append((score, p))
    scores.sort(reverse=True, key=lambda x: x[0])  # descending
    out = [x[1] for x in scores if x[0] > 0]
    filtered = filter_papers(out, topic_names=topic_names, min_score=min_score, open_source=open_source,
                             publication_filters=publication_filters)
    if sort_by:
        return _sort_papers(filtered, sort_by=sort_by, sort_order=sort_order)
    return filtered


def papers_similar(pid):
    rawpid = strip_version(pid)

    # check if we have this paper at all, otherwise return empty list
    if not rawpid in db:
        return []

    # check if we have distances to this specific version of paper id (includes version)
    if pid in sim_dict:
        # good, simplest case: lets return the papers
        return [db[strip_version(k)] for k in sim_dict[pid]]
    else:
        # ok we don't have this specific version. could be a stale URL that points to,
        # e.g. v1 of a paper, but due to an updated version of it we only have v2 on file
        # now. We want to use v2 in that case.
        # lets try to retrieve the most recent version of this paper we do have
        kok = [k for k in sim_dict if rawpid in k]
        if kok:
            # ok we have at least one different version of this paper, lets use it instead
            id_use_instead = kok[0]
            return [db[strip_version(k)] for k in sim_dict[id_use_instead]]
        else:
            # return just the paper. we dont have similarities for it for some reason
            return [db[rawpid]]


def papers_from_library():
    out = []
    if g.user:
        # user is logged in, lets fetch their saved library data
        uid = current_user_id()
        user_library = query_db('''select *
                                   from library
                                   where user_id = ?''', [uid])
        libids = [strip_version(x['paper_id']) for x in user_library]
        out = [db[x] for x in libids]
        out = sorted(out, key=lambda k: k['updated'], reverse=True)
    return out


def papers_from_svm(recent_days=None):
    out = []
    if g.user:

        uid = current_user_id()
        if not uid in user_sim:
            return []

        # we want to exclude papers that are already in user library from the result, so fetch them.
        user_library = query_db('''select *
                                   from library
                                   where user_id = ?''', [uid])
        libids = {strip_version(x['paper_id']) for x in user_library}

        plist = user_sim[uid]
        out = [db[x] for x in plist if not x in libids]

        if recent_days is not None:
            # filter as well to only most recent papers
            curtime = int(time.time())  # in seconds
            out = [x for x in out if curtime - x['time_published'] < recent_days * 24 * 60 * 60]

    return out


def papers_filter_version(papers, v):
    if v != '1':
        return papers  # noop
    intv = int(v)
    filtered = [p for p in papers if p['_version'] == intv]
    return filtered


def papers_from_topic(topic_name, vfilter='all'):
    """Return papers that match the provided arXiv primary category."""

    if not topic_name:
        return []

    pids = TOPIC_PIDS.get(topic_name, [])
    papers = [db[pid] for pid in pids]
    return papers_filter_version(papers, vfilter)


def encode_json(ps, n=10, send_images=True, send_abstracts=True):
    libids = set()
    if g.user:
        # user is logged in, lets fetch their saved library data
        uid = current_user_id()
        user_library = query_db('''select *
                                   from library
                                   where user_id = ?''', [uid])
        libids = {strip_version(x['paper_id']) for x in user_library}

    ret = []
    for i in range(min(len(ps), n)):
        p = ps[i]
        idvv = '%sv%d' % (p['_rawid'], p['_version'])
        struct = {}
        struct['title'] = p['title']
        struct['pid'] = idvv
        struct['rawpid'] = p['_rawid']
        struct['category'] = p['arxiv_primary_category']['term']
        struct['authors'] = [a['name'] for a in p['authors']]
        struct['link'] = p['link']
        struct['in_library'] = 1 if p['_rawid'] in libids else 0
        struct['citation_count'] = p.get('citation_count', 0)
        struct['impact_score'] = p.get('impact_score')
        if send_abstracts:
            struct['abstract'] = p['summary']
        if send_images:
            thumb_fname = idvv + '.pdf.jpg'
            thumb_path = os.path.join(Config.thumbs_dir, thumb_fname)
            if not os.path.isfile(thumb_path):
                struct['img'] = '/static/missing.svg'
            else:
                struct['img'] = '/static/thumbs/' + thumb_fname
        struct['tags'] = [t['term'] for t in p['tags']]

        # Repo metadata is computed during ingest/backfill to keep the API layer pure
        struct['repo_links'] = p.get('repo_links') or []
        struct['is_opensource'] = p.get('is_opensource', False)

        # render time information nicely
        timestruct = dateutil.parser.parse(p['updated'])
        struct['published_time'] = '%s/%s/%s' % (timestruct.month, timestruct.day, timestruct.year)
        timestruct = dateutil.parser.parse(p['published'])
        struct['originally_published_time'] = '%s/%s/%s' % (timestruct.month, timestruct.day, timestruct.year)

        # arxiv comments from the authors (when they submit the paper)
        cc = p.get('arxiv_comment', '')
        if len(cc) > 100:
            cc = cc[:100] + '...'  # crop very long comments
        struct['comment'] = cc

        ret.append(struct)
    return ret


# -----------------------------------------------------------------------------
# flask request handling
# -----------------------------------------------------------------------------

def default_context(papers, **kws):
    top_papers = encode_json(papers, args.num_results)

    # prompt logic
    show_prompt = 'no'
    try:
        if Config.beg_for_hosting_money and g.user and goaway_collection is not None and uniform(0, 1) < 0.05:
            uid = current_user_id()
            entry = goaway_collection.find_one({'uid': uid})
            if not entry:
                lib_count = query_db('''select count(*)
                                        from library
                                        where user_id = ?''', [uid], one=True)
                lib_count = lib_count['count(*)']
                if lib_count > 0:  # user has some items in their library too
                    show_prompt = 'yes'
    except Exception as e:
        print(e)

    ans = dict(
        papers=top_papers,
        numresults=len(papers),
        totpapers=len(db),
        tweets=[],
        msg='',
        show_prompt=show_prompt,
        topics=TOPICS,
        selected_topic='',
        selected_topic_display='',
        selected_topics=[],
        min_score=kws.get('min_score', ''),
        search_query=kws.get('search_query', ''),
        open_source=kws.get('open_source', False),
        publication_statuses=kws.get('publication_statuses', []),
        sort_by=kws.get('sort_by', ''),
        sort_order=kws.get('sort_order', 'desc'),
    )
    ans.update(kws)
    return ans


@app.route('/goaway', methods=['POST'])
def goaway():
    if goaway_collection is None:
        return 'OK'
    if not g.user: return  # weird
    uid = current_user_id()
    entry = goaway_collection.find_one({'uid': uid})
    if not entry:  # ok record this user wanting it to stop
        username = get_username(current_user_id())
        print('adding', uid, username, 'to goaway.')
        goaway_collection.insert_one({'uid': uid, 'time': int(time.time())})
    return 'OK'


def _init_ingest_job(paper_id: str) -> str:
    job_id = uuid.uuid4().hex
    now = int(time.time())
    with ingest_lock:
        ingest_jobs[job_id] = {
            'job_id': job_id,
            'paper_id': paper_id,
            'label': 'Preparing to start the ingest process...',
            'percent': 0,
            'messages': [],
            'done': False,
            'error': False,
            'status': 'running',
            'warning': False,
            'created_at': now,
            'updated_at': now,
        }
        _persist_ingest_job(ingest_jobs[job_id])
    return job_id


def _update_ingest_job(job_id: str, label: str, percent: int, message: Optional[str] = None, done: bool = False,
                       error: bool = False, warning: bool = False):
    with ingest_lock:
        job = ingest_jobs.get(job_id)
        if not job:
            job = _load_ingest_job(job_id)
            if not job:
                return
        job['label'] = label
        job['percent'] = percent
        if message:
            job['messages'].append(message)
        if done:
            job['done'] = True
        if error:
            job['error'] = True
            job['done'] = True
        if warning:
            job['warning'] = True
        if job.get('error'):
            job['status'] = 'error'
        elif job.get('warning'):
            job['status'] = 'warning'
        elif job.get('done'):
            job['status'] = 'done'
        else:
            job['status'] = 'running'
        job['updated_at'] = int(time.time())
        ingest_jobs[job_id] = job
        _persist_ingest_job(job)


def _get_ingest_job(job_id: str):
    with ingest_lock:
        job = ingest_jobs.get(job_id)
        if not job:
            job = _load_ingest_job(job_id)
        if not job:
            return None
        return dict(job)


def _ensure_ingest_jobs_dir():
    os.makedirs(ingest_jobs_dir, exist_ok=True)

def _persist_recompute_status(status: dict):
    _ensure_ingest_jobs_dir()
    with open_atomic(recompute_status_path, 'w') as handle:
        json.dump(status, handle)


def _load_recompute_status():
    if not os.path.isfile(recompute_status_path):
        return None
    with open(recompute_status_path, 'r') as handle:
        return json.load(handle)


def _get_recompute_status():
    with recompute_lock:
        status = recompute_status.get('state') or _load_recompute_status()
        dirty = False
        if not status:
            status = {
                'status': 'idle',
                'updated_at': int(time.time()),
                'percent': 0,
                'stdout_path': recompute_stdout_path,
                'stderr_path': recompute_stderr_path,
            }
            dirty = True
        if 'stdout_path' not in status:
            status['stdout_path'] = recompute_stdout_path
            dirty = True
        if 'stderr_path' not in status:
            status['stderr_path'] = recompute_stderr_path
            dirty = True
        now = int(time.time())
        updated_at = status.get('updated_at', now)
        thread_active = recompute_thread is not None and recompute_thread.is_alive()
        if status.get('status') in {'running', 'queued'}:
            stale = (now - updated_at) > RECOMPUTE_STALE_SECONDS
            if stale and not thread_active:
                status['status'] = 'idle'
                status['updated_at'] = now
                status.pop('message', None)
                status.pop('percent', None)
                status.pop('error', None)
                dirty = True
        if dirty:
            recompute_status['state'] = status
            _persist_recompute_status(status)
        return dict(status)


def _update_recompute_status(status: str, message: Optional[str] = None, error: Optional[str] = None, percent: Optional[Union[int, str]] = None):
    with recompute_lock:
        state = recompute_status.get('state') or _load_recompute_status() or {
            'status': 'idle',
            'updated_at': int(time.time()),
            'percent': 0,
            'stdout_path': recompute_stdout_path,
            'stderr_path': recompute_stderr_path,
        }
        state['status'] = status
        state['updated_at'] = int(time.time())
        if status in {'idle', 'skipped', 'disabled'}:
            state.pop('message', None)
            state.pop('percent', None)
        else:
            if message:
                state['message'] = message
            if percent is not None:
                state['percent'] = percent
        if error:
            state['error'] = error
        recompute_status['state'] = state
        _persist_recompute_status(state)


def _run_recompute_job():
    global recompute_thread
    try:
        _update_recompute_status('running', message='Recomputing caches', percent='—')
        _ensure_ingest_jobs_dir()
        with open(recompute_stdout_path, 'a') as stdout_handle, open(recompute_stderr_path, 'a') as stderr_handle:
            stdout_handle.write(f'[{time.ctime()}] Starting recompute\n')
            stderr_handle.write(f'[{time.ctime()}] Starting recompute\n')
            steps = [
                ('analyze.py', True),
                ('buildsvm.py', False),
                ('make_cache.py', True),
            ]
            for script_name, required in steps:
                _update_recompute_status(
                    'running',
                    message=f'Running {script_name}',
                    percent='—',
                )
                stdout_handle.write(f'[{time.ctime()}] Running {script_name}\n')
                stderr_handle.write(f'[{time.ctime()}] Running {script_name}\n')
                if script_name == 'make_cache.py':
                    percent_pattern = re.compile(r'(\d{1,3})%')
                    proc = subprocess.Popen(
                        [sys.executable, script_name],
                        stdout=subprocess.PIPE,
                        stderr=stderr_handle,
                        text=True,
                        bufsize=1,
                    )
                    assert proc.stdout is not None
                    for line in proc.stdout:
                        stdout_handle.write(line)
                        stdout_handle.flush()
                        match = percent_pattern.search(line)
                        if match:
                            percent_value = int(match.group(1))
                            if 0 <= percent_value <= 100:
                                _update_recompute_status(
                                    'running',
                                    message=f'Running {script_name}',
                                    percent=percent_value,
                                )
                    returncode = proc.wait()
                else:
                    proc = subprocess.run(
                        [sys.executable, script_name],
                        check=required,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                        text=True,
                    )
                    returncode = proc.returncode
                if returncode != 0:
                    if required:
                        raise subprocess.CalledProcessError(
                            returncode,
                            [sys.executable, script_name],
                        )
                    stderr_handle.write(
                        f'[{time.ctime()}] {script_name} exited with {returncode}\n'
                    )
                _update_recompute_status(
                    'running',
                    message=f'Finished {script_name}',
                    percent='—',
                )
            stdout_handle.write(f'[{time.ctime()}] Recompute finished\n')
            stderr_handle.write(f'[{time.ctime()}] Recompute finished\n')
        _refresh_serving_data()
    except Exception as exc:
        _update_recompute_status('failed', error=str(exc), message='Recompute failed')
    else:
        _update_recompute_status('finished', message='Recompute finished successfully', percent=100)

    finally:
        with recompute_lock:
            recompute_thread = None


def _ingest_job_path(job_id: str) -> str:
    return os.path.join(ingest_jobs_dir, f'{job_id}.json')


def _persist_ingest_job(job: dict):
    _ensure_ingest_jobs_dir()
    with open_atomic(_ingest_job_path(job['job_id']), 'w') as handle:
        json.dump(job, handle)


def _load_ingest_job(job_id: str):
    path = _ingest_job_path(job_id)
    if not os.path.isfile(path):
        return None
    with open(path, 'r') as handle:
        return json.load(handle)


def _run_ingest_job(job_id: str, paper_id: str):
    global recompute_thread
    def emit(label: str, percent: int, message: Optional[str] = None, warning: bool = False):
        _update_ingest_job(job_id, label, percent, message, warning=warning)
    try:
        emit('Starting ingest...', 1)
        ingest_paper(paper_id, progress_callback=emit, recompute_caches=False)
        recompute_state = _get_recompute_status()
        if recompute_state.get('status') == 'running':
            emit('Recompute already running...', 90, 'Existing cache recompute is still running.')
        else:
            emit('Queued recompute...', 90, 'Cache recompute will run in the background.')
            _update_recompute_status('queued', message='Recompute queued', percent='—')
            with recompute_lock:
                recompute_thread = threading.Thread(target=_run_recompute_job)
                recompute_thread.daemon = True
                recompute_thread.start()
    except Exception as e:
        _update_ingest_job(job_id, 'Ingest failed', 100, str(e), done=True, error=True)
    else:
        _update_ingest_job(job_id, 'Ingest complete', 100, done=True)


@app.route('/ingest/status/<job_id>')
def ingest_status(job_id):
    job = _get_ingest_job(job_id)
    if job is None:
        return jsonify({'error': 'Unknown job id'}), 404
    return jsonify(job)

@app.route('/ingest/recompute_status')
def ingest_recompute_status():
    return jsonify(_get_recompute_status())


@app.route('/recompute/status')
def recompute_status_endpoint():
    status = _get_recompute_status()
    return jsonify({
        'status': status.get('status'),
        'message': status.get('message'),
        'percent': status.get('percent'),
        'updated_at': status.get('updated_at'),
        'error': status.get('error'),
    })


@app.route('/ingest', methods=['POST'])
def ingest_arxiv():
    paper_id = request.form.get('paper_id', '').strip()
    wants_json = request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.accept_mimetypes[
        'application/json']
    if paper_id == '':
        if wants_json:
            return jsonify({'error': 'Please enter an arXiv identifier to add.'}), 400
        flash('Please enter an arXiv identifier to add.')
        return redirect(request.referrer or url_for('intmain'))

    if not isvalidid(paper_id):
        if wants_json:
            return jsonify({'error': 'Please provide a valid arXiv identifier, e.g., 1512.08756v2.'}), 400
        flash('Please provide a valid arXiv identifier, e.g., 1512.08756v2.')
        return redirect(request.referrer or url_for('intmain'))

    job_id = _init_ingest_job(paper_id)
    ingest_thread = threading.Thread(target=_run_ingest_job, args=(job_id, paper_id))
    ingest_thread.daemon = True
    ingest_thread.start()

    if wants_json:
        return jsonify({'job_id': job_id})

    flash(f'Started ingest for {paper_id}.')
    return redirect(url_for('intmain'))


@app.route("/")
def intmain():
    vstr = request.args.get('vfilter', 'all')
    papers = [db[pid] for pid in DATE_SORTED_PIDS]  # precomputed
    papers = papers_filter_version(papers, vstr)
    ctx = default_context(papers, render_format='recent',
                          msg='Showing most recent Arxiv papers:')
    return render_template('main.html', **ctx)


@app.route("/<request_pid>")
def rank(request_pid=None):
    if not isvalidid(request_pid):
        return ''  # these are requests for icons, things like robots.txt, etc
    papers = papers_similar(request_pid)
    ctx = default_context(papers, render_format='paper')
    return render_template('main.html', **ctx)


@app.route("/search", methods=['GET'])
def search():
    q = request.args.get('q', '')  # get the search request
    selected_topics = request.args.getlist('topics')
    selected_topics += request.args.getlist('topics[]')
    # support legacy single topic param if present
    legacy_topic = request.args.get('topic', '')
    if legacy_topic and legacy_topic not in selected_topics:
        selected_topics.append(legacy_topic)
    min_score_str = request.args.get('min_score', request.args.get('scorebar', ''))

    open_source_filter = request.args.get('open_source', '') == '1'
    publication_statuses = request.args.getlist('publication_status')
    sort_by = request.args.get('sortby', '')
    sort_order = request.args.get('sortorder', 'desc')

    try:
        min_score = float(min_score_str) if min_score_str != '' else None
    except ValueError:
        min_score = None

    papers = papers_search(
        q,
        topic_names=selected_topics,
        min_score=min_score,
        open_source=open_source_filter,
        publication_filters=publication_statuses,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    no_results_message = ''
    if len(papers) == 0:
        topics_display = ', '.join(selected_topics) if selected_topics else '[]'
        no_results_message = 'No paper found for criteria in databases %s' % (topics_display,)
    ctx = default_context(
        papers,
        render_format="search",
        selected_topic=legacy_topic,
        selected_topic_display=translate_topic_name(legacy_topic) if legacy_topic else '',
        selected_topics=selected_topics,
        min_score=min_score_str,
        search_query=q,
        open_source=open_source_filter,
        publication_statuses=publication_statuses,
        sort_by=sort_by,
        sort_order=sort_order,
        no_results_message=no_results_message,
    )
    return render_template('main.html', **ctx)


@app.route('/topics', methods=['GET'])
def topics():
    """Browse papers grouped by arXiv category."""

    topic_name = request.args.get('topic', '')
    vstr = request.args.get('vfilter', 'all')
    papers = papers_from_topic(topic_name, vfilter=vstr)
    selected_topic_display = translate_topic_name(topic_name) if topic_name else ''

    if topic_name:
        msg = 'Most recent papers in topic %s (%s):' % (selected_topic_display, topic_name) if len(
            papers) > 0 else 'No papers found for topic %s (%s).' % (selected_topic_display, topic_name)
    else:
        msg = 'Select an arXiv topic to browse recent papers.'

    ctx = default_context(papers, render_format='topics', msg=msg, topics=TOPICS, selected_topic=topic_name,
                          selected_topic_display=selected_topic_display)
    return render_template('main.html', **ctx)


@app.route('/recommend', methods=['GET'])
def recommend():
    """ return user's svm sorted list """
    ttstr = request.args.get('timefilter', 'week')  # default is week
    vstr = request.args.get('vfilter', 'all')  # default is all (no filter)
    legend = {'day': 1, '3days': 3, 'week': 7, 'month': 30, 'year': 365}
    tt = legend.get(ttstr, None)
    papers = papers_from_svm(recent_days=tt)
    papers = papers_filter_version(papers, vstr)
    ctx = default_context(papers, render_format='recommend',
                          msg='Recommended papers: (based on SVM trained on tfidf of papers in your library, refreshed every day or so)' if g.user else 'You must be logged in and have some papers saved in your library.')
    return render_template('main.html', **ctx)


@app.route('/top', methods=['GET'])
def top():
    """ return top papers """
    ttstr = request.args.get('timefilter', 'week')  # default is week
    vstr = request.args.get('vfilter', 'all')  # default is all (no filter)
    legend = {'day': 1, '3days': 3, 'week': 7, 'month': 30, 'year': 365, 'alltime': 10000}
    tt = legend.get(ttstr, 7)
    curtime = int(time.time())  # in seconds
    top_sorted_papers = [db[p] for p in TOP_SORTED_PIDS]
    papers = [p for p in top_sorted_papers if curtime - p['time_published'] < tt * 24 * 60 * 60]
    papers = papers_filter_version(papers, vstr)
    ctx = default_context(papers, render_format='top',
                          msg='Top papers by OpenAlex recency-adjusted score:')
    return render_template('main.html', **ctx)


@app.route('/toptwtr', methods=['GET'])
def toptwtr():
    """ return top papers """
    ttstr = request.args.get('timefilter', 'day')  # default is day
    tweets_top = {'day': tweets_top1, 'week': tweets_top7, 'month': tweets_top30}[ttstr]
    cursor = tweets_top.find().sort([('vote', pymongo.DESCENDING)]).limit(100)
    papers, tweets = [], []
    for rec in cursor:
        if rec['pid'] in db:
            papers.append(db[rec['pid']])
            tweet = {k: v for k, v in rec.items() if k != '_id'}
            tweets.append(tweet)
    ctx = default_context(papers, render_format='toptwtr', tweets=tweets,
                          msg='Top papers mentioned on Twitter over last ' + ttstr + ':')
    return render_template('main.html', **ctx)


@app.route('/library')
def library():
    """ render user's library """
    papers = papers_from_library()
    ret = encode_json(papers, 500)  # cap at 500 papers in someone's library. that's a lot!
    msg = '%d papers in your library:' % (len(ret),)
    ctx = default_context(papers, render_format='library', msg=msg)
    return render_template('main.html', **ctx)


@app.route('/libtoggle', methods=['POST'])
def review():
    """ user wants to toggle a paper in his library """

    # make sure user is logged in
    if not g.user:
        return 'NO'  # fail... (not logged in). JS should prevent from us getting here.

    idvv = request.form['pid']  # includes version
    if not isvalidid(idvv):
        return 'NO'  # fail, malformed id. weird.
    pid = strip_version(idvv)
    if not pid in db:
        return 'NO'  # we don't know this paper. wat

    uid = current_user_id()

    # check this user already has this paper in library
    record = query_db('''select *
                         from library
                         where user_id = ?
                           and paper_id = ?''', [uid, pid], one=True)
    print(record)

    ret = 'NO'
    if record:
        # record exists, erase it.
        g.db.execute('''delete
                        from library
                        where user_id = ?
                          and paper_id = ?''', [uid, pid])
        g.db.commit()
        # print('removed %s for %s' % (pid, uid))
        ret = 'OFF'
    else:
        # record does not exist, add it.
        rawpid = strip_version(pid)
        g.db.execute('''insert into library (paper_id, user_id, update_time)
                        values (?, ?, ?)''',
                     [rawpid, uid, int(time.time())])
        g.db.commit()
        # print('added %s for %s' % (pid, uid))
        ret = 'ON'

    return ret



# -----------------------------------------------------------------------------
# int main
# -----------------------------------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--prod', dest='prod', action='store_true', help='run in prod?')
    parser.add_argument('-r', '--num_results', dest='num_results', type=int, default=200,
                        help='number of results to return per query')
    parser.add_argument('--port', dest='port', type=int, default=5000, help='port to serve on')
    args = parser.parse_args()
    print(args)

    if not os.path.isfile(Config.database_path):
        print('did not find as.db, trying to create an empty database from schema.sql...')
        print('this needs sqlite3 to be installed!')
        os.system('sqlite3 as.db < schema.sql')

    _refresh_serving_data()

    print('mongodb/tweets integration disabled (no MongoDB, or not needed)')
    client = None
    mdb = None
    tweets_top1 = None
    tweets_top7 = None
    tweets_top30 = None
    goaway_collection = None


    # start
    if args.prod:
        # run on Tornado instead, since running raw Flask in prod is not recommended
        print('starting tornado!')
        from tornado.wsgi import WSGIContainer
        from tornado.httpserver import HTTPServer
        from tornado.ioloop import IOLoop
        from tornado.log import enable_pretty_logging

        enable_pretty_logging()
        http_server = HTTPServer(WSGIContainer(app))
        http_server.listen(args.port)
        IOLoop.instance().start()
    else:
        print('starting flask!')
        app.debug = False
        app.run(port=args.port, host='0.0.0.0')
