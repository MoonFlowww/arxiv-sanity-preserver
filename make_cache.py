"""
computes various cache things on top of db.py so that the server
(running from serve.py) can start up and serve faster when restarted.

this script should be run whenever db.p is updated, and 
creates db2.p, which can be read by the server.
"""

import os
import json
import time
import math
import pickle
import dateutil.parser
import urllib.error
import urllib.request

from sqlite3 import dbapi2 as sqlite3
from utils import safe_pickle_dump, Config

sqldb = sqlite3.connect(Config.database_path)
sqldb.row_factory = sqlite3.Row # to return dicts rather than tuples

CACHE = {}

print('loading the paper database', Config.db_path)
db = pickle.load(open(Config.db_path, 'rb'))

print('loading tfidf_meta', Config.meta_path)
meta = pickle.load(open(Config.meta_path, "rb"))
vocab = meta['vocab']
idf = meta['idf']

OPENALEX_WORK_BASE = 'https://api.openalex.org/works/https://arxiv.org/abs/'


def fetch_citation_count(arxiv_id):
  """Fetch citation counts from OpenAlex for a given arXiv identifier."""

  url = OPENALEX_WORK_BASE + arxiv_id
  req = urllib.request.Request(url, headers={'User-Agent': 'arxiv-sanity-preserver/1.0'})
  try:
    with urllib.request.urlopen(req, timeout=10) as resp:
      if resp.status != 200:
        print('OpenAlex returned status %d for %s' % (resp.status, arxiv_id))
        return None
      payload = json.loads(resp.read().decode('utf-8'))
      return payload.get('cited_by_count')
  except urllib.error.HTTPError as e:
    print('HTTPError fetching citation count for %s: %s' % (arxiv_id, e))
  except urllib.error.URLError as e:
    print('URLError fetching citation count for %s: %s' % (arxiv_id, e))
  except Exception as e:
    print('Unexpected error fetching citation count for %s: %s' % (arxiv_id, e))
  return None

print('decorating the database with additional information...')
seconds_per_year = 365.25 * 24 * 60 * 60


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
  years_since_pub = max(((now_ts or time.time()) - paper['time_published']) / seconds_per_year, 0)
  paper['impact_score'] = math.log(1 + citations) - alpha * years_since_pub
  paper['years_since_pub'] = years_since_pub


for pid,p in db.items():
  ensure_time_metadata(p)

print('fetching citation counts from OpenAlex (if missing)...')
updated_citations = 0
for pid, p in db.items():
  if p.get('citation_count') is not None:
    continue

  count = fetch_citation_count(pid)
  if count is None:
    count = 0
  else:
    updated_citations += 1

  p['citation_count'] = count
  time.sleep(0.1)  # be kind to OpenAlex API
print('Updated citation counts for %d papers.' % updated_citations)

print('computing OpenAlex-inspired recency-aware scores...')
now_ts = time.time()
for pid, p in db.items():
  compute_impact_score(p, now_ts=now_ts)

print('computing min/max time for all papers...')
tts = [time.mktime(dateutil.parser.parse(p['updated']).timetuple()) for pid,p in db.items()]
ttmin = min(tts)*1.0
ttmax = max(tts)*1.0
for pid,p in db.items():
  tt = time.mktime(dateutil.parser.parse(p['updated']).timetuple())
  p['tscore'] = (tt-ttmin)/(ttmax-ttmin)

print('precomputing papers date sorted...')
scores = [(p['time_updated'], pid) for pid,p in db.items()]
scores.sort(reverse=True, key=lambda x: x[0])
CACHE['date_sorted_pids'] = [sp[1] for sp in scores]

# compute top papers in peoples' libraries
print('computing top papers...')
libs = sqldb.execute('''select * from library''').fetchall()
counts = {}
for lib in libs:
  pid = lib['paper_id']
  counts[pid] = counts.get(pid, 0) + 1
top_paper_counts = sorted([(v,k) for k,v in counts.items() if v > 0], reverse=True)
CACHE['library_sorted_pids'] = [q[1] for q in top_paper_counts]

print('computing citation-based popularity...')
citation_scores = []
for pid, paper in db.items():
  citation_scores.append((paper.get('impact_score', 0), pid))
citation_scores.sort(reverse=True, key=lambda x: x[0])
CACHE['top_sorted_pids'] = [pid for _, pid in citation_scores]

# some utilities for creating a search index for faster search
punc = "'!\"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'" # removed hyphen from string.punctuation
trans_table = {ord(c): None for c in punc}
def makedict(s, forceidf=None, scale=1.0):
  words = set(s.lower().translate(trans_table).strip().split())
  idfd = {}
  for w in words: # todo: if we're using bigrams in vocab then this won't search over them
    if forceidf is None:
      if w in vocab:
        # we have idf for this
        idfval = idf[vocab[w]]*scale
      else:
        idfval = 1.0*scale # assume idf 1.0 (low)
    else:
      idfval = forceidf
    idfd[w] = idfval
  return idfd

def merge_dicts(dlist):
  m = {}
  for d in dlist:
    for k,v in d.items():
      m[k] = m.get(k,0) + v
  return m

print('building an index for faster search...')
search_dict = {}
for pid,p in db.items():
  dict_title = makedict(p['title'], forceidf=5, scale=3)
  dict_authors = makedict(' '.join(x['name'] for x in p['authors']), forceidf=5)
  dict_categories = {x['term'].lower():5 for x in p['tags']}
  if 'and' in dict_authors: 
    # special case for "and" handling in authors list
    del dict_authors['and']
  dict_summary = makedict(p['summary'])
  search_dict[pid] = merge_dicts([dict_title, dict_authors, dict_categories, dict_summary])
CACHE['search_dict'] = search_dict

# save the cache
print('writing', Config.serve_cache_path)
safe_pickle_dump(CACHE, Config.serve_cache_path)
print('writing', Config.db_serve_path)
safe_pickle_dump(db, Config.db_serve_path)
