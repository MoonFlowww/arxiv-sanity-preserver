"""
Queries arxiv API and downloads papers (the query is a parameter).
The script is intended to enrich an existing database pickle (by default db.p),
so this file will be loaded first, and then new results will be added to it.
"""

import os
import time
import pickle
import random
import argparse
import urllib.request
import feedparser
from typing import Dict, Optional

from repo_metadata import build_repo_metadata
from utils import Config, safe_pickle_dump


# Default per-category fetch caps used when --category-counts is not supplied.
DEFAULT_CATEGORY_COUNTS = (
  "cs.AI=2000,cs.LG=2000,stat.ML=2000,cs.IT=1500,"  # core ML/AI
  "eess.SP=1500,cs.NE=1000,cs.CL=1000,cs.CV=1500,"  # signal/vision/linguistics
  "cond-mat.stat-mech=500,q-fin.TR=2000,q-fin.RM=2000,q-fin.ST=2000"  # finance + misc
)

def encode_feedparser_dict(d):
  """ 
  helper function to get rid of feedparser bs with a deep copy. 
  I hate when libs wrap simple things in their own classes.
  """
  if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
    j = {}
    for k in d.keys():
      j[k] = encode_feedparser_dict(d[k])
    return j
  elif isinstance(d, list):
    l = []
    for k in d:
      l.append(encode_feedparser_dict(k))
    return l
  else:
    return d

def parse_arxiv_url(url):
  """
  examples is http://arxiv.org/abs/1512.08756v2
  we want to extract the raw id and the version
  """
  ix = url.rfind('/')
  idversion = url[ix+1:] # extract just the id (and the version)
  parts = idversion.split('v')
  assert len(parts) == 2, 'error parsing url ' + url
  return parts[0], int(parts[1])


def parse_category_counts(category_arg: str) -> Dict[str, int]:
  """
  Parse a comma-separated list of category=count pairs into a dictionary.

  Example: "cs.AI=50,cs.CL=20" becomes {"cs.AI": 50, "cs.CL": 20}
  """
  mapping: Dict[str, int] = {}
  if not category_arg:
    return mapping

  parts = category_arg.split(',')
  for part in parts:
    if not part:
      continue
    if '=' not in part:
      raise ValueError('Invalid category specification "%s" (expected format category=count)' % part)
    category, count_str = part.split('=', 1)
    category = category.strip()
    try:
      mapping[category] = int(count_str)
    except ValueError as e:
      raise ValueError('Invalid count for category "%s": %s' % (category, e))

  return mapping


def fetch_for_query(search_query: str, max_results: Optional[int], args, db) -> int:
  """
  Fetch papers for a given query, returning the number of added papers.

  max_results limits how many results are requested from arXiv. When None,
  the script will fall back to args.max_index.
  """
  base_url = 'http://export.arxiv.org/api/query?' # base api query url
  max_fetch = max_results if max_results is not None else args.max_index
  print('Searching arXiv for %s' % (search_query, ))
  print('database has %d entries at start' % (len(db), ))

  num_added_total = 0
  i = args.start_index
  while i < max_fetch:
    results_this_iter = min(args.results_per_iteration, max_fetch - i)

    print("Results %i - %i" % (i, i + results_this_iter))
    query = 'search_query=%s&sortBy=lastUpdatedDate&start=%i&max_results=%i' % (
        search_query, i, results_this_iter)
    with urllib.request.urlopen(base_url + query) as url:
      response = url.read()
    parse = feedparser.parse(response)
    num_added = 0
    num_skipped = 0
    for e in parse.entries:

      j = encode_feedparser_dict(e)

      # extract just the raw arxiv id and version for this paper
      rawid, version = parse_arxiv_url(j['id'])
      j['_rawid'] = rawid
      j['_version'] = version

      # add to our database if we didn't have it before, or if this is a new version
      if not rawid in db or j['_version'] > db[rawid]['_version']:
        j.update(build_repo_metadata(j))
        db[rawid] = j
        print('Updated %s added %s' % (j['updated'].encode('utf-8'), j['title'].encode('utf-8')))
        num_added += 1
        num_added_total += 1
      else:
        num_skipped += 1

    # print some information
    print('Added %d papers, already had %d.' % (num_added, num_skipped))

    if len(parse.entries) == 0:
      print('Received no results from arxiv. Rate limiting? Exiting. Restart later maybe.')
      print(response)
      break

    if num_added == 0 and args.break_on_no_added == 1:
      print('No new papers were added. Assuming no new papers exist. Exiting.')
      break

    i += results_this_iter
    if i < max_fetch:
      print('Sleeping for %i seconds' % (args.wait_time , ))
      time.sleep(args.wait_time + random.uniform(0, 3))

  return num_added_total

if __name__ == "__main__":

  # parse input arguments
  parser = argparse.ArgumentParser()
  parser.add_argument('--search-query', type=str,
                      default=(
                            'cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML'
                            '+OR+cat:cs.IT'
                            '+OR+cat:eess.SP'
                            '+OR+cat:q-fin.TR'
                            '+OR+cat:q-fin.RM'
                            '+OR+cat:q-fin.ST'
                            '+OR+cat:cond-mat.stat-mech'),
                      help='query used for arxiv API. See http://arxiv.org/help/api/user-manual#detailed_examples')
  parser.add_argument('--start-index', type=int, default=0, help='0 = most recent API result')
  parser.add_argument('--max-index', type=int, default=10000, help='upper bound on paper index we will fetch')
  parser.add_argument('--results-per-iteration', type=int, default=100, help='passed to arxiv API')
  parser.add_argument('--wait-time', type=float, default=5.0, help='lets be gentle to arxiv API (in number of seconds)')
  parser.add_argument('--break-on-no-added', type=int, default=1, help='break out early if all returned query papers are already in db? 1=yes, 0=no')
  parser.add_argument('--category-counts', type=str, default=DEFAULT_CATEGORY_COUNTS,
                      help='Comma-separated list like "cs.AI=50,cs.CL=20" to fetch latest papers per category. Overrides --search-query. Default caps: %s' % DEFAULT_CATEGORY_COUNTS)
  args = parser.parse_args()

  # lets load the existing database to memory
  try:
    db = pickle.load(open(Config.db_path, 'rb'))
  except Exception as e:
    print('error loading existing database:')
    print(e)
    print('starting from an empty database')
    db = {}

  # -----------------------------------------------------------------------------
  # main loop where we fetch the new results
  num_added_total = 0

  category_counts = parse_category_counts(args.category_counts)
  if category_counts:
    for category, max_results in category_counts.items():
      query = 'cat:%s' % category
      print('--- Fetching up to %d papers for category %s ---' % (max_results, category))
      num_added_total += fetch_for_query(query, max_results, args, db)
  else:
    num_added_total += fetch_for_query(args.search_query, None, args, db)

  # save the database before we quit, if we found anything new
  if num_added_total > 0:
    print('Saving database with %d papers to %s' % (len(db), Config.db_path))
    safe_pickle_dump(db, Config.db_path)
