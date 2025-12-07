import os
import time
import pickle
import shutil
import random
from urllib.request import urlopen, Request
import urllib.error
from utils import Config

timeout_secs = 10  # after this many seconds we give up on a paper

if not os.path.exists(Config.pdf_dir):
    os.makedirs(Config.pdf_dir)

have = set(os.listdir(Config.pdf_dir))  # get list of all pdfs we already have

numok = 0
numtot = 0
db = pickle.load(open(Config.db_path, 'rb'))

for pid, j in db.items():

    # find the PDF link in the entry
    pdfs = [x['href'] for x in j['links'] if x['type'] == 'application/pdf']
    assert len(pdfs) == 1

    # build URL and normalize to export.arxiv.org (bot-friendly host)
    pdf_url = (pdfs[0] + '.pdf').replace('://arxiv.org', '://export.arxiv.org')

    basename = pdf_url.split('/')[-1]
    fname = os.path.join(Config.pdf_dir, basename)

    numtot += 1
    try:
        if basename not in have:
            print('fetching %s into %s' % (pdf_url, fname))

            # identify ourselves and be polite
            req = Request(
                pdf_url,
                headers={
                    "User-Agent": "arxiv-sanity-preserver (contact: you@example.com)"
                }
            )
            resp = urlopen(req, None, timeout_secs)
            data = resp.read()
            content_type = resp.headers.get("Content-Type", "").lower()

            # if we got HTML (e.g. recaptcha / error page), do NOT save as PDF
            if "text/html" in content_type or data.lstrip().lower().startswith(b"<html"):
                print("Got HTML instead of PDF for", pdf_url, "- skipping")
            else:
                with open(fname, 'wb') as fp:
                    fp.write(data)
                # small delay so we don't hammer arXiv
                time.sleep(0.5 + random.uniform(0, 0.5))
        else:
            print('%s exists, skipping' % (fname,))

        numok += 1

    except urllib.error.HTTPError as e:
        print('HTTP error downloading:', pdf_url)
        print(e)
    except Exception as e:
        print('error downloading:', pdf_url)
        print(e)

    print('%d/%d of %d downloaded ok.' % (numok, numtot, len(db)))

print('final number of papers downloaded okay: %d/%d' % (numok, len(db)))
