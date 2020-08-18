import random
from datetime import datetime
import time
from urllib.parse import urlparse
import hashlib
import subprocess
import requests
from multiprocessing import Pool
import trafilatura
from tqdm import tqdm

def gz_encode(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

def gz_decode(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))

def trafi(url, html):
    clean = trafilatura.extract(html)
    return url,clean

def trafi_callback(trafi_result):
    global clean_store
    url, txt, e = trafi_result
    assert url not in clean_store
    if e is not None:
        print(e)
        return
    clean_store[url] = txtif __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dnscache', default="dnscache.sqld", help='IP address cache default: %(default)s')
    parser.add_argument('--download', default="pages.sqld", help='Here is where the downloaded pages go: %(default)s')
    parser.add_argument('--r404', default="404.sqld", help='Here is where we remember pages that gave 404 etc: %(default)s')
    parser.add_argument('--clean', default="clean.sqld", help='Here is where the cleaned texts go: %(default)s')
    args = parser.parse_args()
    #2) Results setup
    result_store = SqliteDict(args.download, encode=gz_encode, decode=gz_decode,flag="r")
    clean_store = SqliteDict(args.clean, encode=gz_encode, decode=gz_decode, autocommit=True)
    for idx,url in tqdm(enumerate(result_store),total=len(result_store)):
        if url in clean_store:
            assert False
        else:
            url_r,clean=trafi(url,result_store[url])
            assert url_r==url
            clean_store[url]=clean
        assert len(clean_store)==idx+1
    clean_store.close()
