from sqlitedict import SqliteDict
import sys
import socket
import urllib
import argparse
import zlib, pickle, sqlite3
import random
from datetime import datetime
import time
from urllib.parse import urlparse
import hashlib
import subprocess
import requests
from multiprocessing import Pool

def gz_encode(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))
def gz_decode(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dnscache', default="dnscache.sqld", help='IP address cache default: %(default)s')
    parser.add_argument('--download', default="pages.sqld", help='Here is where the downloaded pages go: %(default)s')
    parser.add_argument('--r404', default="404.sqld", help='Here is where we remember pages that gave 404 etc: %(default)s')
    args = parser.parse_args()

    #2) Results setup
    result_store = SqliteDict(args.download, encode=gz_encode, decode=gz_decode, autocommit=True)

    for url,cont in result_store.items():
        print(url,cont[:30])
    
    #3) 404 setup
    r404 = SqliteDict(args.r404, autocommit=True)
    for url,status in r404.items():
        print(url,status)
        
