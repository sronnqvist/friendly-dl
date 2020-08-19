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
import gzip

from multiprocessing import Pool
from tqdm import tqdm
try:
    import localconfig
    headers=localconfig.headers
except:
    print("""You need to make a file localconfig.py with the headers for HTTP requests like headers={"user-agent":"XXXXXXXXXXX","contact":"XXXXXXXXXXXXXXXXXX","org":"XXXXXXXXXXXXXXXX","note":"research_project_mean_no_harm"}""",file=sys.stderr)
    sys.exit(-1)

def gz_encode(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

def gz_decode(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))


def yield_urls(initial_urls,dns_cache,req_delay_sec=20):
    #this yields URLs in an order and with delays that respect between-reqeusts wait times
    timer={} #host or ip -> datetime of last request

    urls_to_yield=set(initial_urls)
    print("Starting with",len(urls_to_yield),"urls",file=sys.stderr)
    while urls_to_yield:
        urls=list(urls_to_yield)
        random.shuffle(urls)
    
        for url in urls:
            p=urlparse(url)
            netloc=p.netloc
            if netloc in dns_cache:
                ip=dns_cache[netloc]
            else:
                try:
                    ip=socket.gethostbyname(netloc)
                except socket.gaierror:
                    #this is an unresolvable url, domain gone
                    urls_to_yield.remove(url)
                    yield url,None
                    continue
                except:
                    print("DNS lookup fail",url,file=sys.stderr)
                    urls_to_yield.remove(url)
                    yield url, None
                    continue
                dns_cache[netloc]=ip
            most_recent_req_ip=timer.get(ip,datetime.min)
            most_recent_req_netloc=timer.get(netloc,datetime.min)
            most_recent_req_any=max(most_recent_req_ip,most_recent_req_netloc)
            seconds_since=(datetime.now()-most_recent_req_any).total_seconds()
            if seconds_since<req_delay_sec:
                #let this guy be
                continue
            #this url should be OK to grab!
            timer[ip]=datetime.now()
            timer[netloc]=datetime.now()
            urls_to_yield.remove(url)
            yield url,ip
        print("Down to",len(urls_to_yield),"urls",file=sys.stderr)
        time.sleep(1) #avoid craze at the end when we are down to few IPs!

def dl_url(url):
    headers=localconfig.headers
    try:
        r=requests.get(url,headers=headers,timeout=(6.1,15))
    except:
        return url, None, None
    return (url, r.text, r.status_code)

def dl_callback(dl_result):
    url,content,status=dl_result
    if status==200:
        result_store[url]=content
    else:
        r404[url]=status

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dnscache', default="dnscache.sqld", help='IP address cache default: %(default)s')
    parser.add_argument('--download', default="pages.sqld", help='Here is where the downloaded pages go: %(default)s')
    parser.add_argument('--r404', default="404.sqld", help='Here is where we remember pages that gave 404 etc: %(default)s')
    parser.add_argument('URLs',nargs="+",help="File(s) of URLs to download")
    args = parser.parse_args()
    
    #1) DNS cache setup
    dns_cache=SqliteDict(args.dnscache,autocommit=True)
    
    #2) Results setup
    result_store = SqliteDict(args.download, encode=gz_encode, decode=gz_decode, autocommit=True)

    #3) 404 setup
    r404 = SqliteDict(args.r404, autocommit=True)



    
    urls=set()
    for f_name in args.URLs:
        with open(f_name, 'rt') as f:
            for line in tqdm(f):
                if not line.startswith("###C:warc-target-uri"):
                    continue
                url=line.strip().split("\t")[-1].strip()
                if url not in result_store and url not in r404:
                    urls.add(url)

                
    with Pool(200) as pool:
        for url,ip in tqdm(yield_urls(urls,dns_cache)):
            if ip is None:
                #there was a DNS error
                r404[url]="DNSERR"
                continue
            pool.apply_async(dl_url,(url,),callback=dl_callback)
            
        
    
            
        
