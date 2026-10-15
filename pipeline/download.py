"""Fetch the fixed CEPII BACI archive, validating resumable responses."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import requests

URL="https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS92_V202601.zip"
EXPECTED_BYTES=2417732497
VERSION="202601"

def save(path,obj):
    path=Path(path)
    tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(json.dumps(obj,ensure_ascii=False,indent=2)+"\n")
    os.replace(tmp,path)

def download(output):
    private=Path(output)/"private"
    private.mkdir(parents=True,exist_ok=True,mode=0o700)
    target=private/"BACI_HS92_V202601.zip"
    part=private/"BACI_HS92_V202601.zip.part"
    manifest=private/"source.json"
    session=requests.Session()
    session.headers.update({"User-Agent":"Bayanat-Labs-ArabTradeExposure/0.1 (public bulk data research; https://github.com/Bayanat-Labs)","Accept-Encoding":"identity"})
    response=session.head(URL,timeout=60)
    response.raise_for_status()
    total=int(response.headers["Content-Length"])
    etag=response.headers.get("ETag")
    if total!=EXPECTED_BYTES:
        raise RuntimeError("Pinned source size changed; review a new version explicitly")
    meta={"archive_url":URL,"content_length":total,"etag":etag,"last_modified":response.headers.get("Last-Modified"),"version":VERSION,"nomenclature":"HS92","years":list(range(1995,2025)),"publisher":"CEPII","license":"Etalab Open Licence 2.0","license_url":"https://www.etalab.gouv.fr/licence-ouverte-open-licence/","publication_url":"https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html","started_at":datetime.now(timezone.utc).isoformat(),"complete":False}
    old=json.loads(manifest.read_text()) if manifest.exists() else None
    if old and (old["archive_url"],old["etag"],old["content_length"])!=(URL,etag,total):
        raise RuntimeError("Source changed; do not mix vintages or resume into another object")
    if old and old.get("complete") and target.exists() and target.stat().st_size==total:
        print(json.dumps({"status":"already_complete","archive":str(target)}),flush=True)
        return
    save(manifest,meta)
    for attempt in range(10):
        offset=part.stat().st_size if part.exists() else 0
        if offset==total:
            break
        if shutil.disk_usage(private).free<total-offset+3*1024**3:
            raise RuntimeError("Insufficient disk headroom")
        if offset and not etag:
            raise RuntimeError("Safe resume requires an ETag")
        headers={"Range":f"bytes={offset}-","If-Range":etag} if offset else {}
        try:
            with session.get(URL,headers=headers,stream=True,timeout=(30,90)) as r:
                r.raise_for_status()
                if offset and (r.status_code!=206 or r.headers.get("Content-Range")!=f"bytes {offset}-{total-1}/{total}"):
                    raise RuntimeError("Resume range was not confirmed")
                if not offset and r.status_code!=200:
                    raise RuntimeError("Unexpected partial response")
                if etag and r.headers.get("ETag")!=etag:
                    raise RuntimeError("ETag changed during download")
                count,last=offset,time.monotonic()
                with part.open("ab" if offset else "wb") as f:
                    for chunk in r.iter_content(1024*1024):
                        if not chunk:continue
                        f.write(chunk)
                        count+=len(chunk)
                        if count>total:raise RuntimeError("Response larger than fixed object")
                        if time.monotonic()-last>=20:
                            print(json.dumps({"downloaded_bytes":count,"total_bytes":total}),flush=True)
                            last=time.monotonic()
                if count!=total:raise requests.ConnectionError("Truncated response")
            break
        except (requests.ConnectionError,requests.Timeout) as e:
            print(json.dumps({"retry":attempt+1,"message":str(e)[:180]}),flush=True)
            time.sleep(min(2**attempt,30))
    if not part.exists() or part.stat().st_size!=total:
        raise RuntimeError("Incomplete archive after retries")
    digest=hashlib.sha256()
    with part.open("rb") as f:
        for chunk in iter(lambda:f.read(8*1024*1024),b""):digest.update(chunk)
    os.replace(part,target)
    meta.update(complete=True,downloaded_at=datetime.now(timezone.utc).isoformat(),sha256=digest.hexdigest(),archive=str(target.resolve()))
    save(manifest,meta)
    print(json.dumps({"status":"complete","sha256":meta["sha256"],"bytes":total}),flush=True)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",required=True)
    args=parser.parse_args()
    download(args.output)
