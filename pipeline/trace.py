"""Resolve one exported flow back to its exact source ZIP member and data row."""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import zipfile

def resolve(archive,member,ordinal,expected_sha256=None):
    archive=Path(archive)
    if ordinal<1:
        raise ValueError("Source record ordinals start at1 after the header")
    if expected_sha256:
        h=hashlib.sha256()
        with archive.open("rb") as f:
            for block in iter(lambda:f.read(8*1024*1024),b""):h.update(block)
        if h.hexdigest()!=expected_sha256:
            raise ValueError("Source archive SHA-256 does not match the exported provenance")
    with zipfile.ZipFile(archive) as z:
        info=z.getinfo(member)
        with z.open(info) as stream:
            rows=csv.DictReader(io.TextIOWrapper(stream,encoding="utf-8-sig",newline=""))
            for n,row in enumerate(rows,1):
                if n==ordinal:
                    return {"source_member":member,"source_member_crc32":f"{info.CRC:08x}","source_record_ordinal":n,"source_row":row,"row_grain":"year, exporter, importer, HS92 product; reconciled trade estimate"}
    raise IndexError("Record ordinal is outside the source member")

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--archive",required=True)
    p.add_argument("--member",required=True)
    p.add_argument("--ordinal",type=int,required=True)
    p.add_argument("--expected-sha256")
    a=p.parse_args()
    print(json.dumps(resolve(a.archive,a.member,a.ordinal,a.expected_sha256),ensure_ascii=False,indent=2))
