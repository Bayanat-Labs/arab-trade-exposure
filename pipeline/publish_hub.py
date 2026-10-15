"""Stage the verified package privately, check remote hashes, then publish.

Uses the user's normal Hugging Face login. Credentials are never copied into
the repository, app, manifests, process arguments or logs.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
from huggingface_hub import HfApi, hf_hub_download

REPO='Bayanat-Labs/arab-trade-exposure'
TAG='v0.1.0'

def save(path,obj):
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n')

def hash_file(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()

def verify_local(release):
    validation=json.loads((release/'reports/validation.json').read_text())
    if not validation.get('passed') or len(validation.get('years',[]))!=30:
        raise ValueError('Full validated release required')
    for line in (release/'SHA256SUMS').read_text().splitlines():
        expected,name=line.split('  ',1)
        path=release/name
        if not path.is_relative_to(release) or hash_file(path)!=expected:
            raise ValueError(f'Local release checksum mismatch: {name}')
    return validation

def publish(output,explorer):
    output=Path(output).resolve();release=output/'release'
    verify_local(release)
    state_path=output/'private/publication.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {'repo_id':REPO,'dataset_staged':False,'space_staged':False}
    if state['repo_id']!=REPO:raise ValueError('Publication state belongs to another repo')
    api=HfApi()
    # A repository created by someone else is never adopted silently.
    exists=api.repo_exists(REPO,repo_type='dataset')
    if exists and not state.get('dataset_created'):
        raise ValueError('Dataset repo already exists outside this build publication state')
    if not exists:
        api.create_repo(REPO,repo_type='dataset',private=True)
        state['dataset_created']=True;save(state_path,state)
    if not state.get('dataset_staged'):
        files=[str(p.relative_to(release)) for p in sorted(release.rglob('*')) if p.is_file()]
        # Bounded commits keep large-file preuploads and metadata requests small.
        for start in range(0,len(files),70):
            batch=files[start:start+70]
            print(json.dumps({'stage':'upload_dataset','first_file':start,'files_in_batch':len(batch),'total_files':len(files)}),flush=True)
            info=api.upload_folder(repo_id=REPO,repo_type='dataset',folder_path=release,allow_patterns=batch,commit_message=f'Arab Trade Exposure0.1.0 verified package, batch{start//70+1}')
            state['dataset_commit']=info.oid;save(state_path,state)
        remote=api.repo_info(REPO,repo_type='dataset',files_metadata=True)
        records={x.rfilename:x for x in remote.siblings}
        for path in sorted((release/'data').rglob('*.parquet')):
            name=str(path.relative_to(release));entry=records.get(name)
            if entry is None:
                raise ValueError(f'Remote Parquet missing: {name}')
            remote_hash=entry.lfs.sha256 if entry.lfs else hash_file(Path(hf_hub_download(REPO,name,repo_type='dataset',revision=remote.sha,cache_dir=output/'private'/'remote-verification-cache')))
            if remote_hash!=hash_file(path):
                raise ValueError(f'Remote Parquet hash does not match validated local file: {name}')
        state['dataset_commit']=remote.sha
        api.create_tag(REPO,repo_type='dataset',tag=TAG,revision=remote.sha,exist_ok=True)
        state['dataset_staged']=True;save(state_path,state)
    # Public data and documentation are ready before the application can read it.
    api.update_repo_settings(REPO,repo_type='dataset',private=False)
    state['dataset_public']=True;save(state_path,state)
    explorer=Path(explorer).resolve()
    for name in ('DATA_LICENSE.txt','ATTRIBUTION.txt'):
        shutil.copy2(release/name,explorer/name)
    exists=api.repo_exists(REPO,repo_type='space')
    if exists and not state.get('space_created'):
        raise ValueError('Space repo already exists outside this build publication state')
    if not exists:
        # The explorer is a self-contained static app. DuckDB-Wasm queries the
        # immutable public data tag in the visitor's browser, without a server
        # or a paid organization compute plan.
        api.create_repo(REPO,repo_type='space',space_sdk='static',private=True)
        state['space_created']=True;save(state_path,state)
    if not state.get('space_staged'):
        info=api.upload_folder(repo_id=REPO,repo_type='space',folder_path=explorer,ignore_patterns=['__pycache__/**','*.pyc','.cache/**'],commit_message='Release audited Arab Trade Exposure explorer0.1.0')
        state['space_commit']=info.oid
        api.create_tag(REPO,repo_type='space',tag=TAG,revision=info.oid,exist_ok=True)
        state['space_staged']=True;save(state_path,state)
    api.update_repo_settings(REPO,repo_type='space',private=False)
    state['space_public']=True;save(state_path,state)
    print(json.dumps(state),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--explorer',required=True)
    a=p.parse_args();publish(a.output,a.explorer)
