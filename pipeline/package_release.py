"""Assemble a publication package only after full-corpus validation passes."""
from __future__ import annotations
import argparse
import csv
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import shutil
import zipfile
import pyarrow as pa
import pyarrow.parquet as pq
from ingest import load_lookups

REPO='Bayanat-Labs/arab-trade-exposure'
VERSION='0.1.0'
SOURCE=Path(__file__).resolve().parents[1]

def dump(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()

def package(output):
    output=Path(output).resolve();release=output/'release'
    validation=json.loads((release/'reports/validation.json').read_text())
    if not validation.get('passed') or len(validation.get('years',[]))!=30:
        raise ValueError('Complete validated thirty-year corpus required')
    source=json.loads((output/'private/source.json').read_text())
    if not source.get('complete') or source['sha256']!=validation['source_archive_sha256']:
        raise ValueError('Source download/validation mismatch')
    if validation.get('country_scope_sha256')!=sha(SOURCE/'country_scope.csv') or validation.get('ingest_code_sha256')!=sha(SOURCE/'pipeline/ingest.py') or validation.get('validator_code_sha256')!=sha(SOURCE/'pipeline/validate.py'):
        raise ValueError('Validated scope/code changed after full audit')
    checked=validation.get('checked_data_files',[])
    if len(checked)!=119 or len({x['path'] for x in checked})!=119:raise ValueError('Expected119 validated analytical files')
    for item in checked:
        path=release/item['path']
        if not path.is_relative_to(release) or not path.is_file() or sha(path)!=item['sha256']:raise ValueError(f'Post-validation data change: {item["path"]}')
    with zipfile.ZipFile(source['archive']) as z:
        expected={f'BACI_HS92_Y{y}_V202601.csv' for y in range(1995,2025)}|{'country_codes_V202601.csv','product_codes_HS92_V202601.csv','Readme.txt'}
        if set(z.namelist())!=expected or len(z.namelist())!=33:raise ValueError('Unexpected archive members')
        for member in ('Readme.txt','country_codes_V202601.csv','product_codes_HS92_V202601.csv'):
            if z.read(member)!=(SOURCE/'source_metadata'/member).read_bytes():raise ValueError(f'Source metadata changed: {member}')
        countries,products=load_lookups(z,SOURCE/'country_scope.csv')
        product_rows=products.to_pylist()
        for row in product_rows:
            row['product_is_unspecified']=row['hs6'] in ('999999','9999AA')
            row['product_has_classified_detail']=row['product_is_standard_hs6'] and not row['product_is_unspecified']
        products=pa.Table.from_pylist(product_rows)
        repairs={int(r['baci_code']):r for r in csv.DictReader((SOURCE/'country_label_repairs.csv').open())}
        country_rows=countries.to_pylist()
        for row in country_rows:
            repair=repairs.get(row['country_code'])
            if repair and repair['source_country_name']!=row['country_name']:raise ValueError('Country repair source label mismatch')
            row['country_name_display']=repair['display_country_name'] if repair else row['country_name']
            row['country_label_repaired']=repair is not None
        countries=pa.Table.from_pylist(country_rows)
        for name,table in [('countries',countries),('products',products)]:
            folder=release/'data'/name;folder.mkdir(parents=True,exist_ok=True)
            pq.write_table(table,folder/'part-00000.parquet',compression='zstd')
        inventory=[{'member':i.filename,'uncompressed_bytes':i.file_size,'compressed_bytes':i.compress_size,'crc32':f'{i.CRC:08x}'} for i in z.infolist()]
    public_source={k:v for k,v in source.items() if k not in ('archive',)}
    public_source.update(archive_release_date='2026-01-22',presentation_page_last_update='2026-01-30',data_years=list(range(1995,2025)),original_provider='CEPII BACI; reconciled from UN Comtrade',transformations='Arab-region selection, typed lossless source values, source-code identities, Arabic display labels, computed concentration/context/record-presence changes',source_archive_sha256=source['sha256'])
    dump(release/'metadata/source.json',public_source)
    dump(release/'metadata/source_zip_inventory.json',inventory)
    dump(release/'metadata/country_scope_policy.json',json.loads((SOURCE/'country_scope_policy.json').read_text()))
    dump(release/'metadata/publication_policy.json',json.loads((SOURCE/'publication_policy.json').read_text()))
    shutil.copy2(SOURCE/'country_scope.csv',release/'metadata/country_scope.csv')
    shutil.copy2(SOURCE/'country_label_repairs.csv',release/'metadata/country_label_repairs.csv')
    annual=[]
    for year in range(1995,2025):
        report=json.loads((output/f'private/ingest-reports/ingest-{year}.json').read_text())
        if report['status']!='complete':raise ValueError('Incomplete source year')
        annual.append({k:v for k,v in report.items() if k not in ('binding','output_files','error')})
    dump(release/'reports/source_coverage_by_year.json',annual)
    configs=['flows','exposures','global_context','link_observation_changes','countries','products']
    counts={};schemas={}
    for config in configs:
        files=sorted((release/'data'/config).rglob('*.parquet'))
        if not files:raise ValueError(f'No files for {config}')
        schema=pq.read_schema(files[0]);rows=0
        for file in files:
            if not pq.read_schema(file).equals(schema):raise ValueError(f'Inconsistent schema {file}')
            rows+=pq.ParquetFile(file).metadata.num_rows
        counts[config]={'rows':rows,'files':len(files),'bytes':sum(f.stat().st_size for f in files)}
        schemas[config]=[{'name':field.name,'type':str(field.type),'nullable':field.nullable} for field in schema]
    if counts['flows']['rows']!=validation['released_flow_rows'] or counts['exposures']['rows']!=validation['exposure_rows'] or counts['global_context']['rows']!=validation['global_context_rows'] or counts['link_observation_changes']['rows']!=validation['link_change_rows']:
        raise ValueError('Published counts disagree with independent validation')
    dump(release/'metadata/field_schemas.json',schemas)
    summary={'release_version':VERSION,'built_at':datetime.now(timezone.utc).isoformat(),'source_archive_sha256':source['sha256'],'source_version':'202601','nomenclature':'HS92','years':list(range(1995,2025)),'modern_member_count':22,'historical_context_source_areas':1,'global_source_rows_processed':validation['global_source_rows_processed'],'tables':counts,'validation_passed':True,'synthetic_records':0,'primary_row_grain':'directed year/exporter/importer/source-product reconciled flow; not shipment, company or product instance'}
    dump(release/'metadata/release_summary.json',summary)
    # Source is copied from a strict allowlist; private inputs, credentials,
    # browsing captures and machine-specific logs are excluded.
    code=release/'code';code.mkdir(exist_ok=True)
    for folder in ['pipeline','tests','queries','docs','examples']:
        destination=code/folder;destination.mkdir(exist_ok=True)
        for file in (SOURCE/folder).rglob('*'):
            if file.is_file() and '__pycache__' not in file.parts and file.suffix in ('.py','.sql','.md','.json'):
                target=destination/file.relative_to(SOURCE/folder);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(file,target)
    for name in ['country_scope.csv','country_scope_policy.json','country_label_repairs.csv','publication_policy.json','requirements.txt','DATA_LICENSE.txt','CODE_LICENSE.txt','SOURCE_POLICY_REVIEW.md','CITATION.cff']:
        shutil.copy2(SOURCE/name,code/name)
    for name in ['country_codes_V202601.csv','product_codes_HS92_V202601.csv','Readme.txt']:
        folder=code/'source_metadata';folder.mkdir(exist_ok=True);shutil.copy2(SOURCE/'source_metadata'/name,folder/name)
    for name in ['DATA_LICENSE.txt','CODE_LICENSE.txt']:
        shutil.copy2(SOURCE/name,release/name)
    for folder in ['docs','queries','examples']:
        shutil.copytree(SOURCE/folder,release/folder,dirs_exist_ok=True)
    shutil.copy2(SOURCE/'CITATION.cff',release/'CITATION.cff')
    (release/'.gitattributes').write_text('*.parquet filter=lfs diff=lfs merge=lfs -text\n')
    config_yaml='\n'.join(f'  - config_name: {c}\n    data_files:\n      - split: train\n        path: data/{c}/**/*.parquet' for c in configs)
    rows_table='\n'.join(f"| `{name}` | {info['rows']:,} | {info['bytes']/1024**2:,.1f} MiB |" for name,info in counts.items())
    attribution=f'Source: CEPII, BACI database, HS92, version 202601; archive release date 22 January 2026. Downloaded {source["downloaded_at"][:10]}. Reused under Etalab Open Licence 2.0. Bayanat Labs selected source areas, added display metadata and computed analytical tables. CEPII does not endorse this derived release.'
    (release/'ATTRIBUTION.txt').write_text(attribution+'\n\nBACI: https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html\nLicense: https://raw.githubusercontent.com/etalab/licence-ouverte/master/open-licence.txt\n\nGaulier, G. and Zignago, S. (2010). BACI: International Trade Database at the Product-Level. The 1994–2007 Version. CEPII Working Paper No. 2010-23. https://www.cepii.fr/pdf_pub/wp/2010/wp2010-23.pdf\n')
    card=f'''---
language:
  - en
  - ar
license: other
license_name: etalab-open-licence-2.0
license_link: https://huggingface.co/datasets/{REPO}/blob/main/DATA_LICENSE.txt
pretty_name: Bayanat Arab Trade Exposure
size_categories:
  - 10M<n<100M
tags:
  - arab-region
  - international-trade
  - economics
  - supply-chain
  - parquet
  - baci
configs:
{config_yaml}
---

# Bayanat Arab Trade Exposure

**{counts['flows']['rows']:,} Arab-region trade flow records · 1995–2024 · {counts['exposures']['rows']:,} country/product/year exposure observations.**

Analyze which countries supply a product to an Arab market, where Arab exports go, how concentrated those relationships are, and how recorded trade links change over time. Values retain exact decimal precision and every flow resolves to its original source archive member and data-row ordinal.

English is the default language for this release, its research documentation and the explorer. Arabic is available as an optional interface language.

[Open the explorer](https://huggingface.co/spaces/{REPO}) · [Source and pipeline](https://github.com/{REPO}) · [Worked examples](docs/USE_CASES.md) · [Methodology](docs/METHODOLOGY.md) · [Validation](reports/validation.json)

## What is included

The pipeline scanned **{validation['global_source_rows_processed']:,} worldwide source rows** from one fixed CEPII BACI vintage and retained flows with an endpoint in the declared Arab-region collection. CEPII performed the original trade reconciliation. Bayanat adds a reproducible regional analytical layer, complete observed-partner denominators, conservative missingness and geographic handling, and practical SQL queries.

| Table / Hugging Face config | Rows | Parquet size |
|---|---:|---:|
{rows_table}

`train` is a Hub loading split for a descriptive collection; this release has no ML train/test split or benchmark labels. Flow rows, derived market observations and recorded-link changes have different grains and must not be added as independent purchases. No synthetic trade records were generated.

The scope contains the **22 modern Arab League member geographies plus former Sudan (BACI736) as separately labeled historical context**. Modern Sudan 729 and former Sudan 736 retain separate identities. The collection does not reconstruct historical legal membership. Data coverage differs across source areas and years; an absent record is not confirmed zero trade.

## What people can use it for

- Research import-source concentration for a particular country and HS92 product, with all recorded partners in the denominator.
- Review export-destination concentration and intra-region trade shares.
- Compare national import-source concentration with worldwide exporter concentration for the same product/year.
- Study persistent concentration and newly/no-longer observed bilateral links without inventing zeros or market-entry claims.
- Build trade dashboards, analytical tools and data-engineering examples using year-partitioned Parquet and auditable source traces.

The measures describe recorded merchandise trade. They do **not** measure domestic import dependence, shortage probability, producer concentration, spare capacity, supplier replaceability, company transactions or retail prices. Trade hubs and re-exports matter. Annual values are nominal, reconciled estimates and can be revised upstream.

## Load one year

```python
from datasets import load_dataset

flows = load_dataset(
    "{REPO}", "flows", split="train", streaming=True,
    data_files={{"train": "data/flows/year=2024/*.parquet"}},
    revision="v{VERSION}",
)
row = next(iter(flows))
print(row["hs6"], row["trade_value_1000usd"])  # Decimal; source value in thousands USD
```

For a precise local query without downloading all years:

```python
import duckdb
from huggingface_hub import hf_hub_download

p = hf_hub_download("{REPO}", "data/exposures/year=2024/part-00000.parquet",
                    repo_type="dataset", revision="v{VERSION}")
con = duckdb.connect()
result = con.execute("SELECT hs6, total_value_usd, top1_share, hhi FROM read_parquet(?) "
                     "WHERE country_code=818 AND direction='import' ORDER BY total_value_usd DESC LIMIT 20", [p]).fetchall()
```

Six executable [SQL examples](queries/) cover market coverage, partner evidence, persistent concentration, world context, regional mix and recorded-link changes. They use explicit country, direction, product and year boundaries. Source product descriptions are English; curated Arabic display labels cover regional source areas. These labels are not official Arabic HS translations.

## Precision, provenance and quality

`v_raw` and `q_raw` retain parsed original CSV strings. `trade_value_1000usd` and `quantity_tonnes` are lossless DECIMAL(38,18); quantities marked NA/blank remain missing and reported zero quantities remain zero. No currency conversion or imputation is applied. Dollar totals multiply the native thousands-USD value by exactly1000. Shares and HHI use double precision with a documented validation tolerance.

Exact storage preserves the publisher's numeric representation, including any upstream precision artifacts. It does not imply that economic measurement is accurate to 18 decimal places.

Original BACI country identities are authoritative; they are not interchangeable with numeric ISO codes. HS product strings retain leading zeros. The source-listed legacy non-WCO code `9999AA` remains flagged `product_is_standard_hs6=false`; it is not forced into a numeric HS hierarchy.

That flag checks source-listed six-digit numeric code shape, not external WCO certification. The product lookup separately flags unspecified catalog entries `999999` and `9999AA`. Neither occurs in the regional flow records of this vintage.

The country lookup supplies four explicitly documented display-label encoding repairs (including `TÃ¼rkiye`→`Türkiye`). Original source country labels remain unchanged in flow/exposure records. [Repair map](metadata/country_label_repairs.csv).

Every source ZIP member is fully read with CRC verification. Per-year completion reports bind source archive SHA-256, scope/code versions and output checksums. Invalid critical values, unknown source identities, duplicate native keys and incomplete inputs block a complete-year release. The full validation recomputes market denominators, worldwide totals, arithmetic bounds and link-presence changes independently. It also checks150 original CSV source traces and300 Python Decimal market calculations across30 years. Those are technical prefix checks, not random expert verification of the source statistics.

All18 semantic, failure-path and validator-corruption tests pass. The release audit also independently recomputes every local market and world-product concentration from the complete data.

For source drill-down, use `code/pipeline/trace.py` with a flow's `source_member`, `source_record_ordinal` and `source_archive_sha256`. The raw2.42GB archive is publicly downloadable from CEPII and is not duplicated here. [Machine-readable source metadata](metadata/source.json) · [Field schemas](metadata/field_schemas.json) · [Derived-field dictionary](metadata/derived_data_dictionary.json) · [Yearly source coverage](reports/source_coverage_by_year.json).

## Reproduce

Clone the [pipeline repository](https://github.com/{REPO}), install `requirements.txt`, then run:

```bash
python pipeline/download.py --output /path/to/build
python pipeline/ingest.py --archive /path/to/build/private/BACI_HS92_V202601.zip --output /path/to/build --country-scope country_scope.csv --archive-sha256 {source['sha256']}
python pipeline/derive.py --output /path/to/build --years 1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024
python pipeline/validate.py --output /path/to/build --archive /path/to/build/private/BACI_HS92_V202601.zip
```

Processing extracts one annual CSV at a time and uses bounded DuckDB memory/disk spill. A completed source year can resume only when its bound inputs and output checksums still match. A rebuilt Parquet may have a different row order/byte hash; native record identities and numerical content are the reproducibility contract.

## Attribution and license

{attribution}

[BACI documentation](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html) · [Full data license](DATA_LICENSE.txt). The presentation page's30January2026 update date is separately preserved; it is not the observation year or our download date. Software is [MIT licensed](CODE_LICENSE.txt).

Please cite Gaulier, G. and Zignago, S. (2010), *BACI: International Trade Database at the Product-Level. The1994–2007 Version*, CEPII Working Paper No.2010-23, and this Bayanat derived release v{VERSION}. Existing tools such as OEC, CEPII GeoDep and the Arab Development Portal already provide trade data/analysis. This release claims a portable, audited regional data product, not a new trade-concentration method or first-ever trade corpus.

<details><summary>Arabic overview (optional)</summary>

مجموعة بيانات لتحليل التجارة السلعية للدول العربية خلال1995–2024، مبنية على إصدار ثابت من قاعدةBACI التابعة لمركزCEPII. تتضمن التدفقات التجارية ومؤشرات تركز مصادر الاستيراد ووجهات التصدير، مع الحفاظ على القيم الأصلية وإمكانية تتبع كل سجل إلى مصدره. تُحفظ بيانات السودان السابق منفصلة عن السودان الحالي. غياب سجل لا يعني بالضرورة انعدام التجارة، ولا تُعد مؤشرات التركز تقديرًا لاحتمال النقص أو الاعتماد على الاستيراد لتلبية الاستهلاك المحلي.

</details>
'''
    # Keep prose readable when adjacent literals above include numeric metadata.
    for before,after in [('exactly1000','exactly 1000'),('checks150','checks 150'),('and300','and 300'),('across30','across 30'),('All18','All 18'),('raw2.42GB','raw 2.42 GB'),("page's30January2026","page's 30 January 2026"),('The1994','The 1994'),('No.2010','No. 2010'),('خلال1995','خلال 1995'),('قاعدةBACI','قاعدة BACI'),('لمركزCEPII','لمركز CEPII')]:card=card.replace(before,after)
    (release/'README.md').write_text(card)
    (SOURCE/'README.md').write_text(card[card.index('# Bayanat Arab Trade Exposure'):].replace('`code/pipeline/trace.py`','`pipeline/trace.py`'))
    (code/'README.md').write_text('# Arab Trade Exposure pipeline\n\nEnglish is the default language for the code documentation and results.\n\nThis directory contains the reproducible pipeline, curated scope, original small source lookups, examples, methodology and tests. Install `requirements.txt`, then run `pipeline/download.py`, `pipeline/ingest.py`, `pipeline/derive.py` and `pipeline/validate.py` as documented in the [full source repository](https://github.com/'+REPO+').\n\n[Dataset and validation reports](https://huggingface.co/datasets/'+REPO+') · [Methodology](docs/METHODOLOGY.md)\n\nCode is MIT licensed; source data and derivatives use Etalab Open Licence2.0 with CEPII attribution.\n')
    # Source README uses source-root paths; dataset card is the canonical card.
    dump(release/'metadata/package_versions.json',{'release':VERSION,'python_contract':'>=3.11','duckdb':'1.5.6','pyarrow':pa.__version__})
    files=[p for p in sorted(release.rglob('*')) if p.is_file() and p.name!='SHA256SUMS']
    (release/'SHA256SUMS').write_text(''.join(f'{sha(p)}  {p.relative_to(release)}\n' for p in files))
    print(json.dumps(summary),flush=True)
    return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();package(a.output)
