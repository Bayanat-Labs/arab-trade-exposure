/** Rebuild the pinned browser engine. Run npm ci --ignore-scripts first. */
import {build} from 'esbuild';
import {readFile,writeFile,copyFile,readdir,stat} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const root=path.dirname(fileURLToPath(import.meta.url));
const vendor=path.resolve(root,'../assets/vendor');
const dependency=path.join(root,'node_modules/@duckdb/duckdb-wasm');
const lock=JSON.parse(await readFile(path.join(root,'package-lock.json'),'utf8'));
const source=lock.packages['node_modules/@duckdb/duckdb-wasm'];
const banner='/* Modified by Bayanat: bundled DuckDB-Wasm 1.32.0 and Apache Arrow 17.0.0 as one browser ES module with esbuild 0.28.2. Original upstream licenses and NOTICE files are preserved alongside this file. */';
await build({absWorkingDir:root,entryPoints:['node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser.mjs'],bundle:true,format:'esm',platform:'browser',target:'es2022',outfile:path.join(vendor,'duckdb-browser-bundle.mjs'),banner:{js:banner}});
for(const name of ['duckdb-browser-eh.worker.js','duckdb-eh.wasm'])await copyFile(path.join(dependency,'dist',name),path.join(vendor,name));
for(const [sourceName,destination] of [
 ['apache-arrow/LICENSE.txt','LICENSE-apache-arrow.txt'],
 ['apache-arrow/NOTICE.txt','NOTICE-apache-arrow.txt'],
 ['flatbuffers/LICENSE','LICENSE-flatbuffers.txt'],
 ['tslib/LICENSE.txt','LICENSE-tslib.txt'],
])await copyFile(path.join(root,'node_modules',sourceName),path.join(vendor,destination));
await copyFile(path.join(dependency,'package.json'),path.join(vendor,'package.json'));
const previous=JSON.parse(await readFile(path.join(vendor,'provenance.json'),'utf8'));
const files={};
for(const name of (await readdir(vendor)).sort()){
 if(name==='provenance.json')continue;
 const file=path.join(vendor,name);
 if(!(await stat(file)).isFile())continue;
 const contents=await readFile(file);
 files[name]={bytes:contents.length,sha256:createHash('sha256').update(contents).digest('hex')};
}
await writeFile(path.join(vendor,'provenance.json'),JSON.stringify({...previous,version:'1.32.0',npm_tarball:source.resolved,npm_integrity:source.integrity,bundle_command:'cd vendor-build && npm ci --ignore-scripts --no-audit --no-fund && node rebuild.mjs',files},null,2)+'\n');
console.log('Rebuilt pinned single-threaded EH browser assets and updated SHA-256 manifest.');
