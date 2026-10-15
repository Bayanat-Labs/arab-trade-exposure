# Browser engine rebuild

From this directory run:

```sh
npm ci --ignore-scripts --no-audit --no-fund
node rebuild.mjs
```

The lockfile pins official npm tarballs, including their integrity digests, for DuckDB-Wasm 1.32.0, Apache Arrow 17.0.0, esbuild 0.28.2 and dependencies. The script bundles the browser module, copies the matched single-threaded EH worker/Wasm and installed license notices, and updates the published SHA-256 manifest. No browser automation is involved.

The separately preserved DuckDB MIT license and notices for dependencies already embedded in the official worker/browser distribution are in `../assets/vendor/`; their official retrieval URLs and hashes are recorded in `notice-sources.json`. Preserve those files on rebuild.

The bundle uses canonical local module-path comments. The generated assets and their SHA-256 hashes are recorded in the manifest. Review generated changes and rerun browser smoke checks before publication.
