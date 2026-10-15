"""Read-only, bounded annual-file explorer for Bayanat Arab Trade Exposure."""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import logging
import os
from pathlib import Path
import re
import threading
import unicodedata
from urllib.parse import parse_qs, urlparse

import duckdb
from huggingface_hub import hf_hub_download

ROOT = Path(__file__).resolve().parent
REPO = "Bayanat-Labs/arab-trade-exposure"
REVISION = os.environ.get("DATASET_REVISION", "v0.1.0")
SOURCE_URL = "https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html"
YEARS = tuple(range(1995, 2025))
LOCAL_DATA: Path | None = None
LOCK = threading.RLock()


def normalized(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


with (ROOT / "assets" / "country_codes_V202601.csv").open(encoding="utf-8-sig", newline="") as stream:
    COUNTRY_NAMES = {int(row["country_code"]): row["country_name"] for row in csv.DictReader(stream)}
with (ROOT / "assets" / "country_scope.csv").open(encoding="utf-8-sig", newline="") as stream:
    SCOPE = {int(row["baci_code"]): row for row in csv.DictReader(stream)}
with (ROOT / "assets" / "country_label_repairs.csv").open(encoding="utf-8-sig", newline="") as stream:
    LABEL_REPAIRS = {int(row["baci_code"]): row["display_country_name"] for row in csv.DictReader(stream)
                     if COUNTRY_NAMES.get(int(row["baci_code"])) == row["source_country_name"]}
with (ROOT / "assets" / "product_codes_HS92_V202601.csv").open(encoding="utf-8-sig", newline="") as stream:
    PRODUCTS = {row["code"]: row["description"] for row in csv.DictReader(stream)}
SEARCH_PRODUCTS = {code: normalized(code + " " + name) for code, name in PRODUCTS.items()}


def dataset_file(table: str, year: int) -> Path:
    if table not in {"flows", "exposures", "global_context"} or year not in YEARS:
        raise ValueError("Unsupported dataset file")
    filename = f"data/{table}/year={year}/part-00000.parquet"
    if LOCAL_DATA:
        result = LOCAL_DATA / filename
        if not result.is_file():
            raise FileNotFoundError(f"Local dataset file is not ready: {filename}")
        return result
    return Path(hf_hub_download(repo_id=REPO, repo_type="dataset", revision=REVISION, filename=filename))


def clean_json(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): clean_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(item) for item in value]
    return value


def fetch_dicts(connection, query: str, parameters: list) -> list[dict]:
    result = connection.execute(query, parameters)
    fields = [column[0] for column in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def validate_filters(query: dict) -> dict:
    try:
        year = int(query.get("year", ["2024"])[0])
        country = int(query.get("country", ["818"])[0])
    except ValueError as error:
        raise ValueError("Year and country must be numeric choices") from error
    direction = query.get("direction", ["import"])[0]
    search = query.get("search", [""])[0].strip()
    product = query.get("product", [""])[0].strip()
    if year not in YEARS or country not in SCOPE or direction not in {"import", "export"}:
        raise ValueError("Unsupported year, country or direction")
    if len(search) > 100 or len(product) > 6:
        raise ValueError("Filter is too long")
    if product and product not in PRODUCTS:
        raise ValueError("Choose a source product code")
    return {"year": year, "country": country, "direction": direction, "search": search, "product": product}


def explore(filters: dict) -> dict:
    year, country, direction = filters["year"], filters["country"], filters["direction"]
    exposure_path = dataset_file("exposures", year)
    scope = SCOPE[country]
    with duckdb.connect() as connection:
        connection.execute("SET memory_limit='1GB'")
        connection.execute("SET threads=2")
        parameters = [str(exposure_path), country, direction]
        matching_codes = [code for code, text in SEARCH_PRODUCTS.items() if normalized(filters["search"]) in text]
        product_filter = " AND list_contains(?, hs6)" if filters["search"] else ""
        if filters["search"]:
            parameters.append(matching_codes)
        base = "FROM read_parquet(?, hive_partitioning=false) WHERE country_code=? AND direction=?" + product_filter
        totals = fetch_dicts(connection, "SELECT count(*) product_count, sum(total_value_1000usd) total_value_1000usd, sum(partner_count) observed_links " + base, parameters)[0]
        top_products = fetch_dicts(connection, "SELECT * " + base + " ORDER BY total_value_1000usd DESC, hs6 LIMIT 20", parameters)
        for row in top_products:
            row["product_name"] = PRODUCTS.get(row["hs6"], row["hs6"])
        requested = filters["product"]
        product = requested or (top_products[0]["hs6"] if top_products else "")
        metrics = None
        context = None
        partners = []
        if product:
            selected = fetch_dicts(connection, "SELECT * FROM read_parquet(?,hive_partitioning=false) WHERE country_code=? AND direction=? AND hs6=?", [str(exposure_path), country, direction, product])
            metrics = selected[0] if selected else None
            if metrics:
                context_path = dataset_file("global_context", year)
                contexts = fetch_dicts(connection, "SELECT * FROM read_parquet(?,hive_partitioning=false) WHERE hs6=?", [str(context_path), product])
                context = contexts[0] if contexts else None
                flow_path = dataset_file("flows", year)
                # These identifiers are fixed, server-chosen constants, never user SQL.
                country_column = "importer_code" if direction == "import" else "exporter_code"
                partner_side = "exporter" if direction == "import" else "importer"
                partners = fetch_dicts(connection, f"""
                    SELECT {partner_side}_code partner_code, {partner_side}_name partner_name,
                      {partner_side}_name_ar partner_name_ar,
                      {partner_side}_in_scope partner_in_scope,
                      {partner_side}_current_member partner_current_member,
                      trade_value_1000usd,v_raw,q_raw,quantity_tonnes,quantity_missing,quantity_positive,
                      source_archive_sha256,source_member,source_member_crc32,source_record_ordinal,source_record_id
                    FROM read_parquet(?,hive_partitioning=false)
                    WHERE {country_column}=? AND hs6=?
                    ORDER BY trade_value_1000usd DESC,partner_code LIMIT 250
                """, [str(flow_path), country, product])
                total = metrics["total_value_1000usd"]
                for row in partners:
                    # Curated presentation repair only; original source fields
                    # and the CSV/evidence exports stay unchanged.
                    row["partner_display_name"] = LABEL_REPAIRS.get(row["partner_code"], row["partner_name"])
                    row["partner_display_name_ar"] = LABEL_REPAIRS.get(row["partner_code"], row["partner_name_ar"])
                    row["share"] = row["trade_value_1000usd"] / total if total else None
                    row["value_usd"] = row["trade_value_1000usd"] * 1000
        return clean_json({
            "filters": {**filters, "product": product}, "country": scope,
            "totals": totals, "top_products": top_products, "metrics": metrics,
            "global_context": context, "partners": partners,
            "product_name": PRODUCTS.get(product, ""), "source_url": SOURCE_URL,
            "dataset_url": f"https://huggingface.co/datasets/{REPO}/tree/{REVISION}",
            "revision": REVISION, "latest_year_provisional": year == 2024,
            "limits": "Observed positive reconciled trade flows; missing links are not proven zero. Concentration describes this recorded trade, not disruption risk, domestic-consumption dependence or a forecast. Quantity coverage is by trade value, not volume."
        })


class Handler(BaseHTTPRequestHandler):
    def send_bytes(self, status: int, body: bytes, content_type: str, attachment: str | None = None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store" if content_type.startswith("application/json") else "public,max-age=300")
        if attachment:
            self.send_header("Content-Disposition", f'attachment; filename="{attachment}"')
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status: int, value):
        self.send_bytes(status, json.dumps(clean_json(value), ensure_ascii=False, allow_nan=False).encode(), "application/json; charset=utf-8")

    def do_GET(self):
        request = urlparse(self.path)
        try:
            if request.path in {"/", "/index.html"}:
                self.send_bytes(200, (ROOT / "index.html").read_bytes(), "text/html; charset=utf-8")
            elif request.path == "/health":
                self.send_json(200, {"status": "ok", "revision": REVISION})
            elif request.path == "/api/meta":
                self.send_json(200, {"years": YEARS, "countries": list(SCOPE.values()), "revision": REVISION,
                    "dataset_url": f"https://huggingface.co/datasets/{REPO}", "source_url": SOURCE_URL})
            elif request.path == "/api/products":
                query = parse_qs(request.query, keep_blank_values=True).get("q", [""])[0][:100]
                search = normalized(query)
                self.send_json(200, [{"code": code, "name": PRODUCTS[code]} for code, text in SEARCH_PRODUCTS.items() if search in text][:30])
            elif request.path in {"/api/explore", "/api/download.csv"}:
                filters = validate_filters(parse_qs(request.query, keep_blank_values=True))
                with LOCK:
                    result = explore(filters)
                if request.path.endswith(".csv"):
                    columns = ["partner_code", "partner_name", "share", "v_raw", "q_raw", "trade_value_1000usd", "quantity_tonnes", "source_member", "source_record_ordinal", "source_record_id", "source_archive_sha256"]
                    output = io.StringIO(newline="")
                    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(result["partners"])
                    self.send_bytes(200, output.getvalue().encode("utf-8-sig"), "text/csv; charset=utf-8", "bayanat-observed-trade-links.csv")
                else:
                    self.send_json(200, result)
            else:
                self.send_json(404, {"error": "Page not found"})
        except ValueError as error:
            self.send_json(400, {"error": str(error)})
        except Exception:
            logging.exception("Dataset request unavailable")
            self.send_json(503, {"error": "The selected annual data is still loading or unavailable. Please try again. The dataset link provides direct downloads."})

    def log_message(self, format, *args):
        logging.info("%s", format % args)


def main():
    global LOCAL_DATA
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-data", type=Path)
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "7860")))
    args = parser.parse_args()
    LOCAL_DATA = args.local_data.resolve() if args.local_data else None
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
