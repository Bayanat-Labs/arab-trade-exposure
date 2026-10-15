SET memory_limit='1500MB';
SET threads=2;
SET VARIABLE data_root='data';
SET VARIABLE analysis_year=2024;
SET VARIABLE analysis_country=818;

-- Different denominators are intentional: this country's import origins vs
-- each exporter's total worldwide exports of the same HS92 product.
WITH local_markets AS (
  SELECT * FROM read_parquet(getvariable('data_root')||'/exposures/year='||
    cast(getvariable('analysis_year') AS VARCHAR)||'/*.parquet',hive_partitioning=false)
  WHERE country_code=getvariable('analysis_country') AND direction='import'
), global_markets AS (
  SELECT * FROM read_parquet(getvariable('data_root')||'/global_context/year='||
    cast(getvariable('analysis_year') AS VARCHAR)||'/*.parquet',hive_partitioning=false)
)
SELECT l.year,l.country_code,l.country_name,l.hs6,l.product_is_standard_hs6,
  l.total_value_usd AS country_observed_import_value_usd,
  l.partner_count AS country_origin_count,l.hhi AS country_import_origin_hhi,
  l.top1_partner_code AS country_top_origin,l.top1_share AS country_top_origin_share,
  g.global_value_usd AS worldwide_product_trade_value_usd,
  g.supplier_count AS global_exporter_area_count,g.global_supplier_hhi,
  g.top1_exporter_code AS worldwide_top_exporter,g.top1_exporter_share,
  g.hs6 IS NOT NULL AS global_context_present,
  l.source_version,l.source_archive_sha256
FROM local_markets l LEFT JOIN global_markets g USING(year,hs6)
ORDER BY country_observed_import_value_usd DESC,l.hs6;
