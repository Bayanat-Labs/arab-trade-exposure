SET memory_limit='1500MB';
SET threads=2;
SET VARIABLE data_root='data';
SET VARIABLE analysis_year=2024;
SET VARIABLE analysis_country=818; -- Source BACI code: Egypt. Do not substitute ISO numeric codes.
SET VARIABLE analysis_direction='import'; -- import or export
SET VARIABLE analysis_product='100190'; -- Exact HS92 source code, not a code from another HS revision.

-- All recorded partners remain in the denominator. No displayed top-N truncation.
WITH selected AS (
  SELECT *,CASE WHEN getvariable('analysis_direction')='import' THEN exporter_code ELSE importer_code END AS partner_code,
    CASE WHEN getvariable('analysis_direction')='import' THEN exporter_name ELSE importer_name END AS partner_name,
    CASE WHEN getvariable('analysis_direction')='import' THEN exporter_in_scope ELSE importer_in_scope END AS partner_in_scope,
    CASE WHEN getvariable('analysis_direction')='import' THEN exporter_current_member ELSE importer_current_member END AS partner_current_member
  FROM read_parquet(getvariable('data_root')||'/flows/year='||
    cast(getvariable('analysis_year') AS VARCHAR)||'/*.parquet',hive_partitioning=false)
  WHERE hs6=getvariable('analysis_product') AND
    ((getvariable('analysis_direction')='import' AND importer_code=getvariable('analysis_country')) OR
     (getvariable('analysis_direction')='export' AND exporter_code=getvariable('analysis_country')))
)
SELECT year,hs6,product_is_standard_hs6,partner_code,partner_name,
  partner_in_scope,partner_current_member,trade_value_1000usd,
  trade_value_1000usd*1000 AS trade_value_usd,
  cast(trade_value_1000usd AS DOUBLE)/nullif(cast(sum(trade_value_1000usd) OVER() AS DOUBLE),0) AS observed_value_share,
  quantity_tonnes,quantity_missing,v_raw,q_raw,
  source_version,source_archive_sha256,source_member,source_record_ordinal
FROM selected ORDER BY trade_value_1000usd DESC,partner_code;
