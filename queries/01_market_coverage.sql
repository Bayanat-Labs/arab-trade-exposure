-- Run from the downloaded release root. Edit these SET VARIABLE values.
SET memory_limit='1500MB';
SET threads=2;
SET VARIABLE data_root='data';
SET VARIABLE analysis_year=2024;

-- Country totals use each product market once; imports and exports stay separate.
SELECT year,country_code,min(country_name) AS country_name,
  min(country_scope_status) AS country_scope_status,direction,
  count(*) AS observed_product_markets,
  count(*) FILTER(WHERE product_is_standard_hs6) AS standard_hs6_markets,
  count(*) FILTER(WHERE NOT product_is_standard_hs6) AS legacy_code_markets,
  sum(total_value_1000usd) AS total_value_1000usd,
  sum(total_value_usd) AS total_value_usd,
  sum(observed_partner_count) AS observed_bilateral_rows,
  sum(positive_quantity_rows) AS positive_quantity_rows,
  sum(missing_quantity_rows) AS missing_quantity_rows,
  sum(nonpositive_quantity_rows) AS nonpositive_quantity_rows,
  cast(sum(quantity_covered_value_1000usd) AS DOUBLE)
    /nullif(cast(sum(total_value_1000usd) AS DOUBLE),0) AS quantity_value_coverage_share,
  min(source_version) AS source_version,
  min(source_archive_sha256) AS source_archive_sha256
FROM read_parquet(getvariable('data_root')||'/exposures/year='||
  cast(getvariable('analysis_year') AS VARCHAR)||'/*.parquet',hive_partitioning=false)
GROUP BY year,country_code,direction
ORDER BY country_code,direction;
