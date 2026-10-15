SET memory_limit='1500MB';
SET threads=2;
SET VARIABLE data_root='data';
SET VARIABLE analysis_year=2024;
SET VARIABLE analysis_country=818;
SET VARIABLE analysis_direction='import';
SET VARIABLE analysis_product='100190';

-- Historical predecessor areas are shown separately from current member areas.
WITH selected AS (
  SELECT *,
    CASE WHEN getvariable('analysis_direction')='import' THEN exporter_in_scope ELSE importer_in_scope END AS partner_in_scope,
    CASE WHEN getvariable('analysis_direction')='import' THEN exporter_current_member ELSE importer_current_member END AS partner_current_member
  FROM read_parquet(getvariable('data_root')||'/flows/year='||
    cast(getvariable('analysis_year') AS VARCHAR)||'/*.parquet',hive_partitioning=false)
  WHERE hs6=getvariable('analysis_product') AND
    ((getvariable('analysis_direction')='import' AND importer_code=getvariable('analysis_country')) OR
     (getvariable('analysis_direction')='export' AND exporter_code=getvariable('analysis_country')))
), grouped AS (
  SELECT CASE WHEN partner_current_member THEN 'modern_member_source_area'
      WHEN partner_in_scope THEN 'historical_predecessor_source_area'
      ELSE 'outside_regional_scope' END AS partner_group,
    count(*) AS recorded_links,sum(trade_value_1000usd) AS value_1000usd
  FROM selected GROUP BY 1
)
SELECT partner_group,recorded_links,value_1000usd,value_1000usd*1000 AS value_usd,
  cast(value_1000usd AS DOUBLE)/nullif(cast(sum(value_1000usd) OVER() AS DOUBLE),0) AS share_of_all_observed_value
FROM grouped ORDER BY value_1000usd DESC,partner_group;
