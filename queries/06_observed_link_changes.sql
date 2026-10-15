SET memory_limit='1500MB';
SET threads=2;
SET VARIABLE data_root='data';
SET VARIABLE analysis_year=2024; -- Comparing 2023 and 2024, never a nonadjacent window.
SET VARIABLE analysis_country=818;
SET VARIABLE analysis_direction='import';

-- Presence changes only: null absent values are not replaced with numeric zero.
WITH changes AS (
  SELECT * FROM read_parquet(getvariable('data_root')||'/link_observation_changes/year='||
    cast(getvariable('analysis_year') AS VARCHAR)||'/*.parquet',hive_partitioning=false)
  WHERE (getvariable('analysis_direction')='import' AND importer_code=getvariable('analysis_country'))
     OR (getvariable('analysis_direction')='export' AND exporter_code=getvariable('analysis_country'))
), evidence AS (
  SELECT * FROM read_parquet(getvariable('data_root')||'/flows/year=*/*.parquet',hive_partitioning=false)
  WHERE year BETWEEN getvariable('analysis_year')-1 AND getvariable('analysis_year')
)
SELECT c.previous_year,c.year,c.hs6,c.product_is_standard_hs6,
  c.exporter_code,c.importer_code,c.exporter_name,c.importer_name,
  c.observation_change,c.present_previous,c.present_current,
  c.previous_value_1000usd,c.current_value_1000usd,
  e.source_member,e.source_record_ordinal,e.v_raw,e.q_raw,
  c.source_version,c.source_archive_sha256,
  'Source-record observation change; not verified zero trade or real market entry/exit.' AS interpretation
FROM changes c LEFT JOIN evidence e
  ON c.hs6=e.hs6 AND c.exporter_code=e.exporter_code AND c.importer_code=e.importer_code
  AND e.year=CASE WHEN c.present_current THEN c.year ELSE c.previous_year END
ORDER BY coalesce(c.current_value_1000usd,c.previous_value_1000usd) DESC,c.hs6,c.exporter_code,c.importer_code
LIMIT 100;
