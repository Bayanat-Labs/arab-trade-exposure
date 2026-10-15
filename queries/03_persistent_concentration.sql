SET memory_limit='1500MB';
SET threads=2;
SET VARIABLE data_root='data';
SET VARIABLE analysis_year=2024;
SET VARIABLE window_years=3;
SET VARIABLE analysis_country=818;
SET VARIABLE analysis_direction='import';

-- A descriptive review list over complete observed markets in every window year.
-- Mean annual HHI is explicitly not HHI of pooled multi-year trade values.
SELECT country_code,direction,hs6,min(country_name) AS country_name,
  min(country_scope_status) AS country_scope_status,
  min(year) AS first_year,max(year) AS last_year,count(*) AS observed_years,
  min(total_value_usd) AS minimum_annual_value_usd,
  avg(hhi) AS mean_annual_hhi,min(hhi) AS minimum_annual_hhi,
  min(top1_share) AS minimum_annual_top1_share,
  count(DISTINCT top1_partner_code)=1 AS same_largest_recorded_partner_each_year,
  list(top1_partner_code ORDER BY year) AS top1_partner_codes_by_year,
  list(year ORDER BY year) AS years,
  min(quantity_value_coverage_share) AS minimum_quantity_value_coverage_share,
  min(source_version) AS source_version
FROM read_parquet(getvariable('data_root')||'/exposures/year=*/*.parquet',hive_partitioning=false)
WHERE country_code=getvariable('analysis_country')
  AND direction=getvariable('analysis_direction') AND product_is_standard_hs6
  AND year BETWEEN getvariable('analysis_year')-getvariable('window_years')+1 AND getvariable('analysis_year')
GROUP BY country_code,direction,hs6
HAVING count(*)=getvariable('window_years')
ORDER BY minimum_annual_hhi DESC,minimum_annual_value_usd DESC,hs6
LIMIT 100;
