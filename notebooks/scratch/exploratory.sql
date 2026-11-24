-- Databricks notebook source
-- PREVIEW BRONZE TABLES
SELECT *
FROM edgar.pipeline.bronze_sub
LIMIT 5;

-- COMMAND ----------

SELECT *
FROM edgar.pipeline.bronze_num
LIMIT 5;

-- COMMAND ----------

-- VERIFY NULLS
SELECT tag, COUNT(DISTINCT adsh) AS filings
FROM edgar.pipeline.bronze_num
WHERE segments IS NULL AND coreg IS NULL AND uom = 'USD' AND qtrs = 4
  AND (tag LIKE '%Revenue%' OR tag LIKE 'Sales%')
GROUP BY tag ORDER BY filings DESC LIMIT 25;

SELECT tag, version, COUNT(DISTINCT adsh) AS filings
FROM edgar.pipeline.bronze_num
WHERE tag IN ('Revenue', 'RevenueFromContractsWithCustomers')
  AND segments IS NULL AND coreg IS NULL AND qtrs = 4
GROUP BY tag, version ORDER BY filings DESC;

-- COMMAND ----------

-- TOP HEALTHCARE COMPANIES
SELECT subsector, COUNT(DISTINCT cik) AS companies
FROM edgar.pipeline.silver_filings
WHERE sector = 'Healthcare'
GROUP BY 1 ORDER BY 2 DESC

-- COMMAND ----------

-- NOTABLE HEALTHCARE COMPANIES
SELECT DISTINCT company_name, sic, sector
FROM edgar.pipeline.silver_filings
WHERE upper(company_name) RLIKE 'UNITEDHEALTH|^HCA|BECTON|PFIZER|ELEVANCE|CIGNA|HUMANA|MCKESSON|WALGREENS|ABBOTT LAB|JOHNSON & JOHNSON'
ORDER BY company_name

-- COMMAND ----------

-- VERIFY ANNUAL FILINGS
SELECT period_year,
       COUNT(*) AS annual_filings,
       SUM(CASE WHEN month(period_end) = 12 THEN 1 ELSE 0 END) AS december_year_ends
FROM edgar.pipeline.gold_company_period
WHERE period_type = 'annual' AND revenue > 0
GROUP BY 1 ORDER BY 1

-- COMMAND ----------

-- CHECK GROWTH
WITH annual AS (
  SELECT cik, company_name AS company, sector, subsector, period_year,
         revenue, net_margin,
         ROW_NUMBER() OVER (PARTITION BY cik, period_year ORDER BY period_end DESC) AS rn
  FROM edgar.pipeline.gold_company_period
  WHERE period_type = 'annual' AND revenue > 0
),
growth AS (
  SELECT c.company, c.sector, c.subsector, c.period_year,
         c.revenue, p.revenue AS prior_revenue,
         (c.revenue - p.revenue) / p.revenue AS revenue_growth,
         c.net_margin - p.net_margin AS margin_change
  FROM annual c
  JOIN annual p
    ON c.cik = p.cik AND c.period_year = p.period_year + 1
   AND c.rn = 1 AND p.rn = 1
  WHERE p.revenue >= 1000000000
    AND c.period_year = 2024
)
SELECT *,
       RANK() OVER (PARTITION BY period_year ORDER BY revenue_growth DESC) AS growth_rank
FROM growth

-- COMMAND ----------

-- VERIFY COMPANY AND SECTOR LEVELS
SELECT c.period_year,
       ROUND(c.rev / 1e12, 3) AS company_level_T,
       ROUND(s.rev / 1e12, 3) AS sector_level_T
FROM (SELECT period_year, SUM(revenue) AS rev
      FROM edgar.pipeline.gold_company_period
      WHERE period_type = 'annual' AND revenue > 0 GROUP BY 1) c
JOIN (SELECT period_year, SUM(total_revenue) AS rev
      FROM edgar.pipeline.gold_sector_summary
      WHERE period_type = 'annual' GROUP BY 1) s USING (period_year)
WHERE c.period_year IN (2023, 2024)

-- COMMAND ----------

-- VERIFY HEALTHCARE PROPORTIONS
SELECT period_year,
       ROUND(SUM(CASE WHEN sector = 'Healthcare' THEN total_revenue END) / SUM(total_revenue), 4) AS hc_share
FROM edgar.pipeline.gold_sector_summary
WHERE period_type = 'annual' AND period_year IN (2023, 2024)
GROUP BY 1

-- COMMAND ----------

-- OUTLIERS
SELECT company_name, period_year, revenue, net_income, net_margin
FROM edgar.pipeline.gold_company_period
WHERE period_type = 'annual' AND revenue >= 1e9
  AND (net_margin > 0.6 OR net_margin < -0.6)
ORDER BY net_margin DESC

-- COMMAND ----------

-- 11/23 JOB PIPELINE TEST
SELECT * FROM edgar.pipeline.ingestion_log ORDER BY quarter

-- COMMAND ----------

SELECT period_year, COUNT(*) AS annual_reports
FROM edgar.pipeline.gold_company_period
WHERE period_type = 'annual' AND revenue > 0
GROUP BY 1 ORDER BY 1