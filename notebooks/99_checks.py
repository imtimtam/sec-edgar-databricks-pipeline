# Databricks notebook source
def scalar(sql):
    return spark.sql(sql).first()[0]

checks = {
    "duplicate filings": scalar("""
        SELECT COUNT(*) - COUNT(DISTINCT adsh) FROM edgar.pipeline.silver_filings"""),
    "duplicate facts": scalar("""
        SELECT COUNT(*) FROM (
          SELECT adsh, tag, ddate, qtrs FROM edgar.pipeline.silver_facts
          GROUP BY ALL HAVING COUNT(*) > 1)"""),
    "quarters in bronze but not silver": scalar("""
        SELECT COUNT(DISTINCT _source_quarter) FROM edgar.pipeline.bronze_sub
        WHERE _source_quarter NOT IN (
          SELECT DISTINCT _source_quarter FROM edgar.pipeline.silver_filings)"""),
    "duplicate company-periods": scalar("""
        SELECT COUNT(*) FROM (
        SELECT cik, period_end, period_type FROM edgar.pipeline.gold_company_period
        GROUP BY ALL HAVING COUNT(*) > 1)"""),
}

for name, bad in checks.items():
    print(f"{'OK  ' if bad == 0 else 'FAIL'} {name}: {bad}")

assert all(v == 0 for v in checks.values()), "Data checks failed, see output above"