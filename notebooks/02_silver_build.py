# Databricks notebook source
from pyspark.sql import functions as F

CATALOG, SCHEMA = "edgar", "pipeline"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
REPROCESS = []   # e.g. ["2025q3"] to rebuild a quarter

REVENUE_TAGS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "RevenuesNetOfInterestExpense",
]
OTHER_TAGS = ["NetIncomeLoss", "ProfitLoss", "Assets", "Liabilities", "StockholdersEquity"]
FACT_TAGS = REVENUE_TAGS + OTHER_TAGS

# COMMAND ----------

sector_rows = [
    (2833, "Healthcare", "Pharma & Biotech"), (2834, "Healthcare", "Pharma & Biotech"),
    (2835, "Healthcare", "Pharma & Biotech"), (2836, "Healthcare", "Pharma & Biotech"),
    (3841, "Healthcare", "Medical Devices"),  (3842, "Healthcare", "Medical Devices"),
    (3843, "Healthcare", "Medical Devices"),  (3844, "Healthcare", "Medical Devices"),
    (3845, "Healthcare", "Medical Devices"),  (3851, "Healthcare", "Medical Devices"),
    (6324, "Healthcare", "Health Insurers"),
    (8000, "Healthcare", "Providers & Services"), (8011, "Healthcare", "Providers & Services"),
    (8050, "Healthcare", "Providers & Services"), (8060, "Healthcare", "Providers & Services"),
    (8062, "Healthcare", "Providers & Services"), (8071, "Healthcare", "Providers & Services"),
    (8082, "Healthcare", "Providers & Services"), (8090, "Healthcare", "Providers & Services"),
    (8093, "Healthcare", "Providers & Services"),
    (5047, "Healthcare", "Distribution & Retail"), (5122, "Healthcare", "Distribution & Retail"),
    (5912, "Healthcare", "Distribution & Retail"),
]
(spark.createDataFrame(sector_rows, "sic int, sector string, subsector string")
      .write.mode("overwrite").saveAsTable(T("ref_sic_sector")))

# COMMAND ----------

def write_quarter(df, table, q):
    w = df.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(table):
        w = w.option("replaceWhere", f"_source_quarter = '{q}'")
    w.saveAsTable(table)

bronze_q = {r[0] for r in spark.table(T("bronze_sub")).select("_source_quarter").distinct().collect()}
done = set()
if spark.catalog.tableExists(T("silver_filings")):
    done = {r[0] for r in spark.table(T("silver_filings")).select("_source_quarter").distinct().collect()}

todo = sorted(q for q in bronze_q if q not in done or q in REPROCESS)
print("To process:", todo)

# Test run: uncomment to test one quarter before building all 12
# todo = todo[:1]

# COMMAND ----------

ref = spark.table(T("ref_sic_sector"))

for q in todo:
    filings = (spark.table(T("bronze_sub"))
        .where((F.col("_source_quarter") == q)
               & F.col("form").isin("10-K", "10-Q", "10-K/A", "10-Q/A"))
        .select(
            "adsh",
            F.col("cik").cast("long").alias("cik"),
            F.col("name").alias("company_name"),
            F.col("sic").cast("int").alias("sic"),
            "form",
            F.col("form").endswith("/A").alias("is_amendment"),
            F.to_date("period", "yyyyMMdd").alias("period_end"),
            F.col("fy").cast("int").alias("fy"),
            "fp",
            F.to_date("filed", "yyyyMMdd").alias("filed"),
            "_source_quarter")
        .join(ref, "sic", "left")
        .withColumn("sector", F.coalesce("sector", F.lit("Other")))
        .withColumn("subsector", F.coalesce("subsector", F.lit("Other"))))

    facts = (spark.table(T("bronze_num"))
        .where((F.col("_source_quarter") == q)
               & F.col("segments").isNull() & F.col("coreg").isNull()
               & (F.col("uom") == "USD")
               & F.col("version").startswith("us-gaap")
               & F.col("tag").isin(FACT_TAGS)
               & F.col("value").isNotNull())
        .select("adsh", "tag",
                F.to_date("ddate", "yyyyMMdd").alias("ddate"),
                F.col("qtrs").cast("int").alias("qtrs"),
                F.col("value").cast("decimal(28,4)").alias("value"),
                "_source_quarter")
        .join(filings.select("adsh"), "adsh", "left_semi"))

    # Facts first: silver_filings is the "done" marker, so a failure
    # between the two writes means the quarter is retried next run
    write_quarter(facts, T("silver_facts"), q)
    write_quarter(filings, T("silver_filings"), q)
    print(q, "done")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f._source_quarter, COUNT(DISTINCT f.adsh) AS filings, COUNT(*) AS facts
# MAGIC FROM edgar.pipeline.silver_facts f
# MAGIC GROUP BY 1 ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT company_name, sic, sector, subsector
# MAGIC FROM edgar.pipeline.silver_filings
# MAGIC WHERE sector = 'Healthcare'
# MAGIC ORDER BY subsector, company_name