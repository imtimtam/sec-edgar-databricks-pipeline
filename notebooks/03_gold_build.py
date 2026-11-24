# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG, SCHEMA = "edgar", "pipeline"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

# Must match silver's list; order = priority, first match wins
REVENUE_TAGS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "RevenuesNetOfInterestExpense",
]

# COMMAND ----------

filings = (spark.table(T("silver_filings"))
    .withColumn("period_type",
                F.when(F.col("form").startswith("10-K"), "annual").otherwise("quarterly"))
    .withColumn("expected_qtrs",
                F.when(F.col("period_type") == "annual", 4).otherwise(1)))

facts = (spark.table(T("silver_facts"))
    .join(filings.select("adsh", "period_end", "expected_qtrs"), "adsh")
    .where(F.col("ddate") == F.col("period_end"))       # current period, not prior-year comparatives
    .where(
        ((F.col("qtrs") == 0) & F.col("tag").isin("Assets", "Liabilities", "StockholdersEquity"))
        | ((F.col("qtrs") == F.col("expected_qtrs"))
           & F.col("tag").isin(REVENUE_TAGS + ["NetIncomeLoss", "ProfitLoss"]))))

# Revenue: best-ranked tag per filing
rank_expr = F.expr("array_position(array(" + ",".join(f"'{t}'" for t in REVENUE_TAGS) + "), tag)")
rev = (facts.where(F.col("tag").isin(REVENUE_TAGS))
       .withColumn("r", rank_expr)
       .withColumn("rn", F.row_number().over(Window.partitionBy("adsh").orderBy("r")))
       .where("rn = 1")
       .select("adsh", F.col("value").alias("revenue")))

# Everything else: one value per tag per filing (silver check guarantees uniqueness)
def pick(tag):
    return F.max(F.when(F.col("tag") == tag, F.col("value")))

other = (facts.groupBy("adsh").agg(
            pick("NetIncomeLoss").alias("ni"),
            pick("ProfitLoss").alias("pl"),
            pick("Assets").alias("assets"),
            pick("Liabilities").alias("liabilities"),
            pick("StockholdersEquity").alias("equity"))
         .withColumn("net_income", F.coalesce("ni", "pl"))   # prefer NetIncomeLoss
         .drop("ni", "pl"))

wide = (other.join(rev, "adsh", "left")
        .join(filings.select("adsh", "cik", "company_name", "sic", "sector", "subsector",
                             "period_type", "period_end", "fy", "fp", "filed", "form"), "adsh"))

w = Window.partitionBy("cik", "period_end", "period_type") \
          .orderBy(F.col("filed").desc(), F.col("adsh").desc())

company_period = (wide
    .withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
    .withColumn("period_year", F.year("period_end"))
    .withColumn("period_quarter",
                F.when(F.col("period_type") == "quarterly", F.quarter("period_end")))
    .withColumn("net_margin",
                F.when(F.col("revenue") > 0, F.round(F.col("net_income") / F.col("revenue"), 4))))

(company_period.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(T("gold_company_period")))

# COMMAND ----------

(spark.table(T("gold_company_period"))
    .where((F.col("revenue") > 0) & F.col("net_income").isNotNull())
    .groupBy("sector", "subsector", "period_type", "period_year", "period_quarter")
    .agg(F.countDistinct("cik").alias("companies"),
         F.sum("revenue").alias("total_revenue"),
         F.sum("net_income").alias("total_net_income"))
    .withColumn("net_margin",
                F.when(F.col("total_revenue") > 0,
                       F.round(F.col("total_net_income") / F.col("total_revenue"), 4)))
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(T("gold_sector_summary")))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TOP COMPANIES BY REVENUE
# MAGIC SELECT company_name, sector, revenue, net_income, net_margin
# MAGIC FROM edgar.pipeline.gold_company_period
# MAGIC WHERE period_type = 'annual' AND period_year = 2024
# MAGIC ORDER BY revenue DESC LIMIT 15