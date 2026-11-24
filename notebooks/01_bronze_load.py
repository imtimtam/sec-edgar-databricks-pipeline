# Databricks notebook source
import os, re, zipfile, shutil
from datetime import datetime
from pyspark.sql import functions as F

CATALOG, SCHEMA = "edgar", "pipeline"
LANDING   = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
ZIPS      = f"{LANDING}/zips"
EXTRACTED = f"{LANDING}/extracted"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

REPROCESS = []     # e.g. ["2023q1"] to force a reload of a quarter
CLEANUP   = True

# COMMAND ----------

zip_files = sorted(f for f in os.listdir(ZIPS) if re.fullmatch(r"\d{4}q[1-4]\.zip", f))
all_quarters = [f[:-4] for f in zip_files]

if spark.catalog.tableExists(T("ingestion_log")):
    done = {r.quarter for r in spark.table(T("ingestion_log")).select("quarter").collect()}
else:
    done = set()

todo = [q for q in all_quarters if q not in done or q in REPROCESS]
print("To process:", todo)

# COMMAND ----------

def load_table(name, quarter, folder):
    src = f"{folder}/{name}.txt"
    df = (spark.read
          .option("header", True)
          .option("sep", "\t")
          .option("quote", "")        # SEC files aren't quoted; avoids stray " in names
          .csv(src))                  # every column stays a string in bronze

    df = (df.withColumn("_source_quarter", F.lit(quarter))
            .withColumn("_source_file", F.lit(f"{quarter}.zip"))
            .withColumn("_loaded_at", F.current_timestamp()))

    table = T(f"bronze_{name}")
    writer = df.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(table):
        writer = writer.option("replaceWhere", f"_source_quarter = '{quarter}'")
    writer.saveAsTable(table)

    # Check: rows written must equal lines in the file minus the header
    expected = spark.read.text(src).count() - 1
    written = spark.table(table).where(F.col("_source_quarter") == quarter).count()
    if written != expected:
        raise ValueError(f"{quarter} {name}: wrote {written}, file has {expected}")
    return written

# COMMAND ----------

for q in todo:
    print(f"--- {q} ---")
    out_dir = f"{EXTRACTED}/{q}"
    os.makedirs(out_dir, exist_ok=True)

    with zipfile.ZipFile(f"{ZIPS}/{q}.zip") as z:
        for name in ("sub", "num"):
            z.extract(f"{name}.txt", out_dir)

    sub_rows = load_table("sub", q, out_dir)
    num_rows = load_table("num", q, out_dir)

    if spark.catalog.tableExists(T("ingestion_log")):
        spark.sql(f"DELETE FROM {T('ingestion_log')} WHERE quarter = '{q}'")
    (spark.createDataFrame(
        [(q, sub_rows, num_rows, datetime.now())],
        "quarter string, sub_rows long, num_rows long, loaded_at timestamp")
     .write.mode("append").saveAsTable(T("ingestion_log")))

    if CLEANUP:
        shutil.rmtree(out_dir)
    print(f"{q}: sub={sub_rows:,}  num={num_rows:,}")

# Tidy up the empty extracted/ folder
if CLEANUP and os.path.isdir(EXTRACTED) and not os.listdir(EXTRACTED):
    os.rmdir(EXTRACTED)

# COMMAND ----------

display(spark.table(T("ingestion_log")).orderBy("quarter"))
spark.sql(f"""SELECT _source_quarter, COUNT(*) AS rows
              FROM {T('bronze_num')} GROUP BY 1 ORDER BY 1""").show()