import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--watermark-from", required=True)
parser.add_argument("--watermark-to", required=True)
parser.add_argument("--target-path", default="hdfs:///data/raw/payments/settlements")
args = parser.parse_args()

spark = SparkSession.builder.appName("extract_legacy_settlements").getOrCreate()

# Settlements = cash the processor actually paid out, net of processor fees,
# usually 1-3 days after capture. Finance reconciles these to captured payments,
# so this history must migrate exactly. Rows can be updated after the fact
# (fee adjustments, chargebacks), which is why we extract on updated_at.
query = f"""
    (SELECT settlement_id, attempt_id, processor, settled_amount, currency,
            fee_amount, settlement_status, updated_at
       FROM payments.settlements
      WHERE updated_at >= TIMESTAMP '{args.watermark_from}'
        AND updated_at <  TIMESTAMP '{args.watermark_to}') src
"""

df = (spark.read.format("jdbc")
      .option("url", os.environ["LEGACY_JDBC_URL"])
      .option("dbtable", query)
      .option("user", os.environ["LEGACY_USER"])           # injected from secrets vault
      .option("password", os.environ["LEGACY_PASSWORD"])
      .option("fetchsize", 50000)
      .option("numPartitions", 16)
      .option("partitionColumn", "settlement_id")
      .option("lowerBound", 0).option("upperBound", 10**10)
      .load())

(df.withColumn("dt", F.to_date("updated_at"))
   .withColumn("_extracted_at", F.current_timestamp())
   .repartition("dt")
   .write.mode("overwrite")                 # dynamic partition overwrite = idempotent reruns
   .option("partitionOverwriteMode", "dynamic")
   .partitionBy("dt")
   .parquet(args.target_path))
