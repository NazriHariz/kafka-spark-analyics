from datetime import datetime, timedelta
from functools import reduce

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *


NDJSON_FILES_DIR = "data"
OUTPUT_DIR = "output"

spark = (
    SparkSession.builder
    .appName("StockTicksStructured")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

# Define schema
schema = StructType(
    [StructField("transaction_id", StringType()),
     StructField("customer_id", StringType()),
     StructField("merchant", StringType()),
     StructField("amount", FloatType()),
     StructField("currency", StringType()),
     StructField("transaction_type", StringType()),
     StructField("is_international", BooleanType()),
     StructField("transaction_ts", StringType()),
     StructField("status", StringType())]
)

df = (spark.read
      .schema(schema)
#      .option("multiline", 'true')
      .json(f"{NDJSON_FILES_DIR}/*.ndjson"))

rdd = df.rdd

pdfs = []


# Total transactions amount per customer
rdd1 = rdd.map(lambda row: (row.customer_id, row.amount)) \
   .reduceByKey(lambda a, b: a + b) \
   .sortByKey()

df1 = rdd1.toDF(["customer_id", "total_transaction_amount"])
# df1.show()
# df1.write.csv(f"{OUTPUT_DIR}/total_transaction_amount_per_customer",
#               header=True,
#               mode="overwrite")


# Average transactions amount per customer
rdd2 = rdd.map(lambda row: (row.customer_id, row.amount)) \
   .groupByKey() \
   .mapValues(lambda amounts: sum(list(amounts)) / len(list(amounts)) if len(list(amounts)) > 0 else None) \
   .sortByKey()

df2 = rdd2.toDF(["customer_id", "average_transaction_amount"])
# df2.show()
# df2.write.csv(f"{OUTPUT_DIR}/average_transaction_amount_per_customer",
#               header=True,
#               mode="overwrite")


# Maximum single transaction per customer
rdd3 = rdd.map(lambda row: (row.customer_id, row.amount)) \
   .reduceByKey(lambda a, b: a if a > b else b) \
   .sortByKey()

df3 = rdd3.toDF(["customer_id", "maximum_transaction_amount"])
# df3.show()
# df3.write.csv(f"{OUTPUT_DIR}/maximum_transaction_amount_per_customer",
#               header=True,
#               mode="overwrite")


# Number of successful vs failed transactions per customer
def helper(records) -> tuple[int, int]:
    # records is an iterable of tuples

    success = 0
    failed = 0
    for _, status in records:
        if status == "SUCCESS":
            success += 1
        else:
            failed += 1

    return (success, failed)


rdd4 = rdd.map(lambda row: (row.customer_id, (row.amount, row.status))) \
   .groupByKey() \
   .mapValues(helper) \
   .sortByKey() \
   .map(lambda kv: (kv[0], kv[1][0], kv[1][1]))

df4 = rdd4.toDF(["customer_id", "successful transactions", "failed transactions"])
# df4.show()
# df4.write.csv(f"{OUTPUT_DIR}/successful_and_failed_transaction_counts_per_customer",
#               header=True,
#               mode="overwrite")

customer_df = df1.join(df2, on='customer_id', how='outer') \
                 .join(df3, on='customer_id', how='outer') \
                 .join(df4, on='customer_id', how='outer')
customer_df.show()
customer_df.write.csv(f"{OUTPUT_DIR}/metrics_per_customer",
                      header=True,
                      mode="overwrite")

# Total transaction counts per merchant
rdd5 = rdd.map(lambda row: (row.merchant, 1)) \
   .reduceByKey(lambda a, b: a + b) \
   .sortByKey()

df5 = rdd5.toDF(["merchant", "total_transaction_counts"])
# df5.show()
# df5.write.csv(f"{OUTPUT_DIR}/total_transaction_counts_per_merchant",
#               header=True,
#               mode="overwrite")


# Total revenues per merchant( success only)
rdd6 = rdd.filter(lambda row: row.status=='SUCCESS') \
          .map(lambda row: (row.merchant, row.amount)) \
          .reduceByKey(lambda a, b: a + b) \
          .sortByKey()

df6 = rdd6.toDF(["merchant", "revenues"])
# df6.show()
# df6.write.csv(f"{OUTPUT_DIR}/total_transactions_count_per_merchant",
#               header=True,
#               mode="overwrite")

# Average transaction amount per merchant
rdd7 = rdd.map(lambda row: (row.merchant, row.amount)) \
          .groupByKey() \
          .mapValues(lambda amounts: sum(list(amounts)) / len(list(amounts)) if len(list(amounts)) > 0 else None) \
          .sortByKey()

df7 = rdd7.toDF(["merchant", "average_transaction_amounts_per_merchant"])
# df7.show()
# df7.write.csv(f"{OUTPUT_DIR}/average_transactions_amounts_per_merchant",
#               header=True,
#               mode="overwrite")

# Top 5 merchants by transaction volume
rdd8 = rdd.map(lambda row: (row.merchant, 1)) \
   .reduceByKey(lambda a, b: a + b) \
   .sortBy(lambda x: x[1], ascending=False) \
   .zipWithIndex() \
   .map(lambda x: (x[0][0], x[1] + 1))

df8 = rdd8.toDF(["merchant", "rank_by_transaction_volumes"]).limit(5)
df8.show()
# df8.write.csv(f"{OUTPUT_DIR}/top_5_merchants_by_transaction_volumes",
#               header=True,
#               mode="overwrite")

merchant_df = df5.join(df6, on='merchant', how='outer') \
                 .join(df7, on='merchant', how='outer') \
                 .join(df8, on='merchant', how='outer')
merchant_df.show()
merchant_df.write.csv(f"{OUTPUT_DIR}/metrics_per_merchant",
              header=True,
              mode="overwrite")

# Transaction counts by hour of day
rdd9 = rdd.map(lambda row: (datetime.strptime(row.transaction_ts, "%Y-%m-%dT%H:%M:%S").hour, 1)) \
          .reduceByKey(lambda a, b: a + b) \
          .sortByKey()

df9 = rdd9.toDF(["hour", "transaction_counts"])
df9.show()
df9.write.csv(f"{OUTPUT_DIR}/transaction_counts_by_hour_of_a_day",
              header=True,
              mode="overwrite")


# Transaction counts by day
rdd10 = rdd.map(lambda row: (datetime.strptime(row.transaction_ts, "%Y-%m-%dT%H:%M:%S").day, 1)) \
          .reduceByKey(lambda a, b: a + b) \
          .sortByKey()

df10 = rdd10.toDF(["day", "transaction_counts"])
df10.show()
df10.write.csv(f"{OUTPUT_DIR}/transactions_counts_per_day",
              header=True,
              mode="overwrite")


# Peak transaction hour across all merchants
rdd11 = rdd.map(lambda row: ((row.merchant, datetime.strptime(row.transaction_ts, "%Y-%m-%dT%H:%M:%S").hour),
                             1)) \
          .reduceByKey(lambda a, b: a + b) \
          .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
          .groupByKey() \
          .mapValues(lambda records: sorted(records, key=lambda record: record[1], reverse=True)) \
          .mapValues(lambda records: records[0]) \
          .map(lambda x: (x[0], x[1][0], x[1][1]))

df11 = rdd11.toDF(["merchant", "peak hour", "transaction_counts"])
df11.show()
df11.write.csv(f"{OUTPUT_DIR}/peak_transactions_hours_per_merchant",
              header=True,
              mode="overwrite")

# High-Value Transactions
rdd12 = rdd.filter(lambda row: row.amount > 1000)
df12 = spark.createDataFrame(rdd12, schema=schema)
df12.show()
df12.write.csv(f"{OUTPUT_DIR}/high_value_transactions",
              header=True,
              mode="overwrite")

# International Risk Transactions
rdd13 = rdd.filter(lambda row: row.is_international and row.amount > 500)
df13 = spark.createDataFrame(rdd13, schema=schema)
df13.show()
df13.write.csv(f"{OUTPUT_DIR}/international_risk_transactions",
              header=True,
              mode="overwrite")

# Rapid transaction activity
def sort_timestamps(timestamps):
    timestamps = list(timestamps)
    timestamps = list(map(lambda ts: datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S"), timestamps))
    timestamps.sort()

    left = 0
    right = 5
    intervals = []
    while left < right < len(timestamps):
        while right < len(timestamps) and timestamps[right] - timestamps[left] <= timedelta(minutes=10):
            # expand the window by moving right rightwards
            right += 1
        # Case 1: never enters the loop
        if right - left == 5:
            left += 1
            right += 1
        # Case 2: enters the loop at least once
        else:
            intervals.append((left, right - 1))
            left = right
            right = left + 5

    results = []
    for start, end in intervals:
        results.append(timestamps[start : end + 1])
    return results


rdd14 = rdd.map(lambda row: (row.customer_id, row.transaction_ts)) \
           .groupByKey() \
           .mapValues(sort_timestamps) \
           .flatMap(lambda kv: [(kv[0], x) for sub in kv[1] for x in sub])

schema_df14 = StructType([
    StructField("customer_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])
df14 = spark.createDataFrame(rdd14, schema=schema_df14)
df14.show()
df14.write.csv(f"{OUTPUT_DIR}/flagged_customer_with_rapid_transactions",
              header=True,
              mode="overwrite")


# suspicious failure patterns
def calc_failure_rate(records):
    failed = 0
    for record in records:
        if record == "FAILED":
            failed += 1
    return round((failed / len(records)) * 100, 2)


rdd15 = rdd.map(lambda row: (row.customer_id, row.status)) \
           .groupByKey() \
           .filter(lambda kv: len(kv[1]) >= 3) \
           .mapValues(calc_failure_rate) \
           .filter(lambda kv: kv[1] > 0.4) \
           .sortByKey() \
           .map(lambda kv: (kv[0],))

schema_df15 = StructType([
    StructField("customer_id", StringType(), True)
])
df15 = spark.createDataFrame(rdd15, schema=schema_df15)
df15.show()
df15.write.csv(f"{OUTPUT_DIR}/suspicious_customers",
              header=True,
              mode="overwrite")


# Identify records with missing or null critical fields:
expected_colns = ["transaction_id", "customer_id", "amount", "transaction_ts"]
missing_rdd = rdd.filter(
    lambda row: any(getattr(row, c) is None for c in expected_colns)
)
df_missing = spark.createDataFrame(missing_rdd, schema=schema)
df_missing.show()
df_missing.write.csv(f"{OUTPUT_DIR}/missingOrNull_fields_transactions",
              header=True,
              mode="overwrite")


# Duplicate detection
duped_rdd = rdd.map(lambda row: (row.transaction_id, row)) \
               .groupByKey() \
               .filter(lambda kv: len(kv[1]) > 1) \
               .flatMap(lambda kv: kv[1])
df_duped = spark.createDataFrame(duped_rdd, schema=schema)
df_duped.show()
df_duped.write.csv(f"{OUTPUT_DIR}/duplicated_transactions",
              header=True,
              mode="overwrite")

# Invalid Value Checks
unsupported_transaction_types = ['ALIPAY']
rdd_invalid = rdd.filter(lambda row: row.amount <= 0 or row.transaction_type in unsupported_transaction_types)
df_invalid = spark.createDataFrame(rdd_invalid, schema=schema)
df_invalid.show()
df_invalid.write.csv(f"{OUTPUT_DIR}/invalid_transactions",
              header=True,
              mode="overwrite")

# Failure rate analysis
overall_failure_rate = rdd.filter(lambda row: row.status == 'FAILED').count() / rdd.count()
print(f"overall failure rate = {overall_failure_rate * 100}%")

# Failure rate per merchant
rdd_failure_per_merchant = rdd.map(lambda row: (row.merchant, row.status)) \
   .groupByKey() \
   .mapValues(calc_failure_rate) \
   .sortByKey()
df_failure_per_merchant = rdd_failure_per_merchant.toDF(["merchant", "failure_rate (%)"])
df_failure_per_merchant.show()
df_failure_per_merchant.write.csv(f"{OUTPUT_DIR}/failed_transaction_rate_per_merchant",
              header=True,
              mode="overwrite")

