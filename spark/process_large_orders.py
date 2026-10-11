"""
PySpark batch job for order files too big for a single-node pandas/DuckDB pass.

    # 1) make a large sample (spark.range -> CSV, e.g. 5 million orders over 30 days)
    python spark/process_large_orders.py make-sample --rows 5000000 --out data/large/orders

    # 2) process it
    python spark/process_large_orders.py process \
        --orders data/large/orders --merchants data/raw/merchants \
        --out data/processed --load-duckdb

What `process` does:
  * reads CSV with an explicit schema (no inferSchema scan over a huge file)
  * drops malformed rows and duplicate order_ids
  * broadcast-joins the small merchant table to the large order table
  * builds two aggregates and writes them as Parquet partitioned by order_date:
        merchant_daily   orders, delivered, GMV, cancel rate, avg + p90 delivery time
        city_hourly      demand curve by city and hour of day
  * optionally registers the result in DuckDB (raw.merchant_daily_spark)
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("order_ts", T.TimestampType()),
    T.StructField("status", T.StringType()),
    T.StructField("items_count", T.IntegerType()),
    T.StructField("subtotal", T.DecimalType(12, 2)),
    T.StructField("delivery_fee", T.DecimalType(12, 2)),
    T.StructField("tip", T.DecimalType(12, 2)),
    T.StructField("total_amount", T.DecimalType(12, 2)),
    T.StructField("currency", T.StringType()),
    T.StructField("delivery_minutes", T.IntegerType()),
])


def spark_session(app: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")               # AQE: coalesce small shuffle partitions
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # rerun a day without wiping others
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# --------------------------------------------------------------------------- #
def make_sample(spark: SparkSession, rows: int, days: int, out: str) -> None:
    """Generate a large, realistic-looking orders CSV in parallel with spark.range."""
    r = lambda seed: F.rand(seed)  # noqa: E731
    df = (
        spark.range(rows)
        .withColumn("day", (r(1) * days).cast("int"))
        .withColumn("order_ts", F.expr("timestamp('2026-09-01') + make_interval(0,0,0,day,0,0,0)")
                    + F.expr("make_interval(0,0,0,0, cast(rand(2)*24 as int), cast(rand(3)*60 as int), 0)"))
        .withColumn("order_id", F.format_string("ord_big_%010d", F.col("id")))
        .withColumn("customer_id", F.format_string("cus_%05d", (r(4) * 5000 + 1).cast("int")))
        .withColumn("merchant_id", F.format_string("mer_%04d", (r(5) * 300 + 1).cast("int")))
        .withColumn("u", r(6))
        .withColumn("status", F.when(F.col("u") < 0.90, "delivered")
                    .when(F.col("u") < 0.97, "cancelled").otherwise("refunded"))
        .withColumn("items_count", (r(7) * 6 + 1).cast("int"))
        .withColumn("subtotal", F.round(F.col("items_count") * (r(8) * 14 + 4), 2))
        .withColumn("delivery_fee", F.element_at(F.array(*[F.lit(x) for x in (0.0, 1.99, 2.49, 2.99, 3.99)]),
                                                 (r(9) * 5 + 1).cast("int")))
        .withColumn("tip", F.floor(r(10) * 4).cast("double"))
        .withColumn("total_amount", F.round(F.col("subtotal") + F.col("delivery_fee") + F.col("tip"), 2))
        .withColumn("currency", F.lit("EUR"))
        .withColumn("delivery_minutes", F.when(F.col("status") != "cancelled", (r(11) * 60 + 15).cast("int")))
    )
    # sprinkle in ~0.1% duplicates and a few malformed rows, like real exports
    dupes = df.sample(fraction=0.001, seed=12)
    bad = df.limit(50).withColumn("order_ts", F.lit(None)).withColumn("total_amount", F.lit(-1.0))
    (
        df.unionByName(dupes).unionByName(bad)
        .select([f.name for f in ORDER_SCHEMA.fields])
        .write.mode("overwrite").option("header", True).csv(out)
    )


# --------------------------------------------------------------------------- #
def clean_orders(raw: DataFrame) -> DataFrame:
    valid = raw.where(
        F.col("order_id").isNotNull()
        & F.col("order_ts").isNotNull()
        & (F.col("total_amount") >= 0)
        & F.col("status").isin("delivered", "cancelled", "refunded")
    )
    latest = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc())
    return (
        valid.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("order_hour", F.hour("order_ts"))
    )


def merchant_daily(orders: DataFrame, merchants: DataFrame) -> DataFrame:
    delivered = F.col("status") == "delivered"
    return (
        orders.join(F.broadcast(merchants), "merchant_id", "left")
        .groupBy("order_date", "merchant_id", "merchant_name", "category", "city")
        .agg(
            F.count("*").alias("orders"),
            F.sum(delivered.cast("int")).alias("delivered_orders"),
            F.sum(F.when(delivered, F.col("total_amount")).otherwise(0)).alias("gmv"),
            F.round(F.avg((F.col("status") == "cancelled").cast("int")), 4).alias("cancel_rate"),
            F.round(F.avg("delivery_minutes"), 1).alias("avg_delivery_minutes"),
            F.percentile_approx("delivery_minutes", 0.9).alias("p90_delivery_minutes"),
        )
    )


def city_hourly(orders: DataFrame, merchants: DataFrame) -> DataFrame:
    return (
        orders.join(F.broadcast(merchants.select("merchant_id", "city")), "merchant_id", "left")
        .groupBy("order_date", "city", "order_hour")
        .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("gross_amount"))
    )


def process(spark: SparkSession, orders_path: str, merchants_path: str, out: str, load_duckdb: bool) -> None:
    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").schema(ORDER_SCHEMA).csv(orders_path)
    merchants = (
        spark.read.option("header", True).csv(merchants_path)
        .select("merchant_id", "merchant_name", "category", "city")
        .dropDuplicates(["merchant_id"])
    )
    orders = clean_orders(raw).cache()

    raw_n, clean_n = raw.count(), orders.count()
    print(f"read {raw_n:,} rows, kept {clean_n:,} after cleaning ({raw_n - clean_n:,} dropped)")

    md = merchant_daily(orders, merchants)
    md.write.mode("overwrite").partitionBy("order_date").parquet(f"{out}/merchant_daily")
    city_hourly(orders, merchants).write.mode("overwrite").partitionBy("order_date").parquet(f"{out}/city_hourly")
    print(f"wrote {out}/merchant_daily and {out}/city_hourly")

    if load_duckdb:
        import duckdb

        from ingestion.config import Settings

        with duckdb.connect(str(Settings().duckdb_path)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS raw")
            con.execute(
                "CREATE OR REPLACE TABLE raw.merchant_daily_spark AS "
                f"SELECT * FROM read_parquet('{out}/merchant_daily/**/*.parquet', hive_partitioning = true)"
            )
            n = con.execute("SELECT count(*) FROM raw.merchant_daily_spark").fetchone()[0]
        print(f"registered raw.merchant_daily_spark in DuckDB ({n:,} rows)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("make-sample")
    s.add_argument("--rows", type=int, default=5_000_000)
    s.add_argument("--days", type=int, default=30)
    s.add_argument("--out", default="data/large/orders")
    r = sub.add_parser("process")
    r.add_argument("--orders", default="data/large/orders")
    r.add_argument("--merchants", default="data/raw/merchants")
    r.add_argument("--out", default="data/processed")
    r.add_argument("--load-duckdb", action="store_true")
    a = p.parse_args()

    spark = spark_session(f"delivery-{a.cmd}")
    try:
        if a.cmd == "make-sample":
            make_sample(spark, a.rows, a.days, a.out)
            print(f"wrote ~{a.rows:,} orders to {a.out}")
        else:
            process(spark, a.orders, a.merchants, str(Path(a.out).resolve()), a.load_duckdb)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
