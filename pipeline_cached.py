import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
import time


def main() -> None:
    start_time = time.time()
    input_csv = os.environ.get("INPUT_CSV", "/data/sample/yellow_tripdata_sample.csv")
    output_dir = os.environ.get("OUTPUT_DIR", "/artifacts/sample")

    spark = (
        SparkSession.builder
        .appName("nyc-tlc-arrival-instability-cached")
        .getOrCreate()
    )

    trips_df = None
    try:
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(input_csv)
        )

        # 0. Фільтрація валідних поїздок та підготовка ключів
        trips_df = (
            raw_df
            .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
            .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
            .withColumn(
                "duration_sec",
                F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"),
            )
            .withColumn("hour_of_day", F.hour("pickup_ts"))
            .filter(
                F.col("PULocationID").isNotNull()
                & F.col("DOLocationID").isNotNull()
                & F.col("hour_of_day").isNotNull()
                & (F.col("duration_sec") > 30)
                & (F.col("duration_sec") < 14400)
                & (F.col("trip_distance") > 0)
            )
            .select(
                F.col("PULocationID").cast("int").alias("pu_id"),
                F.col("DOLocationID").cast("int").alias("do_id"),
                F.col("hour_of_day").cast("int").alias("hour_of_day"),
                F.col("duration_sec").cast("double").alias("duration_sec"),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # 1. Еталонна тривалість по маршруту та годині доби (Shuffle 1)
        baseline_df = (
            trips_df
            .groupBy("pu_id", "do_id", "hour_of_day")
            .agg(
                F.expr("percentile_approx(duration_sec, 0.5)").alias("median_dur_sec"),
                F.count("*").alias("route_hour_trips"),
            )
            .filter(F.col("route_hour_trips") >= 3)
        )

        # 2. Зіставлення поїздки з нормою та розрахунок відхилення
        joined_df = (
            trips_df.alias("t")
            .join(
                baseline_df.alias("b"),
                on=["pu_id", "do_id", "hour_of_day"],
                how="inner",
            )
            .withColumn(
                "ratio",
                F.col("t.duration_sec") / F.col("b.median_dur_sec"),
            )
            .withColumn(
                "is_delayed",
                F.when(F.col("ratio") >= 1.5, F.lit(1.0)).otherwise(F.lit(0.0)),
            )
            .select(
                F.col("t.do_id").alias("do_id"),
                F.col("is_delayed"),
                F.col("ratio"),
            )
        )

        # 3. Агрегація по зоні прибуття DOLocationID (Shuffle 2)
        result_df = (
            joined_df
            .groupBy("do_id")
            .agg(
                F.count("*").alias("trips_count"),
                F.avg("is_delayed").alias("delay_rate"),
                F.avg("ratio").alias("avg_ratio"),
            )
            .orderBy(F.desc("delay_rate"), F.desc("trips_count"))
        )

        result_df.explain(mode="formatted")
        result_df.show(20, truncate=False)

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        (
            result_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(output_dir)
        )
    finally:
        if trips_df is not None:
            try:
                trips_df.unpersist()
            except Exception:
                pass
        spark.stop()
        total_time = time.time() - start_time
        print(f"TIME: {total_time:.2f}")


if __name__ == "__main__":
    main()

