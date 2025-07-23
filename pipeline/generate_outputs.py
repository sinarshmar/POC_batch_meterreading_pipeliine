from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, countDistinct, to_date, avg
from pyspark.sql.types import StringType


def generate_final_tables(spark, staging: dict, output_path: str = "data_lake/presentation"):
    readings = staging["readings"]
    agreements = staging["agreements"]

    # Join readings with agreements
    joined = readings.join(agreements, "meterpoint_id")

    # This aggregation is used for BOTH:
    # 1. The required final analytics table:
    #    - One row per half hour
    #    - Count of distinct meterpoints
    #    - Sum of consumption in kWh
    # 2. The gold table: "Aggregate consumption per half hour"
    #    Output is stored in: gold_aggregate_per_half_hour
    print("Generating daily aggregration data")
    daily_agg = readings.groupBy("interval_start").agg(
        countDistinct("meterpoint_id").alias("meterpoint_count"),
        sum("consumption_delta").alias("total_kWh")
    )
    try:
        daily_agg.write.partitionBy("interval_start").format("delta").mode("overwrite").save(
            f"{output_path}/gold_aggregate_per_half_hour")
    except Exception as e:
        print(f"Error writing gold_aggregate_per_half_hour: {e}")


    # Gold table 2: average consumption per product per half hour
    print("Generating Aggregate consumption per half hour")
    avg_by_product = joined.groupBy("interval_start", "display_name").agg(
        avg("consumption_delta").alias("avg_kWh")
    )
    avg_by_product = avg_by_product.withColumn("display_name", col("display_name").cast("string"))
    try:
        avg_by_product.write.partitionBy("interval_start").format("delta").option("overwriteSchema", "true").mode(
            "overwrite").save(f"{output_path}/gold_avg_half_hour_by_product")
    except Exception as e:
        print(f"Error writing gold_avg_half_hour_by_product: {e}")

    # Active agreements snapshot 1st January 2021 (an active agreement has a valid_from less than the 1st January 2021 and a null valid_to or a valid_to greater than 1st January 2021).
    print("Generating active agreements table")
    active = agreements.filter(
        (col("agreement_valid_from") < "2021-01-01") &
        ((col("agreement_valid_to").isNull()) | (col("agreement_valid_to") > "2021-01-01"))
    ).select("agreement_id", "meterpoint_id", "display_name", "is_variable")
    active = active.withColumn("display_name", col("display_name").cast("string"))
    active = active.withColumn("meterpoint_id", col("meterpoint_id").cast(StringType()))
    try:
        active.write.format("delta").mode("overwrite").save(f"{output_path}/active_agreements")
    except Exception as e:
        print(f"Error writing active_agreements: {e}")

    # Total consumption per product per day (MERGE into Delta table)
    print("Generating consumption_per_product_per_day")
    prod_day = joined.withColumn("date", to_date("interval_start")) \
        .groupBy("display_name", "date") \
        .agg(countDistinct("meterpoint_id").alias("meterpoint_count"),
             sum("consumption_delta").alias("total_kWh"))

    dest_path = f"{output_path}/consumption_per_product_per_day"

    try:
        if DeltaTable.isDeltaTable(spark, dest_path):
            delta_tbl = DeltaTable.forPath(spark, dest_path)
            delta_tbl.alias("target").merge(
                prod_day.alias("source"),
                "target.display_name = source.display_name AND target.date = source.date"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            prod_day.write.partitionBy("date").format("delta").save(dest_path)
    except Exception as e:
        print(f"Error writing consumption_per_product_per_day: {e}")
