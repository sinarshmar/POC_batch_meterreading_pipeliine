from pyspark.sql import SparkSession, DataFrame
import json
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from typing import List
from glob import glob
from pyspark.sql.functions import to_date

def load_readings(spark: SparkSession, input_dir: str) -> DataFrame:
    """
    Load all JSON readings from a directory where each file has a custom structure:
    {
        "columns": [...],
        "data": [...]
    }

    Adds a 'source_file' column for metadata and deduplicates readings by
    ['interval_start', 'meterpoint_id'].

    Note: In cloud implementation, we will filter to only the most recent files (e.g., last N hours).
    """
    json_files = glob(os.path.join(input_dir, "*.json"))

    dataframes = []
    print("Loading readings data from", input_dir)
    for file in json_files:
        try:
            with open(file, "r") as f:
                raw = json.load(f)
        except Exception as e:
            print(f"Failed to read JSON from {file}: {e}")
            continue

        try:
            df = spark.createDataFrame(raw["data"], schema=raw["columns"])
        except Exception as e:
            print(f"Failed to create DataFrame for {file}: {e}")
            continue

        df = df.withColumn("source_file", lit(os.path.basename(file)))
        dataframes.append(df)

    if not dataframes:
        return spark.createDataFrame([], schema="interval_start STRING, consumption_delta DOUBLE, meterpoint_id STRING, source_file STRING")

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df)

    # Drop duplicates based on interval + meter ID
    combined_df = combined_df.dropDuplicates(["interval_start", "meterpoint_id"])

    return combined_df


def load_db(spark: SparkSession, db_path: str, jar_path: str):
    """Load tables from SQLite."""
    jdbc_url = f"jdbc:sqlite:{db_path}"
    props = {"driver": "org.sqlite.JDBC"}

    print("Loading SQL lite db data")
    # Read all columns as strings to prevent JDBC driver from doing its own parsing
    try:
        agreement_raw = spark.read.format("jdbc").options(
            url=jdbc_url,
            dbtable="agreement",
            driver="org.sqlite.JDBC"
        ).option("customSchema",
                 "agreement_id INTEGER, agreement_valid_from STRING, agreement_valid_to STRING, meter_point_id STRING, product_id STRING, account_id STRING").load()
    except Exception as e:
        print(f"Failed to load agreement table: {e}")

    # Fix the broken date formats manually
    agreement = agreement_raw \
        .withColumn("agreement_valid_from", to_date("agreement_valid_from", "yyyy-MM-dd")) \
        .withColumn("agreement_valid_to", to_date("agreement_valid_to", "yyyy-MM-dd"))

    try:
        product = spark.read.format("jdbc").options(
            url=jdbc_url,
            dbtable="product",
            driver="org.sqlite.JDBC"
        ).option("customSchema",
                 "display_name STRING, is_variable INTEGER, ID INTEGER, product_id STRING").load()
    except Exception as e:
        print(f"Failed to load product table: {e}")

    try:
        meterpoint = spark.read.format("jdbc").options(
            url=jdbc_url, dbtable="meterpoint", **props).load()
    except Exception as e:
        print(f"Failed to load meterpoint table: {e}")

    return agreement, product, meterpoint
