from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pipeline.load_data import load_readings, load_db
from pipeline.transform_staging import create_staging_tables
from pipeline.generate_outputs import generate_final_tables
from pipeline.utils import log_file_processed


def run_all_jobs(jar_path, db_path, json_path):

    print("Creating the spark session")
    builder = SparkSession.builder \
        .appName("Energy Meter Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars", jar_path)

    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        print("Error creating SparkSession:", e)

    spark.conf.set("spark.sql.legacy.charVarcharAsString", "true")

    try:
        print("Loading data")
        readings = load_readings(spark, json_path)
        agreement, product, meterpoint = load_db(spark, db_path, jar_path)
    except Exception as e:
        print(f"Data loading failed: {e}")
        return

    try:
        print("Staging transformation started")
        staging = create_staging_tables(readings, agreement, product, meterpoint)
        print("Presentation layer transformation started")
        generate_final_tables(spark, staging, output_path="data_lake/presentation")
    except Exception as e:
        print(f"Transformation or output generation failed: {e}")

    try:
        print("Logging processed files")
        source_files = readings.select("source_file").distinct().rdd.flatMap(lambda x: x).collect()
        for file in source_files:
            log_file_processed(spark, "data_lake/logs/raw_ingestion_log", file)
    except Exception as e:
        print(f"Error logging processed files: {e}")
