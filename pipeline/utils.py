from pyspark.sql import SparkSession

def is_file_processed(spark: SparkSession, log_path: str, file_name: str) -> bool:
    try:
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(spark, log_path):
            return False
        df = spark.read.format("delta").load(log_path)
        return df.filter(df.file_name == file_name).count() > 0
    except Exception as e:
        print(f"Error checking file processing status for {file_name}: {e}")
        return False

def log_file_processed(spark: SparkSession, log_path: str, file_name: str):
    try:
        from pyspark.sql import Row
        from datetime import datetime
        df = spark.createDataFrame([Row(file_name=file_name, timestamp=str(datetime.utcnow()))])
        df.write.format("delta").mode("append").save(log_path)
    except Exception as e:
        print(f"Error logging processed file {file_name}: {e}")
