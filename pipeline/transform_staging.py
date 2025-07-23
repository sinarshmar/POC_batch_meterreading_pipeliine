from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, current_date


def create_staging_tables(readings: DataFrame, agreement: DataFrame, product: DataFrame, meterpoint: DataFrame):
    try:
        print("Transforming readings data...")
        """Transform and enrich readings with agreement/product info."""
        readings = readings.withColumn("interval_start", to_timestamp("interval_start")) \
            .withColumn("ingestion_date", current_date())

        # Selecting the columns after joining to make sure the schema stys consistent even if the input format varies.
        agreements = agreement.join(product, "product_id").join(meterpoint, "meterpoint_id").select("agreement_id",
                                                                                                    "product_id",
                                                                                                    "meterpoint_id",
                                                                                                    "agreement_valid_from",
                                                                                                    "agreement_valid_to",
                                                                                                    "display_name",
                                                                                                    "is_variable",
                                                                                                    "region",
                                                                                                    "account_id")

        return {
            "readings": readings,
            "agreements": agreements
        }
    except Exception as e:
        print(f"Error in staging transformation: {e}")
        raise
