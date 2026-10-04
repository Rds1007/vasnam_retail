from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.temporary_view()
def customer_cdc_source():
    """Preprocess customer data for Auto CDC: cast sequence and effective columns to proper types."""
    return (
        spark.readStream.table("bronze_customer")
        .withColumn("source_updated_at", col("source_updated_at").cast("timestamp"))
        .withColumn("effective_date", col("effective_date").cast("date"))
    )


dp.create_streaming_table(
    name="customer_scd2",
    comment="Customer data with SCD Type 2 history tracking via Auto CDC",
    cluster_by_auto=True
)

dp.create_auto_cdc_flow(
    target="customer_scd2",
    source="customer_cdc_source",
    keys=["customer_id"],
    sequence_by="source_updated_at",
    stored_as_scd_type=2,
    except_column_list=["_ingested_at"]
)
