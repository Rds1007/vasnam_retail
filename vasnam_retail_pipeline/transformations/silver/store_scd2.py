from pyspark import pipelines as dp


dp.create_streaming_table(
    name="store_scd2",
    comment="Store data with SCD Type 2 history tracking via Auto CDC",
    cluster_by_auto=True
)

dp.create_auto_cdc_flow(
    target="store_scd2",
    source="bronze_store",
    keys=["store_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=2,
    except_column_list=["_ingested_at"]
)
