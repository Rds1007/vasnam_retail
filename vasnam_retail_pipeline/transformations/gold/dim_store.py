from pyspark import pipelines as dp


@dp.materialized_view(
    name="dim_store",
    comment="Current store dimension (latest SCD2 version per store)",
    cluster_by_auto=True
)
def dim_store():
    return (
        spark.read.table("store_scd2")
        .filter("__END_AT IS NULL")
        .select(
            "store_id",
            "store_name",
            "city",
            "state",
            "region",
            "store_type",
            "opening_date",
        )
    )
