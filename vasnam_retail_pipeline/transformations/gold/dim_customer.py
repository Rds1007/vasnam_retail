from pyspark import pipelines as dp


@dp.materialized_view(
    name="dim_customer",
    comment="Current customer dimension (latest SCD2 version per customer)",
    cluster_by_auto=True
)
def dim_customer():
    return (
        spark.read.table("customer_scd2")
        .filter("__END_AT IS NULL")
        .select(
            "customer_id",
            "customer_name",
            "gender",
            "date_of_birth",
            "email",
            "phone",
            "city",
            "state",
            "region",
            "customer_segment",
            "effective_date",
            "source_version",
        )
    )
