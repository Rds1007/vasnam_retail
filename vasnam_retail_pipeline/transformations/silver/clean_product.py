from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="clean_product",
    comment="Cleaned and deduplicated product data with data quality expectations",
    cluster_by_auto=True
)
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_unit_price", "unit_price >= 0")
@dp.expect_or_drop("valid_cost_price", "cost_price >= 0")
@dp.expect("valid_product_name", "product_name IS NOT NULL")
def clean_product():
    return (
        spark.readStream.table("bronze_product")
        .withWatermark("_ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["product_id"])
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .withColumn("cost_price", col("cost_price").cast("decimal(10,2)"))
    )
