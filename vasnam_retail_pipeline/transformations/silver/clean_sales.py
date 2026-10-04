from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="clean_sales",
    comment="Cleaned and deduplicated sales data with data quality expectations",
    cluster_by_auto=True
)
@dp.expect_or_fail("valid_sales_id", "sales_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_net_amount", "net_sales_amount >= 0")
@dp.expect_or_drop("valid_gross_amount", "gross_amount >= 0")
def clean_sales():
    return (
        spark.readStream.table("bronze_sales")
        .withWatermark("_ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["sales_id"])
        .withColumn("sale_date", col("sale_date").cast("date"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)"))
        .withColumn("gross_amount", col("gross_amount").cast("decimal(12,2)"))
        .withColumn("discount_amount", col("discount_amount").cast("decimal(12,2)"))
        .withColumn("tax_amount", col("tax_amount").cast("decimal(12,2)"))
        .withColumn("net_sales_amount", col("net_sales_amount").cast("decimal(12,2)"))
    )
