from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="clean_promotion",
    comment="Cleaned and deduplicated promotion data with data quality expectations",
    cluster_by_auto=True
)
@dp.expect_or_fail("valid_promotion_id", "promotion_id IS NOT NULL")
@dp.expect_or_drop("valid_discount", "discount_percent >= 0 AND discount_percent <= 100")
@dp.expect("valid_dates", "start_date <= end_date")
def clean_promotion():
    return (
        spark.readStream.table("bronze_promotion")
        .withWatermark("_ingested_at", "1 hour")
        .dropDuplicatesWithinWatermark(["promotion_id"])
        .withColumn("start_date", col("start_date").cast("date"))
        .withColumn("end_date", col("end_date").cast("date"))
        .withColumn("discount_percent", col("discount_percent").cast("int"))
    )
