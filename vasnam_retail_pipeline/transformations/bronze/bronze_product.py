from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_product",
    comment="Raw product data ingested from landing zone via Auto Loader"
)
def bronze_product():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "product*.json")
        .load("/Volumes/vasnam_retail/vesnam/landing/")
        .withColumn("_ingested_at", current_timestamp())
    )
