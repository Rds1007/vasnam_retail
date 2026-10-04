from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_customer",
    comment="Raw customer data ingested from landing zone via Auto Loader"
)
def bronze_customer():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "customer*.json")
        .load("/Volumes/vasnam_retail/vesnam/landing/")
        .withColumn("_ingested_at", current_timestamp())
    )
