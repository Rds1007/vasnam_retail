from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_promotion",
    comment="Raw promotion data ingested from landing zone via Auto Loader"
)
def bronze_promotion():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "promotion*.json")
        .load("/Volumes/vasnam_retail/vesnam/landing/")
        .withColumn("_ingested_at", current_timestamp())
    )
