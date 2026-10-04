from pyspark import pipelines as dp
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dim_product",
    comment="Product dimension (latest version per product)",
    cluster_by_auto=True
)
def dim_product():
    w = Window.partitionBy("product_id").orderBy(col("_ingested_at").desc())
    return (
        spark.read.table("clean_product")
        .withColumn("rn", row_number().over(w))
        .filter("rn = 1")
        .drop("rn", "_ingested_at")
    )
