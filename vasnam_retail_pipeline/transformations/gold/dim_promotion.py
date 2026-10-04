from pyspark import pipelines as dp
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dim_promotion",
    comment="Promotion dimension (latest version per promotion)",
    cluster_by_auto=True
)
def dim_promotion():
    w = Window.partitionBy("promotion_id").orderBy(col("_ingested_at").desc())
    return (
        spark.read.table("clean_promotion")
        .withColumn("rn", row_number().over(w))
        .filter("rn = 1")
        .drop("rn", "_ingested_at")
    )
