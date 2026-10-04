from pyspark import pipelines as dp
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, dayofweek, date_format, weekofyear


@dp.materialized_view(
    name="dim_date",
    comment="Date dimension derived from sales transaction dates",
    cluster_by_auto=True
)
def dim_date():
    return (
        spark.read.table("clean_sales")
        .select(col("sale_date").alias("date"))
        .distinct()
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("week_of_year", weekofyear(col("date")))
        .orderBy("date")
    )
