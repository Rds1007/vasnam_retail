from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.foreach_batch_sink(name="fact_sales_iceberg_sink")
def fact_sales_iceberg_sink(df, batch_id):
    """Write to the Iceberg-compatible Delta table.

    batch_id == 0: overwrite (initial load or full refresh replaces all data).
    batch_id > 0: append only new rows from the incremental stream.
    """
    spark_session = df.sparkSession
    df.createOrReplaceTempView("batch_data")
    if batch_id == 0:
        spark_session.sql(
            """
            INSERT OVERWRITE TABLE vasnam_retail.vasnam.fact_sales_iceberg
            SELECT * FROM batch_data
            """
        )
    else:
        spark_session.sql(
            """
            INSERT INTO vasnam_retail.vasnam.fact_sales_iceberg
            SELECT * FROM batch_data
            """
        )


@dp.append_flow(
    target="fact_sales_iceberg_sink",
    comment="Stream clean_sales enriched with dimensions to Iceberg-enabled Delta sink"
)
def fact_sales_to_iceberg():
    sales = spark.readStream.table("clean_sales").alias("s")
    customers = spark.read.table("dim_customer").alias("c")
    products = spark.read.table("dim_product").alias("p")
    stores = spark.read.table("dim_store").alias("st")
    promotions = spark.read.table("dim_promotion").alias("pr")

    return (
        sales
        .join(customers, col("s.customer_id") == col("c.customer_id"), "left")
        .join(products, col("s.product_id") == col("p.product_id"), "left")
        .join(stores, col("s.store_id") == col("st.store_id"), "left")
        .join(promotions, col("s.promotion_id") == col("pr.promotion_id"), "left")
        .select(
            col("s.sales_id"),
            col("s.sale_date"),
            col("s.customer_id"),
            col("s.product_id"),
            col("s.store_id"),
            col("s.promotion_id"),
            col("s.quantity"),
            col("s.unit_price"),
            col("s.gross_amount"),
            col("s.discount_amount"),
            col("s.tax_amount"),
            col("s.net_sales_amount"),
            col("s.payment_method"),
            col("s.sales_channel"),
            col("c.customer_name"),
            col("c.customer_segment"),
            col("c.region").alias("customer_region"),
            col("p.product_name"),
            col("p.category"),
            col("p.subcategory"),
            col("p.brand"),
            col("st.store_name"),
            col("st.city").alias("store_city"),
            col("st.store_type"),
            col("pr.promotion_name"),
            col("pr.promotion_type"),
            col("pr.discount_percent").alias("promotion_discount_percent"),
        )
    )
