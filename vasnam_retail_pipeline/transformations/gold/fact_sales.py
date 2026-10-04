from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="fact_sales_st",
    comment="Sales fact streaming table enriched with customer, product, store, and promotion dimensions",
    cluster_by_auto=True,
)
def fact_sales_st():
    sales_cp = spark.readStream.table("sales_customer_product_st").alias("scp")
    stores = spark.read.table("dim_store").alias("st")
    promotions = spark.read.table("dim_promotion").alias("pr")

    return (
        sales_cp
        .join(stores, col("scp.store_id") == col("st.store_id"), "left")
        .join(promotions, col("scp.promotion_id") == col("pr.promotion_id"), "left")
        .select(
            col("scp.sales_id"),
            col("scp.sale_date"),
            col("scp.customer_id"),
            col("scp.product_id"),
            col("scp.store_id"),
            col("scp.promotion_id"),
            col("scp.quantity"),
            col("scp.unit_price"),
            col("scp.gross_amount"),
            col("scp.discount_amount"),
            col("scp.tax_amount"),
            col("scp.net_sales_amount"),
            col("scp.payment_method"),
            col("scp.sales_channel"),
            col("scp.customer_name"),
            col("scp.customer_segment"),
            col("scp.customer_region"),
            col("scp.product_name"),
            col("scp.category"),
            col("scp.subcategory"),
            col("scp.brand"),
            col("st.store_name"),
            col("st.city").alias("store_city"),
            col("st.store_type"),
            col("pr.promotion_name"),
            col("pr.promotion_type"),
            col("pr.discount_percent").alias("promotion_discount_percent"),
        )
    )
