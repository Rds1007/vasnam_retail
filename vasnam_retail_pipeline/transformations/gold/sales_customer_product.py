from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="sales_customer_product_st",
    comment="Intermediate streaming table: sales enriched with customer and product dimensions",
    cluster_by_auto=True,
    private=True,
)
def sales_customer_product_st():
    sales = spark.readStream.table("clean_sales").alias("s")
    customers = spark.read.table("dim_customer").alias("c")
    products = spark.read.table("dim_product").alias("p")

    return (
        sales
        .join(customers, col("s.customer_id") == col("c.customer_id"), "left")
        .join(products, col("s.product_id") == col("p.product_id"), "left")
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
        )
    )
