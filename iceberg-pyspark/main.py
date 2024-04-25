import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def init_spark():
    conf = SparkConf() \
        .setAppName("Apache Iceberg with PySpark") \
        .setMaster("local[2]") \
        .setAll([
            ("spark.driver.memory", "1g"),
            ("spark.executor.memory", "2g"),
            ("spark.sql.shuffle.partitions", "40"),

            # Add Iceberg SQL extensions like UPDATE or DELETE in Spark
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

            # Register `my_iceberg_catalog`
            ("spark.sql.catalog.my_iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),

            # Configure SQL connection to track tables inside `my_iceberg_catalog`
            ("spark.sql.catalog.my_iceberg_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"),
            ("spark.sql.catalog.my_iceberg_catalog.uri", "jdbc:postgresql://postgres:5432/iceberg_db"),
            ("spark.sql.catalog.my_iceberg_catalog.jdbc.user", "postgres"),
            ("spark.sql.catalog.my_iceberg_catalog.jdbc.password", "postgres"),

            # Configure Warehouse on MinIO
            ("spark.sql.catalog.my_iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            ("spark.sql.catalog.my_iceberg_catalog.s3.endpoint", "http://minio:9000"),
            ("spark.sql.catalog.my_iceberg_catalog.s3.path-style-access", "true"),
            ("spark.sql.catalog.my_iceberg_catalog.warehouse", "s3://warehouse"),
        ])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark


def create_table(spark: SparkSession):
    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_geolocation_dataset (
        geolocation_zip_code_prefix string COMMENT 'Código de Prefixo do CEP',
        geolocation_lat decimal(10,8) COMMENT 'Latitude Geográfica',
        geolocation_lng decimal(11,8) COMMENT 'Longitude Geográfica',
        geolocation_city string COMMENT 'Cidade de Geolocalização',
        geolocation_state string COMMENT 'Estado de Geolocalização'
      ) USING iceberg;
    """)
    
    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_order_items_dataset (
        order_id STRING COMMENT 'ID do Pedido',
        order_item_id INT COMMENT 'ID do Item do Pedido',
        product_id STRING COMMENT 'ID do Produto',
        seller_id STRING COMMENT 'ID do Vendedor',
        shipping_limit_date TIMESTAMP COMMENT 'Data Limite de Envio',
        price DECIMAL(10,2) COMMENT 'Preço',
        freight_value DECIMAL(10,2) COMMENT 'Valor do Frete'
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_order_payments_dataset (
        order_id STRING COMMENT 'ID do Pedido',
        payment_sequential INT COMMENT 'Sequência de Pagamento',
        payment_type STRING COMMENT 'Tipo de Pagamento',
        payment_installments INT COMMENT 'Parcelas do Pagamento',
        payment_value DECIMAL(10,2) COMMENT 'Valor do Pagamento'
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_order_reviews_dataset (
        review_id STRING COMMENT 'ID da Avaliação',
        order_id STRING COMMENT 'ID do Pedido',
        review_score INT COMMENT 'Pontuação da Avaliação',
        review_comment_title STRING COMMENT 'Título do Comentário da Avaliação',
        review_comment_message STRING COMMENT 'Mensagem do Comentário da Avaliação',
        review_creation_date TIMESTAMP COMMENT 'Data de Criação da Avaliação',
        review_answer_timestamp TIMESTAMP COMMENT 'Data de Resposta da Avaliação'
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_orders_dataset (
        order_id STRING COMMENT 'ID do Pedido',
        customer_id STRING COMMENT 'ID do Cliente',
        order_status STRING COMMENT 'Status do Pedido',
        order_purchase_timestamp TIMESTAMP COMMENT 'Timestamp de Compra do Pedido',
        order_approved_at TIMESTAMP COMMENT 'Timestamp de Aprovação do Pedido',
        order_delivered_carrier_date TIMESTAMP COMMENT 'Data de Entrega pelo Transportador',
        order_delivered_customer_date TIMESTAMP COMMENT 'Data de Entrega ao Cliente',
        order_estimated_delivery_date TIMESTAMP COMMENT 'Data Estimada de Entrega do Pedido'
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_products_dataset (
        product_id STRING COMMENT 'ID do Produto',
        product_category_name STRING COMMENT 'Nome da Categoria do Produto',
        product_name_length INT COMMENT 'Comprimento do Nome do Produto',
        product_description_length INT COMMENT 'Comprimento da Descrição do Produto',
        product_photos_qty INT COMMENT 'Quantidade de Fotos do Produto',
        product_weight_g INT COMMENT 'Peso do Produto em Gramas',
        product_length_cm INT COMMENT 'Comprimento do Produto em Centímetros',
        product_height_cm INT COMMENT 'Altura do Produto em Centímetros',
        product_width_cm INT COMMENT 'Largura do Produto em Centímetros'
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_sellers_dataset (
        seller_id STRING COMMENT 'ID do Vendedor',
        seller_zip_code_prefix STRING COMMENT 'Prefixo do CEP do Vendedor',
        seller_city STRING COMMENT 'Cidade do Vendedor',
        seller_state STRING COMMENT 'Estado do Vendedor'
      ) USING iceberg
    """)

    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.product_categories_name_translation (
        product_category_name STRING COMMENT 'Nome da Categoria do Produto',
        product_category_name_english STRING COMMENT 'Nome da Categoria do Produto em Inglês'
      ) USING iceberg
    """)
    
    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.olist_customers_dataset (
        customer_id STRING COMMENT 'ID do Cliente',
        customer_unique_id STRING COMMENT 'ID Único do Cliente',
        customer_zip_code_prefix STRING COMMENT 'Prefixo do CEP do Cliente',
        customer_city STRING COMMENT 'Cidade do Cliente',
        customer_state STRING COMMENT 'Estado do Cliente'
      ) USING iceberg
    """)



def drop_table(spark: SparkSession):
    spark.sql("TRUNCATE TABLE my_iceberg_catalog.db.olist_geolocation_dataset;")
    spark.sql("DROP TABLE my_iceberg_catalog.db.olist_geolocation_dataset;")


def write_data(spark: SparkSession):
    current_dir = os.path.realpath(os.path.dirname(__file__))
    path = os.path.join(current_dir, "ecommerce-data", "olist_geolocation_dataset.csv")

    olist_geolocation_dataset: DataFrame = spark.read \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .csv(path)

    olist_geolocation_dataset \
      .writeTo("my_iceberg_catalog.db.olist_geolocation_dataset") \
      .append()
      
def update_data(spark: SparkSession):
    olist_geolocation_dataset: DataFrame = spark.table("my_iceberg_catalog.db.olist_geolocation_dataset")
    olist_geolocation_dataset \
      .filter(F.col("geolocation_zip_code_prefix") == "1001") \
      .update("geolocation_city", F.lit("New York"))


def read_data(spark: SparkSession):
    olist_geolocation_dataset = spark.table("my_iceberg_catalog.db.olist_geolocation_dataset")
    olist_geolocation_dataset.orderBy("geolocation_zip_code_prefix").show(3)


def app():
    spark = init_spark()

    create_table(spark)

    #write_data(spark)

    # add_column(spark)

    # update_data(spark)
    read_data(spark)

    # drop_table(spark)


if __name__ == "__main__":
    app()