import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import Row


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

    tables = {
        "olist_customers_dataset": "olist_customers_dataset.csv",
        "olist_geolocation_dataset": "olist_geolocation_dataset.csv",
        "olist_order_items_dataset": "olist_order_items_dataset.csv",
        "olist_order_payments_dataset": "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset": "olist_order_reviews_dataset.csv",
        "olist_orders_dataset": "olist_orders_dataset.csv",
        "olist_products_dataset": "olist_products_dataset.csv",
        "olist_sellers_dataset": "olist_sellers_dataset.csv",
        "product_categories_name_translation": "product_category_name_translation.csv"
    }

    for table, file_name in tables.items():
        path = os.path.join(current_dir, "ecommerce-data", file_name)

        # Converter tipos de dados durante a leitura do CSV
        if table == "olist_order_reviews_dataset":
            df: DataFrame = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(path) \
                .withColumn("review_score", F.col("review_score").cast("int")) \
                .withColumn("review_creation_date", F.col("review_creation_date").cast("timestamp")) \
                .withColumn("review_answer_timestamp", F.col("review_answer_timestamp").cast("timestamp"))
        elif table == "olist_order_items_dataset":
            df: DataFrame = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(path) \
                .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast("timestamp"))
        elif table == "olist_orders_dataset":
            df: DataFrame = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(path) \
                .withColumn("order_purchase_timestamp", F.col("order_purchase_timestamp").cast("timestamp")) \
                .withColumn("order_approved_at", F.col("order_approved_at").cast("timestamp")) \
                .withColumn("order_delivered_carrier_date", F.col("order_delivered_carrier_date").cast("timestamp")) \
                .withColumn("order_delivered_customer_date", F.col("order_delivered_customer_date").cast("timestamp")) \
                .withColumn("order_estimated_delivery_date", F.col("order_estimated_delivery_date").cast("timestamp"))
        elif table == "olist_products_dataset":

            df: DataFrame = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(path) \
                .withColumnRenamed("product_name_lenght", "product_name_length") \
                .withColumnRenamed("product_description_lenght", "product_description_length") \
                .withColumn("product_name_length", F.col("product_name_length").cast("int")) \
                .withColumn("product_description_length", F.col("product_description_length").cast("int"))
        else:
            df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

        df.writeTo(f"my_iceberg_catalog.db.{table}").append()


def insert_records(spark: SparkSession):
    # Inserir registros nas tabelas usando spark.sql
    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.olist_order_items_dataset
        VALUES ('1', 1, 'produto1', 'vendedor1', TIMESTAMP '2024-04-25 12:00:00', 100.0, 10.0);
    """)

    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.olist_order_payments_dataset
        VALUES ('1', 1, 'debito', 1, 100.0);
    """)

    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.olist_order_reviews_dataset
        VALUES ('1', 'pedido1', 5, 'ótimo produto', 'produto chegou antes do prazo', TIMESTAMP '2024-04-25 12:00:00', TIMESTAMP '2024-04-26 12:00:00');
    """)

    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.olist_orders_dataset
        VALUES ('1', 'cliente1', 'aprovado', TIMESTAMP '2024-04-25 12:00:00', TIMESTAMP '2024-04-25 12:00:00', TIMESTAMP '2024-04-26 12:00:00', TIMESTAMP '2024-04-27 12:00:00', TIMESTAMP '2024-04-28 12:00:00');
    """)

    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.olist_products_dataset
        VALUES ('produto1', 'categoria1', 10, 50, 5, 500, 20, 30, 40);
    """)

    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.olist_sellers_dataset
        VALUES ('vendedor1', '12345', 'Cidade1', 'Estado1');
    """)

    spark.sql("""
        INSERT INTO my_iceberg_catalog.db.product_categories_name_translation
        VALUES ('categoria1', 'category1');
    """)

    # Inserir registros nas tabelas usando spark.createDataFrame
    order_payments_data = [
        Row(order_id='2', payment_sequential=2, payment_type='credito', payment_installments=2, payment_value=200.0),
        Row(order_id='3', payment_sequential=3, payment_type='boleto', payment_installments=3, payment_value=300.0)
    ]
    order_payments_df = spark.createDataFrame(order_payments_data, schema=['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value'])
    order_payments_df.writeTo("my_iceberg_catalog.db.olist_order_payments_dataset").append()

    order_reviews_data = [
        Row(review_id='2', order_id='pedido2', review_score=4, review_comment_title='bom produto', review_comment_message='recomendo', review_creation_date='2024-04-25 12:00:00', review_answer_timestamp='2024-04-26 12:00:00'),
        Row(review_id='3', order_id='pedido3', review_score=3, review_comment_title='produto regular', review_comment_message='poderia melhorar', review_creation_date='2024-04-25 12:00:00', review_answer_timestamp='2024-04-26 12:00:00')
    ]
    order_reviews_df = spark.createDataFrame(order_reviews_data, schema=['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'])
    order_reviews_df = order_reviews_df \
        .withColumn("review_creation_date", F.col("review_creation_date").cast(TimestampType())) \
        .withColumn("review_answer_timestamp", F.col("review_answer_timestamp").cast(TimestampType()))
    order_reviews_df.writeTo("my_iceberg_catalog.db.olist_order_reviews_dataset").append()

    orders_data = [
        Row(order_id='2', customer_id='cliente2', order_status='aprovado', order_purchase_timestamp='2024-04-25 12:00:00', order_approved_at='2024-04-25 12:00:00', order_delivered_carrier_date='2024-04-26 12:00:00', order_delivered_customer_date='2024-04-27 12:00:00', order_estimated_delivery_date='2024-04-28 12:00:00'),
        Row(order_id='3', customer_id='cliente3', order_status='cancelado', order_purchase_timestamp='2024-04-25 12:00:00', order_approved_at='2024-04-25 12:00:00', order_delivered_carrier_date='2024-04-26 12:00:00', order_delivered_customer_date='2024-04-27 12:00:00', order_estimated_delivery_date='2024-04-28 12:00:00')
    ]
    orders_df = spark.createDataFrame(orders_data, schema=['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'])
    orders_df = orders_df \
        .withColumn("order_purchase_timestamp", F.col("order_purchase_timestamp").cast(TimestampType())) \
        .withColumn("order_approved_at", F.col("order_approved_at").cast(TimestampType())) \
        .withColumn("order_delivered_carrier_date", F.col("order_delivered_carrier_date").cast(TimestampType())) \
        .withColumn("order_delivered_customer_date", F.col("order_delivered_customer_date").cast(TimestampType())) \
        .withColumn("order_estimated_delivery_date", F.col("order_estimated_delivery_date").cast(TimestampType()))
    orders_df.writeTo("my_iceberg_catalog.db.olist_orders_dataset").append()

    products_data = [
        Row(product_id='produto2', product_category_name='categoria2', product_name_length=8, product_description_length=100, product_photos_qty=3, product_weight_g=600, product_length_cm=25, product_height_cm=35, product_width_cm=45),
        Row(product_id='produto3', product_category_name='categoria3', product_name_length=12, product_description_length=150, product_photos_qty=5, product_weight_g=800, product_length_cm=30, product_height_cm=40, product_width_cm=50)
    ]
    products_df = spark.createDataFrame(products_data, schema=['product_id', 'product_category_name', 'product_name_length', 'product_description_length', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])
    products_df.writeTo("my_iceberg_catalog.db.olist_products_dataset").append()


def update_data(spark: SparkSession):
  spark.table("my_iceberg_catalog.db.olist_geolocation_dataset").filter(F.col("geolocation_zip_code_prefix") == "1001").show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_geolocation_dataset SET geolocation_city = 'New York' WHERE geolocation_zip_code_prefix = '1001'")
  spark.table("my_iceberg_catalog.db.olist_geolocation_dataset").filter(F.col("geolocation_zip_code_prefix") == "1001").show(3)

  spark.table("my_iceberg_catalog.db.olist_customers_dataset").filter(F.col("customer_zip_code_prefix") == "14409").show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_customers_dataset SET customer_city = 'SOMBRIO', customer_state = 'SC' WHERE customer_zip_code_prefix = '14409'")
  spark.table("my_iceberg_catalog.db.olist_customers_dataset").filter(F.col("customer_zip_code_prefix") == "14409").show(3)

  spark.table("my_iceberg_catalog.db.olist_order_items_dataset").filter((F.col("shipping_limit_date") >= '2017-04-01 00:00:00') & (F.col("shipping_limit_date") <= '2017-06-30 23:59:59')).orderBy('order_id').show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_order_items_dataset SET price = price / 2 WHERE (shipping_limit_date >= '2017-04-01 00:00:00') AND (shipping_limit_date <= '2017-06-30 23:59:59')")
  spark.table("my_iceberg_catalog.db.olist_order_items_dataset").filter((F.col("shipping_limit_date") >= '2017-04-01 00:00:00') & (F.col("shipping_limit_date") <= '2017-06-30 23:59:59')).orderBy('order_id').show(3)

  spark.table("my_iceberg_catalog.db.olist_order_payments_dataset").filter(F.col("payment_type") == "debit_card").orderBy('payment_sequential').show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_order_payments_dataset SET payment_value = payment_value * 1.1 WHERE payment_type = 'debit_card'")
  spark.table("my_iceberg_catalog.db.olist_order_payments_dataset").filter(F.col("payment_type") == "debit_card").orderBy('payment_sequential').show(3)

  spark.table("my_iceberg_catalog.db.olist_order_reviews_dataset").filter((F.col("review_creation_date") >= '2018-01-05') & (F.col("review_creation_date") <= '2018-04-30')).orderBy('order_id').show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_order_reviews_dataset SET review_score = 5 WHERE (review_score = 1) AND (review_creation_date >= '2018-01-05') AND (review_creation_date <= '2018-04-30')")
  spark.table("my_iceberg_catalog.db.olist_order_reviews_dataset").filter((F.col("review_creation_date") >= '2018-01-05') & (F.col("review_creation_date") <= '2018-04-30')).orderBy('order_id').show(3)

  spark.table("my_iceberg_catalog.db.olist_orders_dataset").filter(F.col("order_status") == "delivered").orderBy('order_id').show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_orders_dataset SET order_status = 'processing' WHERE order_status = 'delivered'")
  spark.table("my_iceberg_catalog.db.olist_orders_dataset").filter(F.col("order_status") == "processing").orderBy('order_id').show(3)

  spark.table("my_iceberg_catalog.db.olist_products_dataset").filter(F.col("product_category_name") == "informatica_acessorios").show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_products_dataset SET product_category_name = 'informatica' WHERE product_category_name = 'informatica_acessorios'")
  spark.table("my_iceberg_catalog.db.olist_products_dataset").filter(F.col("product_category_name") == "informatica").show(3)

  spark.table("my_iceberg_catalog.db.olist_sellers_dataset").filter(F.col("seller_zip_code_prefix") == "88804").show(3)
  spark.sql("UPDATE my_iceberg_catalog.db.olist_sellers_dataset SET seller_city = 'Sombrio' WHERE seller_zip_code_prefix = '88804'")
  spark.table("my_iceberg_catalog.db.olist_sellers_dataset").filter(F.col("seller_zip_code_prefix") == "88804").show(3)


def delete_data(spark: SparkSession):
    spark.table("my_iceberg_catalog.db.olist_geolocation_dataset").filter(F.col("geolocation_city") == "nova veneza").show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_geolocation_dataset WHERE geolocation_city = 'nova veneza';")
    spark.table("my_iceberg_catalog.db.olist_geolocation_dataset").filter(F.col("geolocation_city") == "nova veneza").show(3)

    spark.table("my_iceberg_catalog.db.olist_order_items_dataset").filter(F.col("price") < 5000).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_order_items_dataset WHERE price < 5000 ;")
    spark.table("my_iceberg_catalog.db.olist_order_items_dataset").filter(F.col("price") < 5000).show(3)

    spark.table("my_iceberg_catalog.db.olist_order_payments_dataset").filter((F.col("payment_type") == "voucher") | (F.col("payment_type") == "boleto")).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_order_payments_dataset WHERE payment_type = 'voucher' OR payment_type = 'boleto' ;")
    spark.table("my_iceberg_catalog.db.olist_order_payments_dataset").filter((F.col("payment_type") == "voucher") | (F.col("payment_type") == "boleto")).show(3)

    spark.table("my_iceberg_catalog.db.olist_order_reviews_dataset").filter(F.col("review_score") < 3).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_order_reviews_dataset WHERE review_score < 3 ;")
    spark.table("my_iceberg_catalog.db.olist_order_reviews_dataset").filter(F.col("review_score") < 3).show(3)

    spark.table("my_iceberg_catalog.db.olist_orders_dataset").filter((F.col("order_status") == "delivered") & (F.col("order_purchase_timestamp") < "2018-01-01 00:00:00")).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_orders_dataset WHERE order_status = 'delivered' AND order_purchase_timestamp < '2018-01-01 00:00:00' ;")
    spark.table("my_iceberg_catalog.db.olist_orders_dataset").filter((F.col("order_status") == "delivered") & (F.col("order_purchase_timestamp") < "2018-01-01 00:00:00")).show(3)

    spark.table("my_iceberg_catalog.db.olist_products_dataset").filter(F.col("product_category_name").isNull()).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_products_dataset WHERE product_category_name IS NULL ;")
    spark.table("my_iceberg_catalog.db.olist_products_dataset").filter(F.col("product_category_name").isNull()).show(3)

    spark.table("my_iceberg_catalog.db.olist_sellers_dataset").filter(F.col("seller_zip_code_prefix") > 50000).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_sellers_dataset WHERE seller_zip_code_prefix > 50000 ;")
    spark.table("my_iceberg_catalog.db.olist_sellers_dataset").filter(F.col("seller_zip_code_prefix") > 50000).show(3)

    spark.table("my_iceberg_catalog.db.olist_customers_dataset").filter((F.col("customer_state") == "SP") & (F.col("customer_city") == "pindamonhangaba")).show(3)
    spark.sql("DELETE FROM my_iceberg_catalog.db.olist_customers_dataset WHERE customer_state = 'SP' AND customer_city = 'pindamonhangaba' ;")
    spark.table("my_iceberg_catalog.db.olist_customers_dataset").filter((F.col("customer_state") == "SP") & (F.col("customer_city") == "pindamonhangaba")).show(3)
    
    
def read_data(spark: SparkSession):
    olist_geolocation_dataset = spark.table("my_iceberg_catalog.db.olist_geolocation_dataset")
    olist_geolocation_dataset.orderBy("geolocation_zip_code_prefix").show(3)


def app():
    spark = init_spark()

    create_table(spark)
<<<<<<< HEAD

    write_data(spark)
=======
    
    update_data(spark)
    #write_data(spark)
>>>>>>> 9f564767694af7f8619475884bf15554e8fb2725

    insert_records(spark)

<<<<<<< HEAD
    #update_data(spark)
    read_data(spark)
=======
    # update_data(spark)
    #read_data(spark)
>>>>>>> 9f564767694af7f8619475884bf15554e8fb2725

    #drop_table(spark)


if __name__ == "__main__":
    app()
