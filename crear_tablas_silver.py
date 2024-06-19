from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SparkTableCreation") \
    .config("spark.sql.warehouse.dir", "hdfs://172.17.0.2:9000/user/spark/warehouse") \
    .getOrCreate()

# Crear la tabla silver_data
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_data (
      latitude FLOAT,
      longitude FLOAT,
      date STRING,
      customer_id STRING,
      employee_id STRING,
      quantity_products INT,
      order_id STRING,
      day STRING,
      month STRING,
      year STRING,
      time STRING,
      comuna STRING,
      codigo_postal STRING
    )
    USING PARQUET
    LOCATION 'hdfs://172.17.0.2:9000/silver_data'
""")

# Crear la tabla customers
spark.sql("""
    CREATE TABLE IF NOT EXISTS customers (
      customer_id BIGINT,
      name STRING,
      phone STRING,
      email STRING,
      address STRING
    )
    USING PARQUET
    LOCATION 'hdfs://172.17.0.2:9000/customers'
""")

# Crear la tabla employees
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees (
      employee_id BIGINT,
      name STRING,
      phone STRING,
      email STRING,
      address STRING,
      comission DOUBLE
    )
    USING PARQUET
    LOCATION 'hdfs://172.17.0.2:9000/employees'
""")

# Detener la sesión de Spark
spark.stop()


