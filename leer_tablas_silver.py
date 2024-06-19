from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("QuerySparkTables") \
    .config("spark.sql.warehouse.dir", "hdfs://172.17.0.2:9000/user/spark/warehouse") \
    .getOrCreate()

# Registrar las tablas como vistas temporales
spark.read.parquet("hdfs://172.17.0.2:9000/silver_data").createOrReplaceTempView("silver_data")
spark.read.parquet("hdfs://172.17.0.2:9000/customers").createOrReplaceTempView("customers")
spark.read.parquet("hdfs://172.17.0.2:9000/employees").createOrReplaceTempView("employees")

# Consultar las tablas usando SQL
try:
    silver_data = spark.sql("SELECT * FROM silver_data")
    silver_data.show()
except Exception as e:
    print("Error al consultar la tabla silver_data:", e)

try:
    customers = spark.sql("SELECT * FROM customers")
    customers.show()
except Exception as e:
    print("Error al consultar la tabla customers:", e)

try:
    employees = spark.sql("SELECT * FROM employees")
    employees.show()
except Exception as e:
    print("Error al consultar la tabla employees:", e)

# Detener la sesión de Spark
spark.stop()

