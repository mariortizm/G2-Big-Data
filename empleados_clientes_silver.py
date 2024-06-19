import subprocess
from pyspark.sql import SparkSession

# Función para ejecutar comandos de shell
def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(f"Command failed with error {stderr.decode('utf-8')}")
    return stdout.decode('utf-8')

# Crear directorio en HDFS y subir archivos locales
run_command("hdfs dfs -mkdir -p /user/root/base.data")
run_command("hdfs dfs -put -f ./base.data/customers.parquet /user/root/base.data/")
run_command("hdfs dfs -put -f ./base.data/employees.parquet /user/root/base.data/")

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Parquet to HDFS with Gzip Compression") \
    .getOrCreate()

# Leer y escribir el archivo customers.parquet desde HDFS
customers_df = spark.read.parquet("hdfs://172.17.0.2:9000/user/root/base.data/customers.parquet")
customers_df.write.mode('append').parquet("hdfs://172.17.0.2:9000/customers", compression='gzip')

# Leer y escribir el archivo employees.parquet desde HDFS
employees_df = spark.read.parquet("hdfs://172.17.0.2:9000/user/root/base.data/employees.parquet")
employees_df.write.mode('append').parquet("hdfs://172.17.0.2:9000/employees", compression='gzip')

# Cerrar la sesión de Spark
spark.stop()



