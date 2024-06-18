from pyhive import hive
from TCLIService.ttypes import TOperationState
import time

# Conectar a Hive
conn = hive.Connection(host='localhost', port=10000, username='your_username', database='default')
cursor = conn.cursor()

# Crear la tabla en Hive
create_table_query = """
CREATE TABLE IF NOT EXISTS silver_data (
    latitude FLOAT,
    longitude FLOAT,
    date STRING,
    customer_id INT,
    employee_id INT,
    quantity_products INT,
    order_id STRING,
    day STRING,
    month STRING,
    year STRING,
    time STRING,
    comuna STRING,
    codigo_postal STRING
)
STORED AS PARQUET
LOCATION 'hdfs://172.17.0.2:9000/silver_data';
"""

# Ejecutar la consulta de creación de tabla
cursor.execute(create_table_query)

# Verificar el estado de la operación
operation_state = cursor.poll()
while operation_state.operationState in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
    print("Waiting for operation to complete...")
    time.sleep(1)
    operation_state = cursor.poll()

print("Table created successfully.")

# Cargar los datos desde el archivo Parquet en HDFS a la tabla de Hive
load_data_query = """
LOAD DATA INPATH 'hdfs://172.17.0.2:9000/silver_data'
INTO TABLE silver_data;
"""

# Ejecutar la consulta de carga de datos
cursor.execute(load_data_query)

# Verificar el estado de la operación
operation_state = cursor.poll()
while operation_state.operationState in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
    print("Waiting for data load to complete...")
    time.sleep(1)
    operation_state = cursor.poll()

print("Data loaded successfully into Hive table.")

# Cerrar la conexión
cursor.close()
conn.close()
