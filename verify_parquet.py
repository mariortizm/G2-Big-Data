import pandas as pd
from hdfs import InsecureClient
import pyarrow.parquet as pq
import io

# Conectar al cliente HDFS
client = InsecureClient('http://localhost:50070', user='root')

# Leer el archivo Parquet desde HDFS
with client.read('/user/hadoop_user/events/events.parquet') as reader:
    parquet_data = reader.read()

# Convertir los datos leídos en un buffer de bytes
parquet_buffer = io.BytesIO(parquet_data)

# Usar pyarrow para leer el buffer de bytes y convertirlo en un DataFrame de pandas
df = pq.read_table(parquet_buffer).to_pandas()

# Mostrar las primeras filas del DataFrame
print(df.head())
print(df.shape)