#!/bin/bash

# Inicializar HDFS
docker exec -it hadoop /workspace/scripts/init_hdfs.sh

# Inicializar Spark
docker exec -it spark /workspace/scripts/init_spark.sh

# Generar eventos y enviarlos al socket
docker exec -d spark python /workspace/scripts/generate_events.py

# Stream de eventos a HDFS
docker exec -d spark python /workspace/scripts/stream_to_hdfs.py

# Transformar datos de Bronze a Silver
docker exec -it spark python /workspace/scripts/transform_bronze_to_silver.py

# Transformar datos de Silver a Gold y cargar en PostgreSQL
docker exec -it spark python /workspace/scripts/transform_silver_to_gold.py
