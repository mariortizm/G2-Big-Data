#!/bin/bash

# Instalar geopandas
pip install geopandas

# Iniciar Spark
start-master.sh
start-worker.sh spark://spark:7077
