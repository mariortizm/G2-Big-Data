#!/bin/bash

# Formatear HDFS
hdfs namenode -format

# Iniciar HDFS
start-dfs.sh
start-yarn.sh
