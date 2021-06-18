#!/bin/bash

docker build -t spark-base:v1.0 docker/base
docker build -t spark-master:v1.0 docker/spark-master
docker build -t spark-worker:v1.0 docker/spark-worker
docker build -t spark-submit:v1.0 docker/spark-submit