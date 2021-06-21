#!/bin/bash
SPARK_APPLICATION_JAR_LOCATION="/opt/spark-apps/task3/task3.jar"
SPARK_APPLICATION_MAIN_CLASS="scala.ETLv2"
#SPARK_SUBMIT_ARGS="--conf spark.executor.extraJavaOptions='-Dconfig-path=/opt/spark-apps/dev/config.conf'"

docker run --network proton_default -v :/opt/spark-apps --env SPARK_APPLICATION_JAR_LOCATION=$SPARK_APPLICATION_JAR_LOCATION --env SPARK_APPLICATION_MAIN_CLASS=$SPARK_APPLICATION_MAIN_CLASS spark-submit:v1.0

#SEE THE RESULTS AT:
# http://localhost:9091