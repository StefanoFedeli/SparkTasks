#!/bin/bash
SPARK_APPLICATION_JAR_LOCATION="/opt/spark-apps/task1.jar"
SPARK_APPLICATION_MAIN_CLASS="scala.WordCount"
#SPARK_SUBMIT_ARGS="--conf spark.executor.extraJavaOptions='-Dconfig-path=/opt/spark-apps/dev/config.conf'"

docker run --network proton_default -v task1:/opt/spark-apps --env SPARK_APPLICATION_JAR_LOCATION=$SPARK_APPLICATION_JAR_LOCATION --env SPARK_APPLICATION_MAIN_CLASS=$SPARK_APPLICATION_MAIN_CLASS spark-submit:v1.0