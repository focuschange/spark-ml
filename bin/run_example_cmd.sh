#!/bin/bash
# gcp cluster에서 실행시키기 위한 스크립트

if [ "$#" -lt 1 ]
then
    echo "$0 [class-name]"
    echo "  ex) $0 ExampleCmd"
    echo

    exit;
fi

gsutil cp gs://lsh-test-1/jars/ml-1.0-SNAPSHOT.jar .
gsutil cp gs://lsh-test-1/config/gcp.properties .

spark-submit \
 --class org.irgroup.spark.ml.$1 \
 ml-1.0-SNAPSHOT.jar \
 gcp.properties

#spark-submit \
# --class org.irgroup.spark.ml.ExampleCmd \
# --master yarn \
# --executor-memory 8g \
# --executor-cores 8 \
# --driver-memory 8g \
# --driver-cores 4 \
# --num-executors 16 \
# ml-1.0-SNAPSHOT.jar \
# gcp.properties
