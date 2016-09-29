#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "$0 [data-dir]"
    exit;
fi

spark-submit \
 --class org.irgroup.spark.ml.ExampleCmd \
 --master local[*] \
 --executor-memory 4g \
 target/ml-1.0-SNAPSHOT.jar "$1"
