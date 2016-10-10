#!/bin/bash

if [ "$#" -lt 1 ]
then
    echo "$0 [class-name] [config-file]"
	echo "  ex) $0 ExampleCmd"
	echo "  ex) $0 ExampleCmd config/config.properties"
	echo
    exit;
fi

_CONFIG="$1.properties"
if [ "$#" -eq 2 ]
then
	_CONFIG=$2
fi

cd ..

spark-submit \
 --class org.irgroup.spark.ml.$1 \
 --master local[*] \
 --executor-memory 8g \
 target/ml-1.0-SNAPSHOT.jar \
 $_CONFIG
