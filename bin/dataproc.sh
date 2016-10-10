#!/bin/bash

# ex : ./dataproc.sh ExampleCmd
# ex : ./dataproc.sh Word2VecExample gcp.properties

if [ "$#" -lt 1 ]
then
    echo "$0 [class-name] [config-file]"
    echo "  ex) $0 ExampleCmd"
    echo "  ex) $0 ExampleCmd config.properties"
    echo
    exit
fi

_CONFIG="$1.properties"
if [ "$#" -eq 2 ]
then
    _CONFIG=$2
fi

cd ..

#gcloud dataproc jobs submit spark \
#	--cluster="lsh-cluster-1" \
#	--bucket="lsh-test-1" \
#	--class=org.irgroup.spark.ml.ExampleCmd \
#	--jar=gs://lsh-test-1/jars/ml-1.0-SNAPSHOT.jar \
#	--files=gs://lsh-test-1/config/config.properties \
#	--properties spark.executor.instances=16 \
#	config.properties

gcloud dataproc jobs submit spark \
	--cluster="lsh-cluster-1" \
	--class=org.irgroup.spark.ml.$1 \
	--jar=target/ml-1.0-SNAPSHOT.jar \
	--files=config/$_CONFIG \
	$_CONFIG

# 경로명은 7bit ascii만 지원 함. 젠장.. 
#	--jar="/Users/focuschange/Google 드라이브/program/java/spark-ml/target/ml-1.0-SNAPSHOT.jar"
