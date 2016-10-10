#!/bin/bash
# ex : gsup.sh target/ml.jar jars
# ex : gsup.sh target/lib jars
# ex : gsup.sh "*.sh" bin <= 여러 파일 카피는 반드시 "" 사용할 것 

function help {
    echo "Usage : $0 [target]"
	echo "  ex) $0 jar"
	echo "  ex) $0 lib"
	echo "  ex) $0 bin"
	echo "  ex) $0 conf"
	echo "  ex) $0 all"
	echo
}

if [ "$#" -lt 1 ]
then
    help
    exit
fi

TARGET=$1
GS_HOME_DIR="gs://lsh-test-1"

if [ $TARGET == "jar" ]
then
    gsutil cp ../target/ml-1.0-SNAPSHOT.jar $GS_HOME_DIR/jars/
elif [ $TARGET == "lib" ]
then
    gsutil -m cp -r ../target/lib $GS_HOME_DIR/jars/
elif [ $TARGET == "bin" ]
then
    gsutil -m cp "*.sh" $GS_HOME_DIR/bin/
elif [ $TARGET == "conf" ]
then
    gsutil -m cp "../config/*.properties" $GS_HOME_DIR/config/
elif [ $TARGET == "all" ]
then
    gsutil -m cp -r ../target/lib $GS_HOME_DIR/jars/
    gsutil cp ../target/ml-1.0-SNAPSHOT.jar $GS_HOME_DIR/jars/
    gsutil -m cp "*.sh" $GS_HOME_DIR/bin/
else
    echo "[ERROR] invalid target."
    help
fi


