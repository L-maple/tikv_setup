#! /bin/sh

tar -xzvf deps.tar.gz
cd deps/

curdir=`pwd`
# flink-ml
mvn install:install-file -Dfile=$curdir/flink-ml-framework-0.3.0.jar -DgroupId=com.alibaba.flink.ml -DartifactId=flink-ml-framework -Dversion=0.3.0 -Dpackaging=jar
mvn install:install-file -Dfile=$curdir/flink-ml-operator-0.3.0.jar -DgroupId=com.alibaba.flink.ml -DartifactId=flink-ml-operator -Dversion=0.3.0 -Dpackaging=jar
mvn install:install-file -Dfile=$curdir/flink-ml-athena-0.3.0.jar -DgroupId=com.alibaba.flink.ml -DartifactId=flink-ml-athena -Dversion=0.3.0 -Dpackaging=jar

# tikv
mvn install:install-file -Dfile=$curdir/tikv-client-java-2.0-SNAPSHOT.jar -DgroupId=org.tikv -DartifactId=tikv-client-java -Dversion=2.0-SNAPSHOT -Dpackaging=jar

echo "deps has been installed."
