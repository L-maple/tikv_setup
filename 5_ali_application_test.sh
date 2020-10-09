#! /bin/sh

echo "deploy the recommendation_application ..."

echo "set mainClass to RecPipelineWithPrometheus and recompile..."
projectDir=codeForali/recommendation-keti2-v9.1/

echo "copy necessary files..."
cp alter_mainclass.py $projectDir
cp jdk-8u131-linux-x64.tar.gz $projectDir
cp -R flink/ $projectDir

cd $projectDir
python alter_mainclass.py cn.edu.neu.tiger.tikv.data.RecPipelineWithPrometheus

mvn clean install

stat target

echo "copy jar package in the current dir..."
cp target/recommendation-keti2-1.0-SNAPSHOT.jar .

echo "the project has been recompiled..."
echo "build the docker recommendation-deployment image..."
echo "cat dockerfilecontent to Dockerfile..."
cat > Dockerfile <<EOF 
FROM centos

WORKDIR /opt/flink
COPY ./flink/ /opt/flink
COPY ./recommendation-keti2-1.0-SNAPSHOT.jar /alidemo.jar

# 配置java环境
RUN mkdir /usr/local/java
ADD jdk-8u131-linux-x64.tar.gz /usr/local/java/
RUN ln -s /usr/local/java/jdk1.8.0_131 /usr/local/java/jdk
ENV JAVA_HOME /usr/local/java/jdk
ENV JRE_HOME ${JAVA_HOME}/jre
ENV CLASSPATH .:${JAVA_HOME}/lib:${JRE_HOME}/lib
ENV PATH ${JAVA_HOME}/bin:$PATH

ENV FLINK_HOME /opt/flink
# 注意hdfs的地址变更
ENV HDFS_PREFIX hdfs://172.17.175.126:9000/data/keti3 
ENV CLAZZ cn.edu.neu.tiger.RecPipelineWithPrometheus

ENTRYPOINT ["sh", "-c", "/opt/flink/bin/flink run -m 172.26.11.207:8081 -c cn.edu.neu.tiger.RecPipelineWithPrometheus \
    /alidemo.jar \
    --paraKafka 1 \
    --paraRecall 1 \
    --paraGen 1 \
    --paraInference 1 \
    --paraSink 1 \
    --codePath hdfs://172.17.175.126:9000/data/keti3/athena-wdl.zip \
    --pyFile main.py \ 
    --encodeType STRING,INT_64"]
EOF

docker build -f Dockerfile -t recommendation-deployment .
echo "the recommendation-deployment image has been build..."
kubectl delete deploy recommendation-deployment -n tidb-cluster
kubectl apply -f recommendation-deployment.yaml
echo "the recommendation-deployment has been created..."

# delete unnecessary files
rm -f alter_mainclass.py 
rm -f jdk-8u131-linux-x64.tar.gz 
rm -rf flink/ 
rm -f recommendation-keti2-1.0-SNAPSHOT.jar

echo "the application demo has been deployed..."
