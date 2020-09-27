FROM centos

WORKDIR /opt/flink
COPY ./flink/ /opt/flink
# COPY ./recommendation-keti2-1.0-SNAPSHOT-jar-with-dependencies.jar /alidemo.jar
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
ENV HDFS_PREFIX hdfs://172.17.175.126:9000/data/keti3
ENV CLAZZ cn.edu.neu.tiger.RecPipelineWithPrometheus

ENTRYPOINT ["sh", "-c", "${FLINK_HOME}/bin/flink run -m 172.26.11.207:8081 -c ${CLAZZ} \
    /alidemo.jar \
    --paraKafka 2 \
    --paraRecall 60 \
    --paraGen 60 \
    --paraInference 9 \
    --paraSink 9 \
    --codePath ${HDFS_PREFIX}/athena-wdl.zip \
    --pyFile main.py \ 
    --encodeType STEING, INT_64"]

# flink WordCount测试用例
# ENV HDFS_PREFIX1 hdfs://172.17.175.126:9000/

# ENTRYPOINT ["sh", "-c", "./bin/flink run -m 172.26.11.207:8081 ./examples/batch/PageRank.jar --input ${HDFS_PREFIX1}/word.txt"]

# flink GPU测试用例
#ENTRYPOINT ["sh", "-c", "${FLINK_HOME}/bin/flink run -m 172.26.11.207:8081 \
#    -c cn.edu.neu.tiger.athena.AthenaTest \
#    /alidemo.jar \
#    --codePath ${HDFS_PREFIX}/athena-wdl.zip \
#    --pyFile main.py"] 
#    # --parallelism 1

