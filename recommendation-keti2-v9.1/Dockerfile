FROM centos

WORKDIR /opt/flink
COPY ./flink/ /opt/flink
COPY ./recommendation-keti2-1.0-SNAPSHOT.jar /alidemo.jar

# 配置java环境
RUN mkdir /usr/local/java
ADD jdk-8u131-linux-x64.tar.gz /usr/local/java/
RUN ln -s /usr/local/java/jdk1.8.0_131 /usr/local/java/jdk
ENV JAVA_HOME /usr/local/java/jdk
ENV JRE_HOME /usr/java/jdk1.8.0_131/jre
ENV CLASSPATH .:/usr/java/jdk1.8.0_131/lib:/usr/java/jdk1.8.0_131/jre/lib
ENV PATH /usr/java/jdk1.8.0_131/bin:/usr/hadoop/hadoop-2.7.5/bin:/usr/hadoop/hadoop-2.7.5/sbin:/usr/java/jdk1.8.0_131/bin:/usr/java/jdk1.8.0_131/jre/bin:/usr/local/maven/apache-maven-3.6.3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin

ENV FLINK_HOME /opt/flink
ENV HDFS_PREFIX hdfs://172.17.175.126:9000/data/keti3
ENV CLAZZ cn.edu.neu.tiger.RecPipelineWithPrometheus

ENTRYPOINT ["sh", "-c", "/opt/flink/bin/flink run -m 172.26.11.207:8081 -c cn.edu.neu.tiger.RecPipelineWithPrometheus     /alidemo.jar     --paraKafka 24     --paraRecall 24     --paraGen 24     --paraInference 24     --paraSink 24     --codePath hdfs://172.17.175.126:9000/data/keti3/athena-wdl.zip     --pyFile main.py \ 
    --encodeType STRING,INT_64"]
