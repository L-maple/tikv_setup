FROM openjdk:8-jre-alpine

COPY target/recommendation-keti2-1.0-SNAPSHOT.jar /tikv-test-app.jar

ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.data.WriteClickRecordToTiKV"]
