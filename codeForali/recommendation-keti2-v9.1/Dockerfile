FROM openjdk:8-jre-alpine

COPY target/recommendation-keti2-1.0-SNAPSHOT.jar /tikv-test-app.jar

<<<<<<< HEAD
ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.LoadDataToTiKV", "Leo_click"]
=======
ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.data.WriteToKafka", "1", "100", "100000"]
>>>>>>> 080bc189714b48509cca9350d9babcff9ce76a82
