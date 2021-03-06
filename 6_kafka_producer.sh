echo "set mainClass to WriteToKafka and recompile..."
projectDir=recommendation-keti2-v9.1/
cp alter_mainclass.py $projectDir
cd $projectDir
#! /bin/sh

python alter_mainclass.py cn.edu.neu.tiger.tikv.data.WriteToKafka
mvn clean install
echo "the project has been recompiled..."
echo "build the docker kafka image..."
echo "cat dockerfilecontent to Dockerfile..."
cat > Dockerfile <<EOF 
FROM openjdk:8-jre-alpine

COPY target/recommendation-keti2-1.0-SNAPSHOT.jar /tikv-test-app.jar

ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.data.WriteToKafka", "1", "100", "100000"]
EOF

docker build -f Dockerfile -t aliuchangjie/recommendation-kafka .
echo "the kafka image has been build..."
kubectl delete -n tidb-cluster deploy kafka-producer-deployment
kubectl apply -f kafka-producer-deployment.yaml
echo "the kafka-producer-job has been created..."
