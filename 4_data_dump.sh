#! /bin/sh

codeDirName="recommendation-keti2-v9.1/"
alterShellFileName="alter_mainclass.py"

echo "cp docker_generator.py to recommendation-keti2..."
cp $alterShellFileName $codeDirName
echo "docker_generator.py has been removed..."

echo "cd into recommendation-keti2..."
cd $codeDirName

# generate the CreateItemDataByTiKV docker && apply yaml
object="cn.edu.neu.tiger.tikv.data.CreateItemDataByTiKV"
echo "STEP1..."
echo "executing the alter_mainclass.py..." 
python $alterShellFileName $object
echo "execute mvn to generate jar package..."
mvn clean install
echo "jar package has been generated in target/..."
stat target
echo "================================="
echo "cat dockerfilecontent to Dockerfile..."
cat > Dockerfile <<EOF 
FROM openjdk:8-jre-alpine

COPY target/recommendation-keti2-1.0-SNAPSHOT.jar /tikv-test-app.jar

ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.data.CreateItemDataByTiKV"]
EOF
echo "generate docker image..."
docker build -f Dockerfile -t aliuchangjie/recommendation-tikv-loaditem .
echo "the recommendation-tikv-loaditem has been created successfully!!!"
echo "kubectl apply the tikv-loaditem-job.yaml..."
isNamespaceExist=`kubectl get namespace | grep tidb-cluster`
if [[ $isNamespaceExist == "" ]]
then
    kubectl create namespace tidb-cluster
fi
kubectl apply -f tikv-loaditem-job.yaml
echo "+++++++++++++++++++++++++++++++++"

# generate the CreateItemDataByTiKV docker && apply yaml
object="cn.edu.neu.tiger.tikv.data.CreateUserDataByTiKV"
echo "STEP2..."
echo "executing the alter_mainclass.py..." 
python $alterShellFileName $object
echo "execute mvn to generate jar package..."
mvn clean install
echo "jar package has been generated in target/..."
stat target
echo "================================="
echo "cat dockerfilecontent to Dockerfile..."
cat > Dockerfile <<EOF 
FROM openjdk:8-jre-alpine

COPY target/recommendation-keti2-1.0-SNAPSHOT.jar /tikv-test-app.jar

ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.data.CreateUserDataByTiKV"]
EOF
echo "generate docker image..."
docker build -f Dockerfile -t aliuchangjie/recommendation-tikv-loaduser .
echo "the recommendation-tikv-loaduser has been created successfully!!!"
echo "kubectl apply the tikv-loaduser-job.yaml..."
isNamespaceExist=`kubectl get namespace | grep tidb-cluster`
if [[ $isNamespaceExist == "" ]]
then
    kubectl create namespace tidb-cluster
fi
kubectl apply -f tikv-loaduser-job.yaml
echo "+++++++++++++++++++++++++++++++++"

# generate the CreateItemDataByTiKV docker && apply yaml
object="cn.edu.neu.tiger.tikv.data.WriteClickRecordToTiKV"
echo "STEP3..."
echo "executing the alter_mainclass.py..." 
python $alterShellFileName $object
echo "execute mvn to generate jar package..."
mvn clean install
echo "jar package has been generated in target/..."
stat target
echo "================================="
echo "cat dockerfilecontent to Dockerfile..."
cat > Dockerfile <<EOF 
FROM openjdk:8-jre-alpine

COPY target/recommendation-keti2-1.0-SNAPSHOT.jar /tikv-test-app.jar

ENTRYPOINT ["java","-cp","/tikv-test-app.jar","cn.edu.neu.tiger.tikv.data.WriteClickRecordToTiKV"]
EOF
echo "generate docker image..."
docker build -f Dockerfile -t aliuchangjie/recommendation-tikv-loadclick .
echo "the recommendation-tikv-loadclick has been created successfully!!!"
echo "kubectl apply the tikv-loadclick-job.yaml..."
isNamespaceExist=`kubectl get namespace | grep tidb-cluster`
if [[ $isNamespaceExist == "" ]]
then
    kubectl create namespace tidb-cluster
fi
kubectl apply -f tikv-loadclick-job.yaml
echo "+++++++++++++++++++++++++++++++++"

# delete the alter_mainclass.py
rm -f $alterShellFileName
