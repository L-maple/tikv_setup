#! /bin/sh

srcDirName=tikv_deploy

# install the helm
## install the helm client
tar -zxvf helm-v2.10.0-linux-amd64.tar.gz
cp linux-amd64/helm /usr/local/bin
rm -rf linux-amd64/
## install the helm server
kubectl apply -f $srcDirName/tiller-rbac.yaml

helm init --upgrade -i registry.cn-hangzhou.aliyuncs.com/google_containers/tiller:v2.10.0 --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts

isTillerExist=kubectl get pods -n kube-system | grep "tiller-deploy"
if [[ $isTillerExist != "" ]]
then
    echo "helm has been installed successfully."
fi

# deploy the local-volume-provisioner.yaml
echo "deploy the provisioner in the cluster..."
kubectl apply -f $srcDirName/local-volume-provisioner.yaml
kubectl get po -n kube-system -o wide -l app=local-volume-provisioner
echo "provisioners have been deployed in the cluster..."

# deploy the tikv cluster
echo "deploy the tidb cluster..."
## helm add pingcap chart
helm repo add pingcap https://charts.pingcap.org/
## deploy the tidb crd resources
kubectl apply -f $srcDirName/crd.yaml && kubectl get crd tidbclusters.pingcap.com
## deploy the tidb operator
kubectl create namespace tidb-cluster
helm install pingcap/tidb-operator --name=tidb-operator --namespace=tidb-cluster --version=v1.0.0 -f $srcDirName/values-tidb-operator.yaml
## check the result
kubectl get po -n tidb-admin -l app.kubernetes.io/name=tidb-operator
## deploy the tidb cluster
helm install pingcap/tidb-cluster --name=tidb-cluster --namespace=tidb-cluster --version=v1.0.0 -f $srcDirName/values-tidb-cluster.yaml
kubectl get po -n tidb-cluster -l app.kubernetes.io/instance=tidb-cluster
echo "tidb cluster has been deployed..."

echo "inspect the tidb cluster via grafana..."
kubectl get service -n tidb-cluster





















