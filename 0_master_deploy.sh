#! /bin/sh

# install the helm


# deploy the local-volume-provisioner.yaml
echo "deploy the provisioner in the cluster..."
srcDirName=tikv_deploy
kubectl apply -f $srcDirName/local-volume-provisioner.yaml
kubectl get po -n kube-system -o wide -l app=local-volume-provisioner
echo "provisioners have been deployed in the cluster..."

# deploy the tikv cluster




