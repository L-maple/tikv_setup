#! /bin/sh

# 删除示范应用部署的deployment
recommendation_deployment_name=`kubectl get deploy -n tidb-cluster | grep recommendation_deployment | awk '{print $1}'`
if [[ $recommendation_deployment_name != "" ]]
then
    kubectl delete deploy $recommendation_deployment_name -n tidb-cluster
fi
echo "recommendation_deployment has been deleted..."

echo "Success!"

