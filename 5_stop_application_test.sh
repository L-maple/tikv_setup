#! /bin/sh

# 删除示范应用部署的deployment
recommendation_deployment_name=`kubectl get deploy -n tidb-cluster | grep recommendation_deployment | awk '{print $1}'`
if [[ $recommendation_deployment_name != "" ]]
then
    kubectl delete deploy $recommendation_deployment_name -n tidb-cluster
fi

# 删除kafka producer的deployment
kafka_producer_name=`kubectl get deploy -n tidb-cluster | grep kafka-producer-deployment | awk '{print $1}'`
if [[ $kafka_producer_name != "" ]]
then
    kubectl delete deploy $kafka_producer_name -n tidb-cluster
fi


