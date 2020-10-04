#! /bin/sh

# 删除示范应用部署的deployment
recommendation_deployment_name=`kubectl get deploy -n tidb-cluster | grep recommendation_deployment* | awk '{print $1}'`
kubectl delete deploy $recommendation_deployment_name -n tidb-cluster

# 删除kafka producer的deployment
kafka_producer_name=`kubectl get deploy -n tidb-cluster | grep *producer* | awk '{print $1}'`
kubectl delete deploy $kafka_producer_name -n tidb-cluster


