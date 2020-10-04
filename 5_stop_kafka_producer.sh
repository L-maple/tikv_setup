# 删除kafka producer的deployment
kafka_producer_name=`kubectl get deploy -n tidb-cluster | grep kafka-producer-deployment | awk '{print $1}'`
if [[ $kafka_producer_name != "" ]]
then
    kubectl delete deploy $kafka_producer_name -n tidb-cluster
fi
echo "kafka-producer-deployment has been deleted..."

echo "Success!"

