### 1_java_setup.sh 
`function:` install java environment;

`prerequisites:` jdk-8u261-linux-x64.tar.gz

`load from localhost:` rz -be choose jdk-8u261-linux-x64.tar.gz

### 2_mvn_setup.sh 
`function:` install maven environment;

`prerequisites:` apache-maven-3.6.3-bin.tar.gz

`load from localhost:` rz -be choose apache-maven-3.6.3-bin.tar.gz

### 3_deps_install.sh 
`function:` install project's dependencies;

### 4_data_dump.sh
`function:` dump user/item/click data to tikv;

`check:` see grafana;

`note:` data size in (CreateItemDataByTiKV, CreateUserDataByTiKV, WriteClickRecordToTiKV) should be altered as wanted;

### 5_application_test.sh
`function:` deploy the recommendation_application and kafka-producer

`prerequisites:` jdk-8u131-linux-x64.tar.gz, flink/

`check:` kubectl get deploy -n tidb-cluster

`note:` (1) parallism can be altered in the dockerfile string;
