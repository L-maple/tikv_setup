#! /bin/sh

#################################
echo "install the maven on the k8s master..."
tar -zxvf apache-maven-3.6.3-bin.tar.gz
rm -rf /usr/local/maven
mkdir /usr/local/maven
mv apache-maven-3.6.3 /usr/local/maven 

is_maven_exist=`cat /etc/profile | grep maven`
if [[ $is_maven_exist == "" ]]
then 
    echo "MAVEN_HOME=/usr/local/maven/apache-maven-3.6.3" >> /etc/profile
    echo 'export PATH=${MAVEN_HOME}/bin:${PATH}' >> /etc/profile
    source /etc/profile
fi
mvn -v
echo "the maven has been installed."
echo "请配置/usr/local/maven/apache-maven-3.6.3/conf/settings.xml文件"
#################################
