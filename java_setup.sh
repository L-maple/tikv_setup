#! /bin/sh

echo "begin to set up the java environment..."
rm -rf /usr/local/java
mkdir /usr/local/java
tar -zxvf jdk-8u261-linux-x64.tar.gz -C /usr/local/java
echo "export JAVA_HOME=/usr/local/java/" >> /etc/profile
echo "export JRE_HOME=${JAVA_HOME}/jre" >> /etc/profile
echo "export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib" >> /etc/profile
echo "export PATH=${JAVA_HOME}/bin:$PATH" >> /etc/profile

