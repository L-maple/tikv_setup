#! /bin/sh

echo "starting to setup the java environment..."

rm -rf /usr/local/java
mkdir /usr/local/java

tar -zxvf jdk-8u261-linux-x64.tar.gz -C /usr/local/java
isJavaExist=`cat /etc/profile | grep java`
if [[ $isJavaExist == "" ]]
then
    echo 'export JAVA_HOME=/usr/local/java/jdk-8u261' >> /etc/profile
    echo 'export JRE_HOME=${JAVA_HOME}/jre'           >> /etc/profile
    echo 'export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib' >> /etc/profile
    echo 'export PATH=${JAVA_HOME}/bin:$PATH'         >> /etc/profile
fi
source /etc/profile

ln -s /usr/local/java/jdk-8u261/bin/java /usr/bin/java
java -version

echo "java environment has set up."
