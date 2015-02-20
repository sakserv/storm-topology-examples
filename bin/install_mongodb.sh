#!/bin/bash

# Install Maven
echo -e "\n#### Installing Maven"
cd /tmp
wget -N http://www.gtlib.gatech.edu/pub/apache/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
tar -xzvf apache-maven-3.2.5-bin.tar.gz
export M2_HOME=/tmp/apache-maven-3.2.5/
export M2=$M2_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64
export PATH=$PATH:$M2:$JAVA_HOME/bin
mvn --version

# Install MongoDB
echo -e "\n#### Installing and starting mongodb"
cp /tmp/storm-topology-examples/bin/yum/mongodb.repo /etc/yum.repos.d/
yum install mongodb-org -y
service mongod start