#!/bin/bash

# Install Maven
echo -e "\n#### Installing Maven"
cd /tmp
wget http://www.gtlib.gatech.edu/pub/apache/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
tar -xzvf apache-maven-3.2.5-bin.tar.gz
export M2_HOME=/tmp/apache-maven-3.2.5/
export M2=$M2_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64
export PATH=$PATH:$M2:$JAVA_HOME/bin
mvn --version

# Build storm-topology-examples
echo -e "\n#### Building storm-topology-examples"
cd /tmp
git clone https://github.com/sakserv/storm-topology-examples.git
cd /tmp/storm-topology-examples/ && mvn clean package install

# Install MongoDB
echo -e "\n#### Installing and starting mongodb"
cd /tmp/storm-topology-examples/
cp bin/yum/mongodb.repo /etc/yum.repos.d/
yum install mongodb-org -y
service mongod start

# Create the Kafka Topic
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --topic test_topic --zookeeper localhost:2181 --partitions 1 --replication-factor 1

# Create the Hive Table
cd /tmp/storm-topology-examples/
hive -f bin/hive/create_orc_table.sql