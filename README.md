# storm-topology-examples

Overview:
---------
This project provides a collection of examples on using various Apache Storm topologies.

Includes:
---------
*  KafkaSpout
*  HdfsBolt
*  HiveBolt
*  MongoBolt

Dependencies:
-------------
The examples depend on the [hadoop-mini-clusters](https://github.com/sakserv/hadoop-mini-clusters) project, which is already added to the pom.

Setup:
------

* Clone the project
```
cd /tmp && git clone https://github.com/sakserv/storm-topology-examples.git
```

* Build the project
```
cd /tmp/storm-topology-examples && bash -x bin/build.sh
```

* If using the MongoBolt, install MongoDB
```
cd /tmp/storm-topology-examples.git && bash -x bin/install_mongo.sh
```

* If using the HiveBolt, create the table (you likely want to modify the ddl)
```
cd /tmp/storm-topology-examples.git && bash -x bin/create_orc_table.sh [/path/to/create_orc_table.sql]
```

* Create the Kafka topic (if auto creation of topics is disabled)
```
cd /tmp/mypipe-example && bash -x bin/create_kafka_topic.sh <topic_name>
```

* Copy the properties template and edit with the appropriate properties
```
cd /tmp/storm-topology-examples 
cp src/main/resources/sandbox_kafka_mongo.properties /tmp/foo.properties
vi /tmp/foo.properties
```

* Run the topology:

** KafkaHdfsTopology
```
cd /tmp/storm-topology-examples/target
storm jar storm-topology-examples-*.jar com.github.sakserv.storm.KafkaHdfsTopology /tmp/foo.properties
```

** KafkaHiveTopology
```
cd /tmp/storm-topology-examples/target
storm jar storm-topology-examples-*.jar com.github.sakserv.storm.KafkaHiveTopology /tmp/foo.properties
```

** KafkaMongoTopology
```
cd /tmp/storm-topology-examples/target
storm jar storm-topology-examples-*.jar com.github.sakserv.storm.KafkaMongoTopology /tmp/foo.properties
```
