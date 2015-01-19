/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.config;

public class ConfigVars {
    
    // Zookeeper
    public static final String ZOOKEEPER_TEMP_DIR_KEY = "zookeeper.temp.dir";
    public static final String ZOOKEEPER_PORT_KEY = "zookeeper.port";
    public static final String ZOOKEEPER_HOSTS_KEY = "zookeeper.hosts";
    public static final String ZOOKEEPER_CONNECTION_STRING_KEY = "zookeeper.connection.string";
    
    // Kafka
    public static final String KAFKA_TOPIC_KEY = "kafka.topic";
    public static final String KAFKA_PORT_KEY = "kafka.port";
    
    // Kafka Spout
    public static final String KAFKA_SPOUT_START_OFFSET_KEY = "kafka.spout.start.offset";
    public static final String KAFKA_SPOUT_NAME_KEY = "kafka.spout.name";
    public static final String KAFKA_SPOUT_PARALLELISM_KEY = "kafka.spout.parallelism";
    public static final String KAFKA_SPOUT_SCHEME_CLASS_KEY = "kafka.spout.scheme.class";
    
    // Kafka Test
    public static final String KAFKA_TEST_TEMP_DIR_KEY = "kafka.temp.dir";
    public static final String KAFKA_TEST_BROKER_LIST_KEY = "kafka.broker.list";
    public static final String KAFKA_TEST_BROKER_ID_KEY = "kafka.broker.id";
    public static final String KAFKA_TEST_MSG_COUNT_KEY = "kafka.test.msg.count";
    public static final String KAFKA_TEST_MSG_PAYLOAD_KEY = "kafka.test.msg.payload";

    // MongoDB
    public static final String MONGO_IP_KEY = "mongo.ip";
    public static final String MONGO_PORT_KEY = "mongo.port";
    public static final String MONGO_DATABASE_NAME_KEY = "mongo.database.name";
    public static final String MONGO_COLLECTION_NAME_KEY = "mongo.collection.name";
    
    // MongoDB Bolt
    public static final String MONGO_BOLT_NAME_KEY = "mongo.bolt.name";
    public static final String MONGO_BOLT_PARALLELISM_KEY = "mongo.bolt.parallelism";
    
    // Storm
    public static final String STORM_TOPOLOGY_NAME = "storm.topology.name";
    
}
