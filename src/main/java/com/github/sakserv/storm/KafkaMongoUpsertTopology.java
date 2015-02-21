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
package com.github.sakserv.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.config.ConfigVars;
import com.github.sakserv.config.PropertyParser;
import com.github.sakserv.storm.config.StormConfig;

public class KafkaMongoUpsertTopology {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.out.println("USAGE: storm jar </path/to/topo.jar> <com.package.TopologyMainClass> " +
                    "</path/to/config.properties>");
            System.exit(1);
        }
        String propFilePath = args[0];

        // Parse the properties file
        PropertyParser propertyParser = new PropertyParser();
        propertyParser.parsePropsFile(propFilePath);

        TopologyBuilder builder = new TopologyBuilder();

        // Setup the Kafka Spout
        ConfigureKafkaSpout.configureKafkaSpout(builder,
                propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_START_OFFSET_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_PARALLELISM_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_SCHEME_CLASS_KEY));

        // Setup the Mongo Bolt
        // Configure the MongoBolt
        ConfigureMongodbBolt.configureMongodbUpsertBolt(builder,
                propertyParser.getProperty(ConfigVars.MONGO_IP_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)),
                propertyParser.getProperty(ConfigVars.MONGO_DATABASE_NAME_KEY),
                propertyParser.getProperty(ConfigVars.MONGO_COLLECTION_NAME_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_BOLT_PARALLELISM_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.MONGO_BOLT_NAME_KEY));


        // Storm Topology Config
        Config stormConfig = StormConfig.createStormConfig(
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)));

        // Submit the topology
        StormSubmitter.submitTopologyWithProgressBar(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY),
                stormConfig, builder.createTopology());

    }
}
