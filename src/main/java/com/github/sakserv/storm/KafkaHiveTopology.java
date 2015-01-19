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
import com.github.sakserv.storm.scheme.JsonScheme;
import org.apache.hadoop.hdfs.DFSClient;


public class KafkaHiveTopology {

    public static void main(String[] args) throws Exception {

        //TODO: Get rid of args and hardcoded properties file

        if (args.length < 2) {
            System.out.println("USAGE: storm jar </path/to/topo.jar> <com.package.TopologyMainClass> " +
                    "<topo_display_name> </path/to/config.properties>");
            System.exit(1);
        }
        String stormTopologyName = args[0];
        String propFilePath = args[1];

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

        // Setup the Hive Bolt
        // Configure the HiveBolt
        ConfigureHiveBolt.configureHiveStreamingBolt(builder,
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_COLUMN_LIST_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_PARTITION_LIST_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_COLUMN_PARTITION_LIST_DELIMITER_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_METASTORE_URI_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_DATABASE_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_PARALLELISM_KEY)),
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HIVE_BOLT_AUTO_CREATE_PARTITIONS_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_TXNS_PER_BATCH_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_MAX_OPEN_CONNECTIONS_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_BATCH_SIZE_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_IDLE_TIMEOUT_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_HEARTBEAT_INTERVAL)));

        // Storm Topology Config
        Config stormConfig = StormConfig.createStormConfig(
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS)));

        // Submit the topology
        StormSubmitter.submitTopologyWithProgressBar(stormTopologyName, stormConfig, builder.createTopology());

    }
}
