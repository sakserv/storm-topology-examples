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


public class KafkaHiveTopology {

    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            System.out.println("USAGE: storm jar </path/to/topo.jar> <com.package.TopologyMainClass> " +
                    "<topo_display_name> <zookeeper_host:port[,zookeeper_host:port]> " +
                    "<kafka_topic_name> <offset_time_to_start_from> <hivecol1,[hivecol2]> " +
                    "<hivepartition1,[hivepartiton2]> <metastoreUri> <hivedb> <hivetable>");
            System.exit(1);
        }

        TopologyBuilder builder = new TopologyBuilder();

        // Setup the Kafka Spout
        ConfigureKafkaSpout.configureKafkaSpout(builder, args[1], args[2], args[3]);

        // Setup the Hive Bolt
        String[] cols = args[4].split(",");
        String[] parts = {args[5]};
        ConfigureHiveBolt.configureHiveStreamingBolt(builder, cols, parts, args[6], args[7], args[8]);

        // Topology
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);

        // Submit the topology
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

    }
}
