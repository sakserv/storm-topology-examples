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

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

public class ConfigureKafkaSpout {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigureKafkaSpout.class);

    public static void configureKafkaSpout(TopologyBuilder builder, String zkHostString, String kafkaTopic, 
                                           String kafkaStartOffset, int parallelismHint, String spoutName,
                                           String spoutScheme) {

        LOG.info("KAFKASPOUT: Configuring the KafkaSpout");

        // Configure the KafkaSpout
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zkHostString),
                kafkaTopic,      // Kafka topic to read from
                "/" + kafkaTopic, // Root path in Zookeeper for the spout to store consumer offsets
                UUID.randomUUID().toString());  // ID for storing consumer offsets in Zookeeper
        try {
            spoutConfig.scheme = new SchemeAsMultiScheme(getSchemeFromClassName(spoutScheme));
        } catch(Exception e) {
            LOG.error("ERROR: Unable to create instance of scheme: " + spoutScheme);
            e.printStackTrace();
        }
        setKafkaOffset(spoutConfig, kafkaStartOffset);
        
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        // Add the spout and bolt to the topology
        builder.setSpout(spoutName, kafkaSpout, parallelismHint);

    }
    
    private static void setKafkaOffset(SpoutConfig spoutConfig, String kafkaStartOffset) {
        // Allow for passing in an offset time
        // startOffsetTime has a bug that ignores the special -2 value
        if(kafkaStartOffset.equals("-2")) {
            spoutConfig.forceFromStart = true;
        } else if (kafkaStartOffset != null) {
            spoutConfig.startOffsetTime = Long.parseLong(kafkaStartOffset);
        }
        
    }
    
    private static Scheme getSchemeFromClassName(String spoutSchemeCls) throws Exception {
        return (Scheme)Class.forName(spoutSchemeCls).getConstructor().newInstance();
    }
}
