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

import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.storm.bolt.SimpleMongoBolt;
import org.apache.log4j.Logger;

public class ConfigureMongodbBolt {

    private static final Logger LOG = Logger.getLogger(ConfigureMongodbBolt.class);

    public static void configureMongodbBolt(TopologyBuilder builder, String mongodbHost, int mongodbPort, String mongodbDB, String mongodbCollection) {
        
        LOG.info("HDFS: Configuring the MongoBolt");
        SimpleMongoBolt bolt = new SimpleMongoBolt(mongodbHost, mongodbPort, mongodbDB, mongodbCollection);
        builder.setBolt("mongobolt", bolt, 1).shuffleGrouping("kafkaspout");

    }
    
}
