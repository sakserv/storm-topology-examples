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
import backtype.storm.tuple.Fields;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigureHiveBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigureHiveBolt.class);

    public static void configureHiveStreamingBolt(TopologyBuilder builder, 
                                                  String colNames, 
                                                  String partitionCol,
                                                  String columnPartitionStringDelimiter,
                                                  String metastoreUri, 
                                                  String dbName, 
                                                  String tableName,
                                                  String hiveBoltName, 
                                                  String spoutName,
                                                  int parallelismHint,
                                                  boolean autoCreatePartitions,
                                                  int txnsPerBatch,
                                                  int maxOpenConnections,
                                                  int batchSize,
                                                  int idleTimeout,
                                                  int heartBeatInterval) {

        LOG.info("HIVEBOLT: Configuring the HiveBolt");

        // Define the record mapper
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(stringToStringArray(colNames, columnPartitionStringDelimiter)))
                .withPartitionFields(new Fields(stringToStringArray(partitionCol, columnPartitionStringDelimiter)));
        
        // Defind the Hive options
        HiveOptions hiveOptions = new HiveOptions(metastoreUri, dbName, tableName, mapper)
                .withAutoCreatePartitions(autoCreatePartitions)
                .withTxnsPerBatch(txnsPerBatch)
                .withMaxOpenConnections(maxOpenConnections)
                .withBatchSize(batchSize)
                .withIdleTimeout(idleTimeout)
                .withHeartBeatInterval(heartBeatInterval);
        HiveBolt bolt = new HiveBolt(hiveOptions);
        
        // Set the bolt
        builder.setBolt(hiveBoltName, bolt, parallelismHint).shuffleGrouping(spoutName);

    }
    
    private static String[] stringToStringArray(String stringToParse, String delimiter) {
        return stringToParse.split(delimiter);
    }
}
