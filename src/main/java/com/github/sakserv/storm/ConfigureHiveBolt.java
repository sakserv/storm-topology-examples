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

public class ConfigureHiveBolt {

    public static void configureHiveStreamingBolt(TopologyBuilder builder, String[] colNames, String[] partitionCol, String metastoreUri, String dbName, String tableName) {

        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partitionCol));
        HiveOptions hiveOptions = new HiveOptions(metastoreUri, dbName, tableName, mapper)
                .withAutoCreatePartitions(true)
                .withTxnsPerBatch(100)
                .withMaxOpenConnections(100)
                .withBatchSize(1000)
                .withIdleTimeout(3600)
                .withHeartBeatInterval(240);
        HiveBolt bolt = new HiveBolt(hiveOptions);
        builder.setBolt("hivebolt", bolt, 1).shuffleGrouping("kafkaspout");

    }
}
