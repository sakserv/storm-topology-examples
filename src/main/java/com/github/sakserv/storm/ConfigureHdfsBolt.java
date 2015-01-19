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
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import org.apache.log4j.Logger;

public class ConfigureHdfsBolt {

    private static final Logger LOG = Logger.getLogger(ConfigureHdfsBolt.class);

    public static void configureHdfsBolt(TopologyBuilder builder, String delimiter, String outputPath, String hdfsUri,
                                         String hdfsBoltName, String spoutName) {
        
        LOG.info("HDFS: Configuring the HdfsBolt");
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(delimiter);
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        //FileRotationPolicy rotationPolicy = new TimedRotationPolicy(300, TimedRotationPolicy.TimeUnit.SECONDS);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1, FileSizeRotationPolicy.Units.KB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputPath);
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(hdfsUri)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        builder.setBolt(hdfsBoltName, bolt, 1).shuffleGrouping(spoutName);

    }
    
}
