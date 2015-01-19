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
import com.github.sakserv.config.ConfigVars;
import com.github.sakserv.config.PropertyParser;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import org.apache.log4j.Logger;

import java.io.IOException;

public class ConfigureHdfsBolt {

    private static final Logger LOG = Logger.getLogger(ConfigureHdfsBolt.class);

    public static void configureHdfsBolt(TopologyBuilder builder, 
                                         String delimiter, 
                                         String outputPath, 
                                         String hdfsUri,
                                         String hdfsBoltName, 
                                         String spoutName,
                                         int parallelismHint,
                                         FileRotationPolicy rotationPolicy,
                                         int syncCount) {
        
        LOG.info("HDFSBOLT: Configuring the HdfsBolt");
        
        // Define the RecordFormat, SyncPolicy, and FileNameFormat
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(delimiter);
        SyncPolicy syncPolicy = new CountSyncPolicy(syncCount);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(outputPath);
        
        // Configure the Bolt
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(hdfsUri)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        
        // Set the Bolt
        builder.setBolt(hdfsBoltName, bolt, parallelismHint).shuffleGrouping(spoutName);

    }
    
    public static FileRotationPolicy configureFileRotationPolicy(String propsFileName) throws IOException {
        // Parse the properties file
        PropertyParser propertyParser = new PropertyParser();
        propertyParser.parsePropsFile(propsFileName);
        
        // Get the config value to determine which FileRotationPolicy is enabled
        boolean useTimeBasedFileRotationPolicy = 
                Boolean.parseBoolean(propertyParser.getProperty(
                        ConfigVars.HDFS_BOLT_USE_TIME_BASED_FILEROTATIONPOLICY_KEY));

        boolean useSizeBasedFileRotationPolicy =
                Boolean.parseBoolean(propertyParser.getProperty(
                        ConfigVars.HDFS_BOLT_USE_SIZE_BASED_FILEROTATIONPOLICY_KEY));
        
        // Guard against both size and time based policies being enabled
        if (useSizeBasedFileRotationPolicy && useTimeBasedFileRotationPolicy) {
            LOG.error("ERROR: You cannot use both time and size based rotation policies");
            LOG.error("ERROR: Validate that either hdfs.bolt.use.time.based.filerotationpolicy" +
                    " or hdfs.bolt.use.size.based.filerotationpolicy is false");
            throw new IllegalArgumentException("ERROR: You cannot use both time and size based rotation policies");
        }
        
        
        // Return the appropriate FileRotationPolicy based on the config
        if (useSizeBasedFileRotationPolicy) {
            return getSizeBasedFileRotationPolicy(
                    propertyParser.getProperty(ConfigVars.HDFS_BOLT_USE_SIZE_BASED_FILEROTATIONPOLICY_UNIT_KEY),
                    Integer.parseInt(propertyParser.getProperty(
                            ConfigVars.HDFS_BOLT_USE_SIZE_BASED_FILEROTATIONPOLICY_SIZE_KEY)));
        } else {
            return getTimeBasedFileRotationPolicy(
                    propertyParser.getProperty(ConfigVars.HDFS_BOLT_USE_TIME_BASED_FILEROTATIONPOLICY_UNIT_KEY),
                    Integer.parseInt(propertyParser.getProperty(
                            ConfigVars.HDFS_BOLT_USE_TIME_BASED_FILEROTATIONPOLICY_DURATION_KEY)));
        }
        
    }
    
    private static FileRotationPolicy getSizeBasedFileRotationPolicy(String unitsConfigured, int sizeConfigured) {
        FileSizeRotationPolicy.Units units;
        if (unitsConfigured.toUpperCase().equals("KB")) {
            units = FileSizeRotationPolicy.Units.KB;
        } else if (unitsConfigured.toUpperCase().equals("MB")) {
            units = FileSizeRotationPolicy.Units.MB;
        } else if (unitsConfigured.toUpperCase().equals("GB")) {
            units = FileSizeRotationPolicy.Units.GB;
        } else if (unitsConfigured.toUpperCase().equals("TB")) {
            units = FileSizeRotationPolicy.Units.TB;
        } else {
            units = FileSizeRotationPolicy.Units.MB;
        }

        return new FileSizeRotationPolicy(sizeConfigured, units);
    }

    private static FileRotationPolicy getTimeBasedFileRotationPolicy(String unitsConfigured, int timeConfigured) {
        TimedRotationPolicy.TimeUnit units;
        if (unitsConfigured.toUpperCase().equals("SECONDS")) {
            units = TimedRotationPolicy.TimeUnit.SECONDS;
        } else if (unitsConfigured.toUpperCase().equals("MINUTES")) {
            units = TimedRotationPolicy.TimeUnit.MINUTES;
        } else if (unitsConfigured.toUpperCase().equals("HOURS")) {
            units = TimedRotationPolicy.TimeUnit.HOURS;
        } else if (unitsConfigured.toUpperCase().equals("DAYS")) {
            units = TimedRotationPolicy.TimeUnit.DAYS;
        } else {
            units = TimedRotationPolicy.TimeUnit.MINUTES;
        }

        return new TimedRotationPolicy(timeConfigured, units);
    }
    
}
