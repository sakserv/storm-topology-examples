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
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.config.ConfigVars;
import com.github.sakserv.config.PropertyParser;
import com.github.sakserv.kafka.KafkaProducerTest;
import com.github.sakserv.minicluster.impl.*;
import com.github.sakserv.minicluster.util.FileUtils;
import com.github.sakserv.storm.config.StormConfig;
import com.github.sakserv.storm.scheme.JsonScheme;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaHiveHdfsTopologyTest {
    
    // Logger
    private static final Logger LOG = Logger.getLogger(KafkaHiveHdfsTopologyTest.class);

    // Properties file for tests
    private PropertyParser propertyParser;
    private static final String PROP_FILE = "local.properties";

    private ZookeeperLocalCluster zookeeperLocalCluster;
    private KafkaLocalBroker kafkaLocalBroker;
    private StormLocalCluster stormLocalCluster;
    private HdfsLocalCluster hdfsLocalCluster;
    private HiveLocalMetaStore hiveLocalMetaStore;
    private HiveLocalServer2 hiveLocalServer2;

    @Before
    public void setUp() throws IOException {

        // Parse the properties file
        propertyParser = new PropertyParser();
        propertyParser.parsePropsFile(PROP_FILE);

        // Start Zookeeper
        zookeeperLocalCluster = new ZookeeperLocalCluster(
                Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)),
                propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY));
        zookeeperLocalCluster.start();

        // Start HDFS
        hdfsLocalCluster = new HdfsLocalCluster();
        hdfsLocalCluster.start();

        // Start HiveMetaStore
        hiveLocalMetaStore = new HiveLocalMetaStore();
        hiveLocalMetaStore.start();

        // Start HiveServer2
        hiveLocalServer2 = new HiveLocalServer2();
        hiveLocalServer2.start();
        
        // Start Kafka
        kafkaLocalBroker = new KafkaLocalBroker(propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TEST_TEMP_DIR_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_ID_KEY)),
                propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY));
        kafkaLocalBroker.start();

        // Start Storm
        stormLocalCluster = new StormLocalCluster(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOSTS_KEY),
                Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)));
        stormLocalCluster.start();
    }

    @After
    public void tearDown() {

        // Stop Storm
        try {
            stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME));
        } catch(IllegalStateException e) { }

        // Stop Kafka
        kafkaLocalBroker.stop(true);

        // Stop HiveMetaStore
        hiveLocalMetaStore.stop();

        // Stop HiveServer2
        hiveLocalServer2.stop(true);
        FileUtils.deleteFolder(new File(propertyParser.getProperty(
                ConfigVars.HIVE_TEST_TABLE_LOCATION_KEY)).getAbsolutePath());

        // Stop HDFS
        hdfsLocalCluster.stop(true);

        // Stop ZK
        zookeeperLocalCluster.stop(true);
    }

    public void createTable() throws TException {
        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveLocalMetaStore.getConf());

        hiveClient.dropTable(propertyParser.getProperty(ConfigVars.HIVE_BOLT_DATABASE_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY),
                true, true);

        // Define the cols
        List<FieldSchema> cols = new ArrayList<FieldSchema>();
        cols.add(new FieldSchema("id", Constants.INT_TYPE_NAME, ""));
        cols.add(new FieldSchema("msg", Constants.STRING_TYPE_NAME, ""));

        // Values for the StorageDescriptor
        String location = new File(propertyParser.getProperty(
                ConfigVars.HIVE_TEST_TABLE_LOCATION_KEY)).getAbsolutePath();
        String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
        String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
        int numBuckets = 16;
        Map<String,String> orcProps = new HashMap<String, String>();
        orcProps.put("orc.compress", "NONE");
        SerDeInfo serDeInfo = new SerDeInfo(OrcSerde.class.getSimpleName(), OrcSerde.class.getName(), orcProps);
        List<String> bucketCols = new ArrayList<String>();
        bucketCols.add("id");

        // Build the StorageDescriptor
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(cols);
        sd.setLocation(location);
        sd.setInputFormat(inputFormat);
        sd.setOutputFormat(outputFormat);
        sd.setNumBuckets(numBuckets);
        sd.setSerdeInfo(serDeInfo);
        sd.setBucketCols(bucketCols);
        sd.setSortCols(new ArrayList<Order>());
        sd.setParameters(new HashMap<String, String>());

        // Define the table
        Table tbl = new Table();
        tbl.setDbName(propertyParser.getProperty(ConfigVars.HIVE_BOLT_DATABASE_KEY));
        tbl.setTableName(propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY));
        tbl.setSd(sd);
        tbl.setOwner(System.getProperty("user.name"));
        tbl.setParameters(new HashMap<String, String>());
        tbl.setViewOriginalText("");
        tbl.setViewExpandedText("");
        tbl.setTableType(TableType.MANAGED_TABLE.name());
        List<FieldSchema> partitions = new ArrayList<FieldSchema>();
        partitions.add(new FieldSchema("dt", Constants.STRING_TYPE_NAME, ""));
        tbl.setPartitionKeys(partitions);

        // Create the table
        hiveClient.createTable(tbl);

        // Describe the table
        Table createdTable = hiveClient.getTable(propertyParser.getProperty(ConfigVars.HIVE_BOLT_DATABASE_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY));
        LOG.info("HIVE: Created Table: " + createdTable.toString());
    }

    public void runStormKafkaHiveHdfsTopology() throws IOException {
        LOG.info("STORM: Starting Topology: " + propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME));
        TopologyBuilder builder = new TopologyBuilder();

        // Configure the KafkaSpout
        ConfigureKafkaSpout.configureKafkaSpout(builder,
                propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_START_OFFSET_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_PARALLELISM_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_SCHEME_CLASS_KEY));

        // Configure the HdfsBolt
        FileRotationPolicy fileRotationPolicy = ConfigureHdfsBolt.configureFileRotationPolicy(PROP_FILE);
        ConfigureHdfsBolt.configureHdfsBolt(builder,
                propertyParser.getProperty(ConfigVars.HDFS_BOLT_FIELD_DELIMITER_KEY),
                propertyParser.getProperty(ConfigVars.HDFS_BOLT_OUTPUT_LOCATION_KEY), 
                hdfsLocalCluster.getHdfsUriString(),
                propertyParser.getProperty(ConfigVars.HDFS_BOLT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_BOLT_PARALLELISM_KEY)),
                fileRotationPolicy,
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_BOLT_SYNC_COUNT_KEY)));
        
        // Configure the HiveBolt
        ConfigureHiveBolt.configureHiveStreamingBolt(builder,
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_COLUMN_LIST_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_PARTITION_LIST_KEY),
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_COLUMN_PARTITION_LIST_DELIMITER_KEY),
                hiveLocalMetaStore.getMetaStoreUri(),
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
        stormLocalCluster.submitTopology(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME), new Config(), builder.createTopology());
    }

    public void validateHiveResults() throws ClassNotFoundException, SQLException {
        LOG.info("HIVE: VALIDATING");
        // Load the Hive JDBC driver
        LOG.info("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveLocalServer2.getHiveServerThriftPort() + "/" + 
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY), "user", "pass");

        String selectStmt = "SELECT * FROM " + propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY);
        Statement stmt = con.createStatement();

        LOG.info("HIVE: Running Select Statement: " + selectStmt);
        ResultSet resultSet = stmt.executeQuery(selectStmt);
        while (resultSet.next()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println();
        }
    }

    public void validateHdfsResults() throws IOException {
        LOG.info("HDFS: VALIDATING");
        FileSystem hdfsFsHandle = hdfsLocalCluster.getHdfsFileSystemHandle();
        RemoteIterator<LocatedFileStatus> listFiles = hdfsFsHandle.listFiles(new Path("/tmp/kafka_data"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();

            LOG.info("HDFS READ: Found File: " + file);

            BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFsHandle.open(file.getPath())));
            String line = br.readLine();
            while (line != null) {
                LOG.info("HDFS READ: Found Line: " + line);
                line = br.readLine();
            }
        }
        hdfsFsHandle.close();
    }


    @Test
    public void testKafkaHiveHdfsTopology() throws TException, JSONException, ClassNotFoundException, SQLException, IOException {

        // Run the Kafka Producer
        KafkaProducerTest.produceMessages(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_LIST_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_PAYLOAD_KEY));
        
        // Create the Hive table
        createTable();
        
        // Run the Kafka Hive/HDFS topology and sleep 10 seconds to wait for completion
        runStormKafkaHiveHdfsTopology();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

        // To ensure transactions and files are closed, stop storm
        stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME));
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

        // Validate Hive table is populated
        validateHiveResults();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

        // Validate the HDFS files exist
        validateHdfsResults();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

    }
}
