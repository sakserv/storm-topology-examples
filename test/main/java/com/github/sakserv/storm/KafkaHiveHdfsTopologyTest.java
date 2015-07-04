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
import com.github.sakserv.storm.config.HiveConfig;
import com.github.sakserv.storm.config.StormConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaHiveHdfsTopologyTest {
    
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHiveHdfsTopologyTest.class);
    
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
    public void setUp() throws Exception {

        // Parse the properties file
        propertyParser = new PropertyParser();
        propertyParser.parsePropsFile(PROP_FILE);
        
        // Start Zookeeper
        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        zookeeperLocalCluster.start();

        // Start HDFS
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsConfig(new Configuration())
                .build();
        hdfsLocalCluster.start();

        // Start HiveMetaStore
        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(HiveConfig.buildHiveConf())
                .build();
        hiveLocalMetaStore.start();
        
        
        hiveLocalServer2 = new HiveLocalServer2.Builder()
                .setHiveServer2Hostname(propertyParser.getProperty(ConfigVars.HIVE_SERVER2_HOSTNAME_KEY))
                .setHiveServer2Port(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_SERVER2_PORT_KEY)))
                .setHiveMetastoreHostname(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)))
                .setHiveMetastoreDerbyDbDir(propertyParser.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(propertyParser.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(propertyParser.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(HiveConfig.buildHiveConf())
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        hiveLocalServer2.start();
        
        // Start Kafka
        kafkaLocalBroker = new KafkaLocalBroker.Builder()
                .setKafkaHostname(propertyParser.getProperty(ConfigVars.KAFKA_HOSTNAME_KEY))
                .setKafkaPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)))
                .setKafkaBrokerId(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_ID_KEY)))
                .setKafkaProperties(new Properties())
                .setKafkaTempDir(propertyParser.getProperty(ConfigVars.KAFKA_TEST_TEMP_DIR_KEY))
                .setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();
        kafkaLocalBroker.start();

        // Start Storm
        stormLocalCluster = new StormLocalCluster.Builder()
                .setZookeeperHost(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOSTNAME_KEY))
                .setZookeeperPort(Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setEnableDebug(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)))
                .setNumWorkers(Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)))
                .setStormConfig(new Config())
                .build();
        stormLocalCluster.start();
    }

    @After
    public void tearDown() throws Exception {

        // Stop Storm
        try {
            stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY));
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
        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveLocalMetaStore.getHiveConf());

        LOG.info("HIVE: Dropping hive table: " + propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY));
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

    public void validateHiveResults() throws ClassNotFoundException, SQLException {
        LOG.info("HIVE: VALIDATING");
        // Load the Hive JDBC driver
        LOG.info("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection con = DriverManager.getConnection("jdbc:hive2://" + 
                hiveLocalServer2.getHiveServer2Hostname() + ":" + hiveLocalServer2.getHiveServer2Port() + "/" +
                propertyParser.getProperty(ConfigVars.HIVE_BOLT_DATABASE_KEY), "user", "pass");

        String selectStmt = "SELECT * FROM " + propertyParser.getProperty(ConfigVars.HIVE_BOLT_TABLE_KEY);
        //String selectStmt = "SHOW TABLES";
        Statement stmt = con.createStatement();

        LOG.info("HIVE: Running Select Statement: " + selectStmt);
        ResultSet resultSet = stmt.executeQuery(selectStmt);
        int count = 0;
        while (resultSet.next()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println();
            count++;
        }
        // Validate the number of rows matches the number of kafka messages
        int msgCountLow = Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)) - 2;
        int msgCountHigh = Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)) + 2;

        assertTrue(msgCountLow <= count);
        assertTrue(msgCountHigh >= count);
    }

    public void createTmpHive() throws Exception {
        String sessionPath = propertyParser.getProperty(ConfigVars.HIVE_TEST_HDFS_SESSION_PATH_KEY);
        LOG.info("HDFS: Creating " + sessionPath + " locally and on HDFS");
        
        // Get the filesystem handle and a list of files written by the test
        FileSystem hdfsFsHandle = hdfsLocalCluster.getHdfsFileSystemHandle();

        FsPermission fsPerms = new FsPermission("777");
        hdfsFsHandle.mkdirs(new Path(sessionPath), fsPerms);
        hdfsFsHandle.setOwner(new Path(sessionPath), "hive", "hadoop");
        hdfsFsHandle.setPermission(new Path(sessionPath), fsPerms);

        FileStatus fileStatus = hdfsFsHandle.getFileStatus(new Path(sessionPath));
        LOG.info("HDFS: FILESTATUS: " + fileStatus.toString());

        hdfsFsHandle.close();
        
/*        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);
        perms.add(PosixFilePermission.OTHERS_READ);
        perms.add(PosixFilePermission.OTHERS_WRITE);
        perms.add(PosixFilePermission.OTHERS_EXECUTE);
        Files.setPosixFilePermissions(Paths.get(sessionPath), perms);*/
    }

    public void validateHdfsResults() throws Exception {
        LOG.info("HDFS: VALIDATING");
        
        // Get the filesystem handle and a list of files written by the test
        FileSystem hdfsFsHandle = hdfsLocalCluster.getHdfsFileSystemHandle();
        RemoteIterator<LocatedFileStatus> listFiles = hdfsFsHandle.listFiles(
                new Path(propertyParser.getProperty(ConfigVars.HDFS_BOLT_OUTPUT_LOCATION_KEY)), true);

        // Loop through the files and count up the lines
        int count = 0;
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();

            LOG.info("HDFS READ: Found File: " + file);

            BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFsHandle.open(file.getPath())));
            String line = br.readLine();
            while (line != null) {
                LOG.info("HDFS READ: Found Line: " + line);
                line = br.readLine();
                count++;
            }
        }
        hdfsFsHandle.close();

        // Validate the number of lines matches the number of kafka messages
        assertEquals(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)), count);
    }

    public void runStormKafkaHiveHdfsTopology() throws IOException {
        LOG.info("STORM: Starting Topology: " + propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY));
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
                propertyParser.getProperty(ConfigVars.HDFS_BOLT_DFS_URI_KEY),
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
                Integer.parseInt(propertyParser.getProperty(ConfigVars.HIVE_BOLT_HEARTBEAT_INTERVAL_KEY)));


        // Storm Topology Config
        Config stormConfig = StormConfig.createStormConfig(
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)));

        // Submit the topology
        stormLocalCluster.submitTopology(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY), new Config(), builder.createTopology());
    }

    @Test
    public void testKafkaHiveHdfsTopology() throws Exception {
        
        // Need to manually create /tmp/hive locally and on HDFS for hive session state data
        createTmpHive();

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
        stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY));
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
