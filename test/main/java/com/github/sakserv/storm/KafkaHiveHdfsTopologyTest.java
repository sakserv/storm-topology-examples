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
import com.github.sakserv.kafka.KafkaProducerTest;
import com.github.sakserv.minicluster.impl.*;
import com.github.sakserv.minicluster.util.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    // Kafka static
    private static final String DEFAULT_LOG_DIR = "embedded_kafka";
    private static final String TEST_TOPIC = "test-topic";
    private static final Integer KAFKA_PORT = 9092;
    private static final String LOCALHOST_BROKER = "localhost:" + KAFKA_PORT.toString();
    private static final Integer BROKER_ID = 1;

    // Storm static
    private static final String TEST_TOPOLOGY_NAME = "test";

    // Hive static
    private static final String HIVE_DB_NAME = "default";
    private static final String HIVE_TABLE_NAME = "test";
    private static final String[] HIVE_COLS = {"id", "msg"};
    private static final String[] HIVE_PARTITIONS = {"dt"};
    private static final String HIVE_TABLE_LOC = new File("test_table").getAbsolutePath();

    // HDFS static
    private static final String HDFS_OUTPUT_DIR = "/tmp/kafka_data";

    // Zookeeper
    private ZookeeperLocalCluster zkCluster;

    // Kafka
    private KafkaLocalBroker kafkaCluster;

    // Storm
    private StormLocalCluster stormCluster;

    // HDFS
    private HdfsLocalCluster hdfsCluster;

    // Hive MetaStore
    private HiveLocalMetaStore hiveLocalMetaStore;

    // HiveServer2
    private HiveLocalServer2 hiveLocalServer2;

    @Before
    public void setUp() {

        // Start ZK
        zkCluster = new ZookeeperLocalCluster();
        zkCluster.start();

        // Start HDFS
        hdfsCluster = new HdfsLocalCluster();
        hdfsCluster.start();

        // Start HiveMetaStore
        hiveLocalMetaStore = new HiveLocalMetaStore();
        hiveLocalMetaStore.start();

        hiveLocalServer2 = new HiveLocalServer2();
        hiveLocalServer2.start();

        // Start Kafka
        kafkaCluster = new KafkaLocalBroker(TEST_TOPIC, DEFAULT_LOG_DIR, KAFKA_PORT, BROKER_ID, zkCluster.getZkConnectionString());
        kafkaCluster.start();

        // Start Storm
        stormCluster = new StormLocalCluster(zkCluster.getZkHostName(), Long.parseLong(zkCluster.getZkPort()));
        stormCluster.start();
    }

    @After
    public void tearDown() {

        // Stop Storm
        try {
            stormCluster.stop(TEST_TOPOLOGY_NAME);
        } catch(IllegalStateException e) { }

        // Stop Kafka
        kafkaCluster.stop(true);

        // Stop HiveMetaStore
        hiveLocalMetaStore.stop();

        // Stop HiveServer2
        hiveLocalServer2.stop(true);
        FileUtils.deleteFolder(HIVE_TABLE_LOC);

        // Stop HDFS
        hdfsCluster.stop(true);

        // Stop ZK
        zkCluster.stop(true);
    }

    public void createTable() throws TException {
        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveLocalMetaStore.getConf());

        hiveClient.dropTable(HIVE_DB_NAME, HIVE_TABLE_NAME, true, true);

        // Define the cols
        List<FieldSchema> cols = new ArrayList<FieldSchema>();
        cols.add(new FieldSchema("id", Constants.INT_TYPE_NAME, ""));
        cols.add(new FieldSchema("msg", Constants.STRING_TYPE_NAME, ""));

        // Values for the StorageDescriptor
        String location = HIVE_TABLE_LOC;
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
        tbl.setDbName(HIVE_DB_NAME);
        tbl.setTableName(HIVE_TABLE_NAME);
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
        Table createdTable = hiveClient.getTable(HIVE_DB_NAME, HIVE_TABLE_NAME);
        System.out.println("HIVE: Created Table: " + createdTable.toString());
    }

    public void runStormKafkaHiveHdfsTopology() {
        System.out.println("STORM: Starting Topology: " + TEST_TOPOLOGY_NAME);
        TopologyBuilder builder = new TopologyBuilder();
        ConfigureKafkaSpout.configureKafkaSpout(builder, zkCluster.getZkConnectionString(), TEST_TOPIC, "-2");
        ConfigureHdfsBolt.configureHdfsBolt(builder, ",", HDFS_OUTPUT_DIR, hdfsCluster.getHdfsUriString());
        ConfigureHiveBolt.configureHiveStreamingBolt(builder, HIVE_COLS, HIVE_PARTITIONS, hiveLocalMetaStore.getMetaStoreUri(), HIVE_DB_NAME, HIVE_TABLE_NAME);
        stormCluster.submitTopology(TEST_TOPOLOGY_NAME, new Config(), builder.createTopology());
    }

    public void validateHiveResults() throws ClassNotFoundException, SQLException {
        System.out.println("HIVE: VALIDATING");
        // Load the Hive JDBC driver
        System.out.println("HIVE: Loading the Hive JDBC Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:" + hiveLocalServer2.getHiveServerThriftPort() + "/" + HIVE_DB_NAME, "user", "pass");

        String selectStmt = "SELECT * FROM " + HIVE_TABLE_NAME;
        Statement stmt = con.createStatement();

        System.out.println("HIVE: Running Select Statement: " + selectStmt);
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
        System.out.println("HDFS: VALIDATING");
        FileSystem hdfsFsHandle = hdfsCluster.getHdfsFileSystemHandle();
        RemoteIterator<LocatedFileStatus> listFiles = hdfsFsHandle.listFiles(new Path("/tmp/kafka_data"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();

            System.out.println("HDFS READ: Found File: " + file);

            BufferedReader br = new BufferedReader(new InputStreamReader(hdfsFsHandle.open(file.getPath())));
            String line = br.readLine();
            while (line != null) {
                System.out.println("HDFS READ: Found Line: " + line);
                line = br.readLine();
            }
        }
        hdfsFsHandle.close();
    }


    @Test
    public void testKafkaHiveHdfsTopology() throws TException, JSONException, ClassNotFoundException, SQLException, IOException {

        // Create the Hive table, produce test messages to Kafka, start the kafka-hive-hdfs Storm topology
        // Sleep 10 seconds to let processing complete
        createTable();
        KafkaProducerTest.produceMessages(LOCALHOST_BROKER, TEST_TOPIC, 50);
        runStormKafkaHiveHdfsTopology();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }

        // To ensure transactions and files are closed, stop storm
        stormCluster.stop(TEST_TOPOLOGY_NAME);
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
