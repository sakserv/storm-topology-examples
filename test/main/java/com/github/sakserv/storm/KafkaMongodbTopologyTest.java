package com.github.sakserv.storm;

import backtype.storm.Config;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.config.ConfigVars;
import com.github.sakserv.config.PropertyParser;
import com.github.sakserv.kafka.KafkaProducerTest;
import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import com.github.sakserv.minicluster.impl.MongodbLocalServer;
import com.github.sakserv.minicluster.impl.StormLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.storm.scheme.JsonScheme;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static com.thewonggei.regexTester.hamcrest.RegexMatches.doesMatchRegex;

/**
 * Created by skumpf on 1/8/15.
 */
public class KafkaMongodbTopologyTest {

    // Logger
    private static final Logger LOG = Logger.getLogger(KafkaHiveHdfsTopologyTest.class);
    
    // Properties file for tests
    private PropertyParser propertyParser;
    private static final String PROP_FILE = "local.properties";

    // MongoDB static
    private static final String DEFAULT_MONGODB_DATABASE_NAME = "test_database";
    private static final String DEFAULT_MONGODB_COLLECTION_NAME = "test_collection";
    private static final String DEFAULT_MONGODB_IP = "127.0.0.1";
    private static final int DEFAULT_MONGOD_PORT = 12345;
    private static final int DEFAULT_MONGOBOLT_PARALLELISM = 1;
    private static final String DEFAULT_MONGOBOLT_NAME = "mongobolt";

    // Storm static
    private static final String TEST_TOPOLOGY_NAME = "test_topology";
    
    private ZookeeperLocalCluster zookeeperLocalCluster;
    private MongodbLocalServer mongodbLocalServer;
    private KafkaLocalBroker kafkaLocalBroker;
    private StormLocalCluster stormCluster;
    
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

        // Start Kafka
        kafkaLocalBroker = new KafkaLocalBroker(propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TEST_TEMP_DIR_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_ID_KEY)),
                propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY));
        kafkaLocalBroker.start();
        
        // Start MongoDB
        mongodbLocalServer = new MongodbLocalServer(DEFAULT_MONGODB_IP, DEFAULT_MONGOD_PORT);
        mongodbLocalServer.start();

        // Start Storm
        stormCluster = new StormLocalCluster(zookeeperLocalCluster.getZkHostName(), 
                Long.parseLong(zookeeperLocalCluster.getZkPort()));
        stormCluster.start();
    }
    
    @After
    public void tearDown() {
        // Stop Storm
        try {
            stormCluster.stop(TEST_TOPOLOGY_NAME);
        } catch(IllegalStateException e) { }
        
        mongodbLocalServer.stop();
        
        kafkaLocalBroker.stop(true);
        
        zookeeperLocalCluster.stop(true);
    }

    public void runStormKafkaMongodbTopology() {
        LOG.info("STORM: Starting Topology: " + TEST_TOPOLOGY_NAME);
        TopologyBuilder builder = new TopologyBuilder();
        
        // Configure the KafkaSpout
        ConfigureKafkaSpout.configureKafkaSpout(builder, 
                propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_START_OFFSET_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_PARALLELISM_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_SCHEME_CLASS_KEY));
        
        // Configure the MongoBolt
        ConfigureMongodbBolt.configureMongodbBolt(builder, 
                mongodbLocalServer.getBindIp(), 
                mongodbLocalServer.getBindPort(), 
                DEFAULT_MONGODB_DATABASE_NAME, 
                DEFAULT_MONGODB_COLLECTION_NAME,
                DEFAULT_MONGOBOLT_PARALLELISM,
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY), DEFAULT_MONGOBOLT_NAME);
        
        // Submit the topology
        stormCluster.submitTopology(TEST_TOPOLOGY_NAME, new Config(), builder.createTopology());
    }
    
    public void validateMongo() throws UnknownHostException {
        MongoClient mongo = getMongoClient(DEFAULT_MONGODB_IP, DEFAULT_MONGOD_PORT);
        DB db = getMongoDb(mongo, DEFAULT_MONGODB_DATABASE_NAME);
        DBCollection collection = getMongoCollection(db, DEFAULT_MONGODB_COLLECTION_NAME);

        LOG.info("MONGODB: Number of items in collection: " + collection.count());
        assertEquals(Long.parseLong(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)), collection.count());

        DBCursor cursor = collection.find();
        int counter = 0;
        while(cursor.hasNext()) {
            Map msg = cursor.next().toMap();
            assertEquals(counter, Integer.parseInt(msg.get("id").toString()));
            assertEquals(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_PAYLOAD_KEY), msg.get("msg").toString());
            assertThat(msg.get("dt").toString(), doesMatchRegex("\\d{4}-\\d{2}-\\d{2}"));
            counter++;
            LOG.info("MONGODB: Document output: " + msg);
        }
        cursor.close();
    }
    
    public MongoClient getMongoClient(String mongodbIp, int mongodbPort) throws UnknownHostException {
        return new MongoClient(mongodbIp, mongodbPort);
    }
    
    public DB getMongoDb(MongoClient mongo, String mongoDatabaseName) {
        return mongo.getDB(mongoDatabaseName);
    }
    
    public DBCollection getMongoCollection(DB mongoDatabase, String mongoCollection) {
        return mongoDatabase.getCollection(mongoCollection);
    }
    
    @Test
    public void testKafkaMongodbTopology() throws JSONException, IOException {

        KafkaProducerTest.produceMessages(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_LIST_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_PAYLOAD_KEY));
        runStormKafkaMongodbTopology();
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }
        
        validateMongo();
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}
