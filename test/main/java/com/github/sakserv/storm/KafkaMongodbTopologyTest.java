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
import com.github.sakserv.storm.config.StormConfig;
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
    
    private ZookeeperLocalCluster zookeeperLocalCluster;
    private MongodbLocalServer mongodbLocalServer;
    private KafkaLocalBroker kafkaLocalBroker;
    private StormLocalCluster stormLocalCluster;
    
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
        mongodbLocalServer = new MongodbLocalServer(propertyParser.getProperty(ConfigVars.MONGO_IP_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)));
        mongodbLocalServer.start();

        // Start Storm
        stormLocalCluster = new StormLocalCluster(propertyParser.getProperty(ConfigVars.ZOOKEEPER_HOSTS_KEY),
                Long.parseLong(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)));
        stormLocalCluster.start();
    }
    
    @After
    public void tearDown() {
        // Stop Storm
        try {
            stormLocalCluster.stop(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY));
        } catch(IllegalStateException e) { }
        
        mongodbLocalServer.stop();
        
        kafkaLocalBroker.stop(true);
        
        zookeeperLocalCluster.stop(true);
    }

    public void runStormKafkaMongodbTopology() {
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
        
        // Configure the MongoBolt
        ConfigureMongodbBolt.configureMongodbBolt(builder, 
                propertyParser.getProperty(ConfigVars.MONGO_IP_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)),
                propertyParser.getProperty(ConfigVars.MONGO_DATABASE_NAME_KEY),
                propertyParser.getProperty(ConfigVars.MONGO_COLLECTION_NAME_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_BOLT_PARALLELISM_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_SPOUT_NAME_KEY),
                propertyParser.getProperty(ConfigVars.MONGO_BOLT_NAME_KEY));

        // Storm Topology Config
        Config stormConfig = StormConfig.createStormConfig(
                Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.STORM_ENABLE_DEBUG_KEY)),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.STORM_NUM_WORKERS_KEY)));
        
        // Submit the topology
        stormLocalCluster.submitTopology(propertyParser.getProperty(ConfigVars.STORM_TOPOLOGY_NAME_KEY),
                stormConfig, builder.createTopology());
    }
    
    public void validateMongo() throws UnknownHostException {
        
        // Establish a connection to the Mongo Collection
        MongoClient mongo = getMongoClient(propertyParser.getProperty(ConfigVars.MONGO_IP_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.MONGO_PORT_KEY)));
        DB db = getMongoDb(mongo, propertyParser.getProperty(ConfigVars.MONGO_DATABASE_NAME_KEY));
        DBCollection collection = getMongoCollection(db, 
                propertyParser.getProperty(ConfigVars.MONGO_COLLECTION_NAME_KEY));

        // Validate that all Kafka events were consumed by the MongoBolt and persisted
        LOG.info("MONGODB: Number of items in collection: " + collection.count());
        assertEquals(Long.parseLong(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)), collection.count());

        // Check the payload of each document in Mongo to ensure it matches the Kafka event
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

        // Run the Kafka Producer
        KafkaProducerTest.produceMessages(propertyParser.getProperty(ConfigVars.KAFKA_TEST_BROKER_LIST_KEY),
                propertyParser.getProperty(ConfigVars.KAFKA_TOPIC_KEY),
                Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_COUNT_KEY)),
                propertyParser.getProperty(ConfigVars.KAFKA_TEST_MSG_PAYLOAD_KEY));
        
        // Run the Kafka Mongo topology and sleep 5 sec to wait for completion
        runStormKafkaMongodbTopology();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }
        
        // Validate the results in Mongo
        validateMongo();
    }
}
