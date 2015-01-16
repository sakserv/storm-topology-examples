package com.github.sakserv.storm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.github.sakserv.kafka.KafkaProducerTest;
import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import com.github.sakserv.minicluster.impl.MongodbLocalServer;
import com.github.sakserv.minicluster.impl.StormLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Date;

/**
 * Created by skumpf on 1/8/15.
 */
public class KafkaMongodbTopologyTest {

    // Logger
    private static final Logger LOG = Logger.getLogger(KafkaHiveHdfsTopologyTest.class);

    // Kafka static
    private static final String DEFAULT_LOG_DIR = "embedded_kafka";
    private static final String TEST_TOPIC = "test-topic";
    private static final Integer KAFKA_PORT = 9092;
    private static final String LOCALHOST_BROKER = "localhost:" + KAFKA_PORT.toString();
    private static final Integer BROKER_ID = 1;

    // MongoDB static
    private static final String DEFAULT_MONGODB_DATABASE_NAME = "test_database";
    private static final String DEFAULT_MONGODB_COLLECTION_NAME = "test_collection";
    private static final String DEFAULT_MONGODB_IP = "127.0.0.1";
    private static final int DEFAULT_MONGOD_PORT = 12345;

    // Storm static
    private static final String TEST_TOPOLOGY_NAME = "test";
    
    private ZookeeperLocalCluster zookeeperLocalCluster;
    private MongodbLocalServer mongodbLocalServer;
    private KafkaLocalBroker kafkaLocalBroker;
    private StormLocalCluster stormCluster;
    
    @Before
    public void setUp() {
        zookeeperLocalCluster = new ZookeeperLocalCluster();
        zookeeperLocalCluster.start();

        // Start Kafka
        kafkaLocalBroker = new KafkaLocalBroker(TEST_TOPIC, DEFAULT_LOG_DIR, KAFKA_PORT, BROKER_ID, zookeeperLocalCluster.getZkConnectionString());
        kafkaLocalBroker.start();
        
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
        ConfigureKafkaSpout.configureKafkaSpout(builder, zookeeperLocalCluster.getZkConnectionString(), TEST_TOPIC, "-2");
        ConfigureMongodbBolt.configureMongodbBolt(builder, mongodbLocalServer.getBindIp(), 
                mongodbLocalServer.getBindPort(), DEFAULT_MONGODB_DATABASE_NAME, DEFAULT_MONGODB_COLLECTION_NAME);
        stormCluster.submitTopology(TEST_TOPOLOGY_NAME, new Config(), builder.createTopology());
    }
    
    public void validateMongo() throws UnknownHostException {
        MongoClient mongo = new MongoClient(DEFAULT_MONGODB_IP, DEFAULT_MONGOD_PORT);

        DB db = mongo.getDB(DEFAULT_MONGODB_DATABASE_NAME);
        DBCollection col = db.getCollection(DEFAULT_MONGODB_COLLECTION_NAME);
        
        LOG.info("MONGODB: Number of items in collection: " + col.count());

        DBCursor cursor = col.find();
        while(cursor.hasNext()) {
            LOG.info("MONGODB: Document output: " + cursor.next());
        }
        cursor.close();
    }
    
    @Test
    public void testKafkaMongodbTopology() throws JSONException, UnknownHostException {

        KafkaProducerTest.produceMessages(LOCALHOST_BROKER, TEST_TOPIC, 50);
        runStormKafkaMongodbTopology();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }
        
        validateMongo();
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}
