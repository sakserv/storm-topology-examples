package com.github.sakserv.storm;

import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import com.github.sakserv.minicluster.impl.MongodbLocalServer;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by skumpf on 1/8/15.
 */
public class KafkaMongodbTopologyTest {
    
    private ZookeeperLocalCluster zookeeperLocalCluster;
    private MongodbLocalServer mongodbLocalServer;
    private KafkaLocalBroker kafkaLocalBroker;
    
    @Before
    public void setUp() {
        zookeeperLocalCluster = new ZookeeperLocalCluster();
        zookeeperLocalCluster.start();
        
        kafkaLocalBroker = new KafkaLocalBroker();
        kafkaLocalBroker.start();
        
        mongodbLocalServer = new MongodbLocalServer("127.0.0.1", 12345);
        mongodbLocalServer.start();
    }
    
    @After
    public void tearDown() {
        mongodbLocalServer.stop();
        
        kafkaLocalBroker.stop(true);
        
        zookeeperLocalCluster.stop(true);
    }
    
    @Test
    public void runTest() {

        
    }
}
