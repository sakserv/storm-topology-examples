package com.github.sakserv.kafka;

import com.github.sakserv.datetime.GenerateRandomDay;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by skumpf on 1/10/15.
 */
public class KafkaProducerTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerTest.class);

    public static void produceMessages(String brokerList, String topic, int msgCount, String msgPayload) throws JSONException, IOException {
        
        // Add Producer properties and created the Producer
        ProducerConfig config = new ProducerConfig(setKafkaBrokerProps(brokerList));
        Producer<String, String> producer = new Producer<String, String>(config);

        LOG.info("KAFKA: Preparing To Send " + msgCount + " Events.");
        for (int i=0; i<msgCount; i++){

            // Create the JSON object
            JSONObject obj = new JSONObject();
            obj.put("id", String.valueOf(i));
            obj.put("msg", msgPayload);
            obj.put("dt", GenerateRandomDay.genRandomDay());
            String payload = obj.toString();

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, null, payload);
            producer.send(data);
            LOG.info("Sent message: " + data.toString());
        }
        LOG.info("KAFKA: Sent " + msgCount + " Events.");

        // Stop the producer
        producer.close();
    }
    
    private static Properties setKafkaBrokerProps(String brokerList) {

        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        return props;
    }
}
