package com.github.sakserv.kafka;

import com.github.sakserv.datetime.GenerateRandomDay;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Properties;

/**
 * Created by skumpf on 1/10/15.
 */
public class KafkaProducerTest {

    public static void produceMessages(String brokerList, String topic, int msgCount) throws JSONException {
        // Add Producer properties and created the Producer
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        System.out.println("KAFKA: Preparing To Send " + msgCount + " Events.");
        for (int i=0; i<msgCount; i++){

            // Create the JSON object
            JSONObject obj = new JSONObject();
            obj.put("id", String.valueOf(i));
            obj.put("msg", "test-message" + 1);
            obj.put("dt", GenerateRandomDay.genRandomDay());
            String payload = obj.toString();

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, null, payload);
            producer.send(data);
            System.out.println("Sent message: " + data.toString());
        }
        System.out.println("KAFKA: Initial Messages Sent");

        // Stop the producer
        producer.close();
    }
}
