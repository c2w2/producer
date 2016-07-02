package c2w2;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class producer{
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("metadata.broker.list", "kafka1:9092,kafkat2:9092,kafka3:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        KeyedMessage<String, String> message = new KeyedMessage<String, String>("c2w2", "WOW");  
        producer.send(message);
        producer.close();
    }
}
