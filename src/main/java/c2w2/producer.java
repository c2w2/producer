package c2w2;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class producer{
    public static void main(String[] args) throws Exception {
    	Random rand = new Random(System.currentTimeMillis());
    	Properties props = new Properties();
        props.put("metadata.broker.list", "kafka1:9092,kafkat2:9092,kafka3:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig  producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        String str;
        for(int i=0; i<2147483647; i++)
        {
        	if(i%3==0)
        	{
        		str=Integer.toString((int)(Math.sqrt(rand.nextInt(9)+1)));
        		KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic1", str);  
        		producer.send(message);
        	}else if(i%3==1)
        	{
        		str=Integer.toString((int)(Math.sqrt(rand.nextInt(9)+1)));
        		KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic2", str);  
        		producer.send(message);
        	}else if(i%3==2)
        	{
        		str=Integer.toString((int)(Math.sqrt(rand.nextInt(9)+1)));
        		KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic3", str);  
        		producer.send(message);
        	}
        }
        
        producer.close();
    }
}
