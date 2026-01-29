package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("hello");

        // Properties for kafka
        Properties properties = new Properties();

        // location
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        // Serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // create message
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java","Hello world");

        // send data
        producer.send(record);

        // flush
        producer.flush();

        // close producer
        producer.close();
        log.info("done");
    }
}
