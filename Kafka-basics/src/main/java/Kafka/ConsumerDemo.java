package Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer demo");
        /*
        To demo consumers in kafka
         */

        // Properties for kafka
        Properties properties = new Properties();

        String topic = "demo_java";
        String group_id = "my-java-application";

        // location
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        // Serializer
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        // group
        properties.setProperty("group.id",group_id);
        // offset reset
        properties.setProperty("auto.offset.reset","earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : consumerRecords){
                log.info("Key :"+ record.key() + " | Value :"+record.value()
                + " | Partitions :"+record.partition() + " | Offset :"+record.offset());
            }
        }
    }
}
