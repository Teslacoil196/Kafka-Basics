package Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerShutdownDemo {

    private static Logger log = LoggerFactory.getLogger(ConsumerShutdownDemo.class.getSimpleName());

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

        final Thread mainthread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                // we are adding a showdown hook to throw an wakeup exception
                // such that consumer will throw an error which is handled and it'll
                // close the consumer
                // shutdown hooks only run when JVM is shutting down
                log.info("Detected a shutdown, calling consumer.wakeup()..");
                consumer.wakeup();
                try {
                    mainthread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("Key :" + record.key() + " | Value :" + record.value()
                            + " | Partitions :" + record.partition() + " | Offset :" + record.offset());
                }
            }

        } catch (WakeupException e){
            log.info("shutting down consumer");
        } catch (Exception e){
            log.error("Unhandled error ",e);
        } finally {
            consumer.close(); // close also commits offset if it has to
            log.info("shutdown consumer gracefully");
        }
    }
}
