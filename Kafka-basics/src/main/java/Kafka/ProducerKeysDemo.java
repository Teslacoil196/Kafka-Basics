package Kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeysDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerKeysDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer with sticky partitions demo");
        /*
        To demo that messages with same key gets send to same
        partitions
         */

        // Properties for kafka
        Properties properties = new Properties();

        // location
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        // Serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int j =0; j <2 ; j++) {

            for (int i = 0; i < 30; i++) {
                // create message
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "HelloWorld_" + j;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info(
                                    "Topic : " + recordMetadata.topic() + "|" +
                                    " Partition : " + recordMetadata.partition() + "|" +
                                    " key : " + key + "|" + " Value :" +value
                                    );
                        } else {
                            log.error("Error occurred while sending data " + e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // flush
        producer.flush();

        // close producer
        producer.close();
        log.info("done");
    }
}
