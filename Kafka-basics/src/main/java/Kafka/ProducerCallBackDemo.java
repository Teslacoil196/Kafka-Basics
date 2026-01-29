package Kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallBackDemo {

    private static Logger log = LoggerFactory.getLogger(ProducerCallBackDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer with sticky partitions demo");
        /*
        To demo sticky partitions
        if a lot of data is being in a short amount of time
        it ends up going to same partition, intentional choice to improve performance
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

        for (int i = 0; i<10 ; i++) {
            // create message
            ProducerRecord<String, String> record = new ProducerRecord<>("demo_java","Hello world "+i);

            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Messege send \n" +
                                "Topic : "+ recordMetadata.topic() + "\n" +
                                "Partition : "+ recordMetadata.partition() + "\n" +
                                "Offset : "+ recordMetadata.offset() + "\n" +
                                "Time : "+ recordMetadata.timestamp() + "\n");
                    } else {
                        log.error("Error occurred while sending data "+ e);
                    }
                }
            });
        }
        // flush
        producer.flush();

        // close producer
        producer.close();
        log.info("done");
    }
}
