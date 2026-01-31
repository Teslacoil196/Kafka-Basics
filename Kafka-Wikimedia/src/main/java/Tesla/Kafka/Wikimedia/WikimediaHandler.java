package Tesla.Kafka.Wikimedia;

//import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaHandler implements BackgroundEventHandler {

    private final Logger log = LoggerFactory.getLogger(WikimediaHandler.class.getName());

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaHandler(KafkaProducer<String, String> kafkaProducer, String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // none
    }

    @Override
    public void onClosed() {
        this.kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        log.info(this.topic);
        this.kafkaProducer.send(new ProducerRecord<>(this.topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        // none
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error :",t);
    }
}
