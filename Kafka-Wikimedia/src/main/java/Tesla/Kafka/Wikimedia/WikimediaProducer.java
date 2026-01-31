package Tesla.Kafka.Wikimedia;

//import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import okhttp3.internal.http2.Header;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {

    public static void main(String[] args) throws InterruptedException {
        final Logger log = LoggerFactory.getLogger(WikimediaProducer.class.getName());
        String bootStrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        String topic = "recent-wiki-changes";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        log.info("URI :{} ", URI.create(url));

        BackgroundEventHandler backgroundEventHandler = new WikimediaHandler(producer,topic);

        Headers header = new Headers.Builder().add("user-agent","chinmay-wiki-consumer/1.0 (chinmaymule196@gmail.com)").build();

        EventSource.Builder eventSource =  new EventSource.Builder(ConnectStrategy.http(URI.create(url)).headers(header));

        BackgroundEventSource.Builder back = new BackgroundEventSource.Builder(backgroundEventHandler,eventSource);

        BackgroundEventSource backgroundEventSource = back.build();
        backgroundEventSource.start();
        TimeUnit.MINUTES.sleep(1);

    }
}
