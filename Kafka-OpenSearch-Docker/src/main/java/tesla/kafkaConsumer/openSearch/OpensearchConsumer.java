package tesla.kafkaConsumer.openSearch;

import com.google.gson.JsonParser;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpensearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {

        final Logger log = LoggerFactory.getLogger(RestHighLevelClient.class.getName());

        URI connUri = URI.create("http://localhost:9200/");
        log.info("URI : {}", connUri);

        RestHighLevelClient restHighLevelClient;
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("http", connUri.getHost(), connUri.getPort())));

        } else {
            String[] auth = userInfo.split(":");
            AuthScope authScope = new AuthScope(connUri.getHost(), connUri.getPort());
            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(authScope, new UsernamePasswordCredentials(auth[0], auth[1].toCharArray()));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getScheme(), connUri.getHost(), connUri.getPort()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createConsumer() {

        Properties properties = new Properties();
        String group_id = "opensearch-consumer";

        // location
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // DE-Serializer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // group
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        // offset reset
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        return new KafkaConsumer<String, String>(properties);
    }

    public static String IDfromJson(String json){

        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        Logger log = LoggerFactory.getLogger(OpensearchConsumer.class.getName());
        String topic = "recent-wiki-changes";

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createConsumer();
        BulkRequest bulkRequest = new BulkRequest();

        final Thread mainthread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                // we are adding a showdown hook to throw a wakeup exception
                // such that consumer will throw an error which is handled, and it'll
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

        try (openSearchClient ; consumer) {
            String indexName = "wikimedia";
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest request = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(request, RequestOptions.DEFAULT);
                log.info("Created index {} in open search", indexName);
            } else {
                log.info("Index {} already exists",indexName);
            }
            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords <String, String> records = consumer.poll(Duration.ofSeconds(5));
                log.info("Received {} records ", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    String id = IDfromJson(record.value());
                    IndexRequest request = new IndexRequest(indexName).source(record.value(), XContentType.JSON)
                            .id(id);

                    bulkRequest.add(request);
                    //IndexResponse index = openSearchClient.index(request, RequestOptions.DEFAULT);
                    //log.info(index.getId());
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse responses = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("send {} records to open-search",responses.getItems().length);
                }
            }

        } catch (WakeupException e) {
            log.info("shutting down consumer and commiting request to open search");
            BulkResponse responses = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("commited {} records to open-search while closing",responses.getItems().length);
            Thread.sleep(Duration.ofMillis(5000));
            consumer.close();
            openSearchClient.close();
        }
    }
}
