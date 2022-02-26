package twitterKafka.twitter.streams;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
@NoArgsConstructor
@Slf4j
public class KafkaStreamsConfig {

    private final String STREAM_DESTINATION = "test-destination";
    private final String STREAM_TOPIC = "test";
    private final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final String APPLICATION_ID = "stream-application";

    // A Gson instance in order to handle json objects from twitter
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
//    public KafkaStreamsConfig() {}

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration KStreamsConfigs() {

        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG,APPLICATION_ID);
        props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        return new KafkaStreamsConfiguration(props);
    }

//    @Bean
//    public StreamsBuilderFactoryBean streamsBuilderFactoryBean() {
//
//        Map<String, Object> props = new HashMap<>();
//        props.put(APPLICATION_ID_CONFIG,APPLICATION_ID);
//        props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
//
//        KafkaStreamsConfiguration streamsConfig = new KafkaStreamsConfiguration(props);
//        StreamsBuilderFactoryBean streamsBuilder = new StreamsBuilderFactoryBean(streamsConfig);
////        streamsBuilder.setSingleton(Boolean.FALSE);
//        return streamsBuilder;
//    }

    @Bean
    public KStream<String, String> KStreamTruncated(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(STREAM_TOPIC);

        stream
                .filter((key,value) -> extractFromTweet(gson, value,"truncated").equals("true"))
                .mapValues(
                        value -> mvTruncated(gson, value))
                .to(STREAM_DESTINATION);

//        log.info("{}", stream);
        return stream;
    }

    @Bean
    public KStream<String, String> KStreamNotTruncated(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(STREAM_TOPIC);

        stream
                .filter((key, value) -> extractFromTweet(gson, value,"truncated").equals("false"))
                .mapValues(
                        value -> mvNotTruncated(gson, value))
                .to(STREAM_DESTINATION);

//        log.info("{}", stream);
        return stream;
    }

    private static String extractFromTweet(Gson gson, String value, String key) {
//        log.info("{}", value);
        //{"limit":{"track":5734,"timestamp_ms":"1645708445729"}}
        return gson.fromJson(value, JsonElement.class)
                .getAsJsonObject()
                .get(key)
                .getAsString();
    }

    private static String mvTruncated(Gson gson, String value) {
        String result = "{" + "\"created_at\":\""
                + extractFromTweet(gson, value, "created_at") + "\""
                + ",\"text\":\""
                + gson.fromJson(value, JsonElement.class)
                .getAsJsonObject()
                .get("extended_tweet")
                .getAsJsonObject()
                .get("full_text")
                .getAsString()
                .replace("\n", "\\n")
                .replace("\"", "")
                + "\"}";
        log.info("mvTruncated: {}", result);
        return result;
    }

    private static String mvNotTruncated(Gson gson, String value) {
        String result = "{" + "\"created_at\":\""
                + extractFromTweet(gson, value, "created_at") + "\""
                + ",\"text\":\""
                + extractFromTweet(gson, value, "text")
                .replace("\n", "\\n")
                .replace("\"", "")
                + "\"}";
        log.info("mvNotTruncated: {}", result);
        return result;
    }
}
