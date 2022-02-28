package twitterKafka.dataPipeline.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.util.backoff.FixedBackOff;
import twitterKafka.dataPipeline.kafka.consumer.elasticsearch.EsService;

import java.io.IOException;

@Service
@RequiredArgsConstructor
@Slf4j
public class SpringConsumerApplication {

    @Autowired
    private EsService esKafkaService;

    @Autowired
    private KafkaTemplate<String, String> template;  // injected for handling error while consuming records

//    @KafkaListener(topics = "test",
//                groupId = "test-group-01",
//                properties = {"auto.offset.reset:earliest"},
//                concurrency = "3"
//                )
//    public void customListener(ConsumerRecords<String, String> records,
//                               Acknowledgment ack) throws IOException {
//        records.forEach(record -> log.info("record: {}", record));
//        ack.acknowledge();  // manual commit
//    }


    @KafkaListener(topics = "offset-error-test",
                    groupId = "test-group-01",
                    containerFactory = "customContainerFactory",
                    properties = {"auto.offset.reset:earliest"},
                    concurrency = "3")
    public void testListener(ConsumerRecords<String, String> records,
                             Acknowledgment ack) throws Exception {
        for (ConsumerRecord<String, String> record : records) {
            if(record.value().startsWith("error")) {
                throw new BatchListenerFailedException("error thrown",record);
            } else {
                log.info("{}", record);
                ack.acknowledge();
            }
        }
    }

    /**
     * Kafka Listener Container Factory for default Kafka Listener
     * Error handling while consuming records in a batch available
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new DeadLetterPublishingRecoverer(
                template
        )));
        return factory;
    }
}
