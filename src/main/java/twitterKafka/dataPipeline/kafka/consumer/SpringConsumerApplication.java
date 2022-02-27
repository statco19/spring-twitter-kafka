package twitterKafka.dataPipeline.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SpringConsumerApplication {

    @KafkaListener(topics = "test",
                groupId = "test-group-01",
                containerFactory = "customContainerFactory")
    public void customListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info("record: {}", record));
    }
}
