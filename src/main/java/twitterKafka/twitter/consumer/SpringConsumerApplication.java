package twitterKafka.twitter.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@Slf4j
public class SpringConsumerApplication {

    @KafkaListener(topics = "test",
                groupId = "test-group-01",
                containerFactory = "customContainerFactory")
    public void customListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info("record: {}", record));
    }
}
