package twitterKafka.twitter.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class SpringProducerApplication implements CommandLineRunner {

    private static final String TOPIC_NAME = "test";

    private final KafkaTemplate<Integer, String> template;  // DI by RequiredArgsConstructor

    @Override
    public void run(String... args) {
        for(int i=0;i<10;++i) {
            template.send(TOPIC_NAME, "spring producer application no.2 " + i);
        }
        System.exit(0);
    }
}
