package twitterKafka.dataPipeline;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import twitterKafka.twitter.consumer.SpringConsumerApplication;
import twitterKafka.twitter.producer.SpringProducerApplication;
import twitterKafka.twitter.streams.KafkaStreamsConfig;

@SpringBootApplication
@RequiredArgsConstructor
@Import({SpringProducerApplication.class, SpringConsumerApplication.class, KafkaStreamsConfig.class})
public class DataPipelineApplication{

	public static void main(String[] args) {
		SpringApplication.run(DataPipelineApplication.class, args);
	}
}
