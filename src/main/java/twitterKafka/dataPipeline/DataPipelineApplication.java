package twitterKafka.dataPipeline;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import twitterKafka.twitter.consumer.SpringConsumerApplication;
import twitterKafka.twitter.producer.SpringProducerApplication;

@SpringBootApplication
@RequiredArgsConstructor
@Import({SpringProducerApplication.class, SpringConsumerApplication.class})
//@EnableKafkaStreams
public class DataPipelineApplication{

	public static void main(String[] args) {
		SpringApplication.run(DataPipelineApplication.class, args);
	}
}
