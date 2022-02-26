package twitterKafka.dataPipeline;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import twitterKafka.twitter.consumer.SpringConsumerApplication;
import twitterKafka.twitter.producer.SpringProducerApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class DataPipelineApplication{

	private final KafkaTemplate<Integer, String> template;  // DI by RequiredArgsConstructor

	public static void main(String[] args) {
//		SpringApplication.run(DataPipelineApplication.class, args);
//		SpringApplication application = new SpringApplication(DataPipelineApplication.class);
//		application.run(args);

//		SpringApplication producerApplication = new SpringApplication(SpringProducerApplication.class);
//		producerApplication.run(args);

		SpringApplication consumerApplication = new SpringApplication(SpringConsumerApplication.class);
		consumerApplication.run(args);
	}
}
