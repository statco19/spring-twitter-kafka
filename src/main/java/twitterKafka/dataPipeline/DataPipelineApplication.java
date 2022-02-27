package twitterKafka.dataPipeline;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class DataPipelineApplication{

	public static void main(String[] args) {
		SpringApplication.run(DataPipelineApplication.class, args);
	}
}
