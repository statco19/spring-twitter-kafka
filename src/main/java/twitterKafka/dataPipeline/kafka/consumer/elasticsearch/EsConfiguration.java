package twitterKafka.dataPipeline.kafka.consumer.elasticsearch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class EsConfiguration {

    private RestHighLevelClient restHighLevelClient;

    public EsConfiguration(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Bean
    public RestHighLevelClient createInstance() throws Exception {
//        final ClientConfiguration clientConfiguration = ClientConfiguration.builder()
//                .connectedTo("localhost:9200")
//                .build();
//
//        return RestClients.create(clientConfiguration).rest();

        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
