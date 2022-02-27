package twitterKafka.dataPipeline.kafka.consumer.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Service
@Slf4j
public class EsService {

    String INDEX_NAME = "stream_tweet_destination";

    private RestHighLevelClient restHighLevelClient;
    private ObjectMapper objectMapper;

    public EsService(RestHighLevelClient restHighLevelClient, ObjectMapper objectMapper) {
        this.restHighLevelClient = restHighLevelClient;
        this.objectMapper = objectMapper;
    }

    public void bulk(List<Map<String, String>> listData) throws Exception {
        Stream<IndexRequest> indexRequestStream = listData
                .stream().map(vibrationData -> new IndexRequest()
                        .index(INDEX_NAME).source(objectMapper.convertValue(vibrationData, Map.class)));

        IndexRequest[] indexRequests = indexRequestStream.toArray(IndexRequest[]::new);

        BulkRequest bulkRequest = new BulkRequest();
        // bulkRequest 사용시 connection 한번으로 많은 데이터를 Insert 할 수 있다.
        bulkRequest.add(indexRequests);
        log.info(">>> {} data have been processed.", listData.size());
        log.info(">>> bulk count = {}", indexRequests.length);
        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info(">>> Successful data save with ES<<<");
    }
}
