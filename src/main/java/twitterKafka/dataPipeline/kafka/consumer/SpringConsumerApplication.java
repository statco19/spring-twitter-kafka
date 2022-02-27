package twitterKafka.dataPipeline.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import twitterKafka.dataPipeline.kafka.consumer.elasticsearch.EsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class SpringConsumerApplication {

    @Autowired
    private EsService esKafkaService;

    @KafkaListener(topics = "test",
                groupId = "test-group-01",
                containerFactory = "customContainerFactory")
    public void customListener(ConsumerRecords<String, String> records,
                               Consumer<String, String> consumer) throws IOException {
        records.forEach(record -> log.info("record: {}", record));
    }

    @KafkaListener(topics = "test-destination",
            groupId = "test-group-02",
            containerFactory = "customContainerFactory")
    public void esListener(ConsumerRecords<String, String> records,
                               Consumer<String, String> consumer) throws IOException {

        Map<String,String> map = new HashMap<>();
        List<Map<String, String>> list = new ArrayList<>();

        for (ConsumerRecord<String,String> record : records){
            if (record.value()!= null) {
//                String id = record.topic() + "_" + record.partition() + "_" + record.offset();
//                log.info("id = {}", id);

                map.put(String.valueOf(record.value().hashCode()),record.value());
                log.info("record.value(): {}", record.value());
                log.info("Topic : {}, Partition : {}, Offset : {}, Key : {}",
                        record.topic(),record.partition(),record.offset(),record.key());
            }
        }
        list.add(map);
        try {
            if (!list.get(0).isEmpty()) {
                esKafkaService.bulk(list);
                consumer.commitSync();
            }
        } catch (IOException ie) {
            log.error("",ie);
        } catch (Exception e) {
            log.error("",e);
        }
    }
}
