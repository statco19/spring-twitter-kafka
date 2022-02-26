package twitterKafka.twitter.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import credentials.TokenAndKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class SpringProducerApplication implements CommandLineRunner {

    private static final String TOPIC_NAME = "test";

    private final KafkaTemplate<String, String> twitterKafkaTemplate;  // DI by RequiredArgsConstructor

    // use your own credentials - don't share them with anyone
    private String consumerKey = TokenAndKey.CONSUMER_KEY;
    private String consumerSecret = TokenAndKey.CONSUMER_SECRET;
    private String token = TokenAndKey.ACCESS_TOKEN;
    private String secret = TokenAndKey.ACCESS_TOKEN_SECRET;

    //    ArrayList<Long> followings = Lists.newArrayList(44196397L);  //elon musk
    ArrayList<Long> followings = Lists.newArrayList(TokenAndKey.MY_TWIITER_ID);
    ArrayList<String> terms = Lists.newArrayList("bts");

    @Override
    public void run(String... args) {

        log.info("Setup");

        // message queue size
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        // add a shutdown hook
        // this assures that a producer and a client will be closed under any circumstances
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("shutting down client from twitter...");
            client.stop();
            log.info("closing producer...");
            twitterKafkaTemplate.destroy();
            log.info("done!");
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                log.info(msg);
                twitterKafkaTemplate.send(new ProducerRecord<>(TOPIC_NAME, null, msg)).addCallback
                (new ListenableFutureCallback<SendResult<String, String>>() {
                     @Override
                     public void onFailure(Throwable ex) {
                         log.error(ex.getMessage(), ex);
                     }

                     @Override
                     public void onSuccess(SendResult<String, String> result) {
                         log.info("{}", result);
                     }
                });
            }
        }
        log.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

//        hosebirdEndpoint.trackTerms(terms);  // track terms
        hosebirdEndpoint.followings(followings);  // track users

        // using OAuth1
        // be careful with secret keys
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
