package com.github.stream.kafka.twitter;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

//    kafka-topics --zookeeper 127.0.0.1:2181 --topic twitter_tweets --create --partitions 6  --replication-factor 1
//
//    kafka-console-consumer  --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey="u8r3D1nJAnpfMiW0ZZMy0Dqpq";
    String consumerSecrets ="dHLHSUo11e1bQy1XkOWO3WhMuPBsmo4OJ6ncyj5YaJcuq2T40K";
    String token = "951355002437554176-VK60AtdLXUccFjc0uIs5a2OjGskiCpu";
    String secret = "cf8ipnEUfUWSwdiXQY60tZacKYTRe7Lu3qJTKLg6Y1bGw";

    public TwitterProducer(){

    }
    public static void main(String[] args) {
//        System.out.println("args = [" + args + "]");
        new TwitterProducer().run();

    }
    public void run(){
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //create a twitter client
        Client client = createTwitterClient(msgQueue);

        //attempt to establish the connection.
        client.connect();


        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("stopping the application");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");

        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg !=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       if (e!= null){
                           logger.error("Something bad happend" + e);
                       }
                    }
                });
            }

        }
        logger.info("End of Application");
    }




    public Client createTwitterClient(BlockingQueue<String> msgQueue){





        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        /** Optional: set up some followings and track terms */
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka", "bitcoin", "sports", "politics", "soccer");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        /** These secrets should be read from a config file */
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecrets, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.
//        hosebirdClient.connect();
    }

    public KafkaProducer<String, String> createKafkaProducer(){
        //create Producer Properties
        String bootstrapServers= "127.0.0.1:9092";
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        //kafka 2.0>=1.1 so we can keep this 5, Use 1 otherwise
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer (at the expense of a bit of latency and CPU Usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));



        //create the producer
        KafkaProducer <String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
