package com.github.stream.kafka.producerconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

   private ConsumerDemoWithThread(){

   }

   private void run(){
       String bootserver = "127.0.0.1:9092";
       String groupID = "my-Second-application";
       String topic = "first_topic";
       Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

       // latch for dealing with multiple Threads
       CountDownLatch latch = new CountDownLatch(1);

       //creating the consumer runnable
        logger.info("Creating the consumer Thread");
       Runnable myConsumerRunnable = new ConsumerRunnable(
               latch,
               bootserver,
               groupID,
               topic
       );

       //start the thread
       Thread myThread = new Thread(myConsumerRunnable);
       myThread.start();

       // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shut down hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
       try {
           latch.await();
       } catch (InterruptedException e) {
           logger.error("Application interrupted" + e);
       } finally {
           logger.info("Application is closing");
       }
   }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                              String bootserver,
                              String groupID,
                              String topic){
            this.latch = latch;


            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootserver);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID );
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);
            //subscribe consumer to our topic
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            // poll for new data
            try{
            while(true){
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));  //new in kafka poll is deprecated
                for (ConsumerRecord<String, String> record: records) {
                    logger.info("Key" + record.key() + " value: " + record.value());
                    logger.info("Partition" + record.partition() + " Offset: " + record.offset());
                }
            }}
            catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();

                //tell our main code that we are done
                latch.countDown();
            }

        }

        public void shutdown(){
            // the wakeup() method is a special method to interrup consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();
        }
    }
}
