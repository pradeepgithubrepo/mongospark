package com.mongo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class consumerthreads {

    public static void main(String[] args) {
        new consumerthreads().run();
    }

    private consumerthreads() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(consumerthreads.class.getName());
        Properties prop = new Properties();
        String bootstrapserver = "127.0.0.1:9092";
        String groupId = "esigroup2";
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        Latch is to deal with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

//        Create consumer runnable
        Runnable myconsumerthdrunnable = new consumerthd(latch, prop);

//        Start the thread
        Thread mythread = new Thread(myconsumerthdrunnable);
        mythread.start();

//        Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Caught shutdown hook");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}

class consumerthd implements Runnable {
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public consumerthd(CountDownLatch latch, Properties props) {
        this.latch = latch;
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton("esi"));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for (ConsumerRecord record : records) {
                    System.out.println("Key:" + record.key() + " value" + record.value());
                    System.out.println("Partition info : " + record.partition());
                }

            }
        } catch (WakeupException e) {
            System.out.println("Recevied shutdown signal");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}



