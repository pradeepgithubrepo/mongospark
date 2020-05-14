package com.mongo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class consumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumer.class.getName());
        Properties prop = new Properties();
        String bootstrapserver = "127.0.0.1:9092";
        String groupId = "esigroup1";
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
//        Subscribe to consumer
        consumer.subscribe(Collections.singleton("esi"));

//        Poll for data
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(5000));

            for(ConsumerRecord record : records){
                System.out.println("Key:"+record.key() +" value"+record.value());
                System.out.println("Partition info : "+record.partition());
            }

        }

    }
}
