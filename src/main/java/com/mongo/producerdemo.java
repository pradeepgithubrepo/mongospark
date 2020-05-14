package com.mongo;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class producerdemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(producerdemo.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topicname = "esi";

for ( Integer i = 0 ; i < 10 ; i++ ){
    String keyvalue = "key"+ i.toString();
    String value = "hello world1"+i.toString();
            ProducerRecord record = new ProducerRecord<String, String>(topicname,keyvalue,value);

//        Send data -async
    producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                Executes everytime a record is sent

            if (e == null) {
                System.out.println(
//                        "Topic :" + recordMetadata.topic() +"\n"+
                        "Partition :" + recordMetadata.partition() +"\n"+
                        "Offset :" + recordMetadata.offset());
//                logger.info("Reeived new info \n" +
//                        "Topic" + recordMetadata.topic() +
//                        "\n Partition :" + recordMetadata.partition() +
//                        "\n Offset:" + recordMetadata.offset());
            } else {
                System.out.println("Into callback Error");
                logger.error("Error while producing data" + e);
            }
        }
    });
}


        producer.flush();
        producer.close();
    }
}
