package com.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class);

        String bootStrapServers="localhost:9092";
        String groupId="my-third-application";
        String topic="second_topic";

        //create Consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);

        //Subscribe consumer to topics
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key: "+record.key() +" ,Value: "+record.value());
                logger.info("Partition: "+record.partition()+" ,Offset: "+record.offset());

            }

        }


    }
}
