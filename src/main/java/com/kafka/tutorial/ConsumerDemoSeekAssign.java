package com.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoSeekAssign {

    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemoSeekAssign.class);

        String bootStrapServers="localhost:9092";
        String topic="second_topic";
        int partition=0;


        //create Consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);

        // replay data or fetch a specific message
        // assign
        TopicPartition topicPartition = new TopicPartition(topic,partition);
        consumer.assign(Arrays.asList(topicPartition));

        long offsetToReadFrom=25;

        consumer.seek(topicPartition,offsetToReadFrom);

        long messagesReadSoFar=0;
        long messagesToRead=5;
        boolean keepOnReading=true;

        //poll for new data
        while(keepOnReading){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records){
                messagesReadSoFar++;
                logger.info("Key: "+record.key() +" ,Value: "+record.value());
                logger.info("Partition: "+record.partition()+" ,Offset: "+record.offset());

                if(messagesReadSoFar>=messagesToRead){
                    keepOnReading=false;
                    break;
                }

            }

        }


    }
}
