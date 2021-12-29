package com.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootStrapServers="localhost:9092";

        String topic="second_topic";
        String key="key_";
        String value="hello_world_";

        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0;i<10;i++){

            //Create producer record
            ProducerRecord<String,String> record =new ProducerRecord<>(topic,key+i,value+i);

            //send data-async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or exception is thrown
                    if(e==null){
                        //a record is successfully sent
                        logger.info("Received new meta data:+" +
                                "Topic: "+recordMetadata.topic()+"\n" +
                                "Partition: "+recordMetadata.partition()+"\n" +
                                "Offset: "+recordMetadata.offset()+"\n" +
                                "TimeStamp: "+recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing",e);
                    }
                }
            });

        }

        //flush data
        producer.flush();

        //flush and close data
        producer.close();
    }
}
