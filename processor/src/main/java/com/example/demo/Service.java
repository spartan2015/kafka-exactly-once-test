package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

@org.springframework.stereotype.Service
public class Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(Service.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "inputTransactions", groupId = "withTransactions", errorHandler = "errorHandler")
    @Transactional
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        //LOGGER.info("received data='{}'", consumerRecord.toString());
        String key = (String) consumerRecord.key();
        //System.out.println(key);

      /*  if (true){
            throw new RuntimeException("my ex");
        }*/

        //transactionalService.doService();
        kafkaTemplate.send("output", key, (String) consumerRecord.value());
       /* kafkaTemplate.executeInTransaction(kafkaTemplate ->{

            return null;
        });*/
    }
}
