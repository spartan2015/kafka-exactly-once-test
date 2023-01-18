package com.codenotfound.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;
import java.util.random.RandomGenerator;

@Component
public class Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  public void send(String topic, String key, String data) {
    LOGGER.info("sending data='{}' to topic='{}'", data, topic);
    kafkaTemplate.send(topic, key, data);
  }

  @KafkaListener(topics = "input", groupId = "group_id")
  @Transactional
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    LOGGER.info("received data='{}'", consumerRecord.toString());
    String key = (String)consumerRecord.key();

 /*   if (Random.from(RandomGenerator.getDefault()).nextInt() % 3 == 0){
      throw new RuntimeException("fail");
    }
*/
    send("output", key, (String)consumerRecord.value());
  }
}
