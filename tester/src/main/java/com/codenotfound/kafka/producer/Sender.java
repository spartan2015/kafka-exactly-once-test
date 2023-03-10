package com.codenotfound.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class Sender {

  private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Transactional
  public void send(String topic, String key, String data) {
    //LOGGER.info("sending data='{}' to topic='{}'", data, topic);
    kafkaTemplate.send(topic, key, data);
    kafkaTemplate.flush();
  }
}
