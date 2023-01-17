package com.codenotfound.kafka.consumer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OutputReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(OutputReceiver.class);

  private CountDownLatch latch;

  public CountDownLatch getLatch() {
    return latch;
  }

  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  public Set<String> receivedSet = new HashSet<>();
  public List<String> duplicated =  new ArrayList<>();

  @KafkaListener(topics = "${topic.boot}")
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    LOGGER.info("output received data='{}'", consumerRecord.toString());
    String key = (String)consumerRecord.key();

    if (receivedSet.contains(key)) {
      duplicated.add(key);
      System.err.println("Duplicated key " + key);
    }
    receivedSet.add(key);

    latch.countDown();
  }
}
