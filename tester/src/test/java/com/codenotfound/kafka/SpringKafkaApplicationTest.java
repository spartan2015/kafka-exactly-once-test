package com.codenotfound.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.codenotfound.kafka.consumer.OutputReceiver;
import com.codenotfound.kafka.producer.Sender;

@SpringBootTest
public class SpringKafkaApplicationTest {

  @Autowired
  private Sender sender;

  @Autowired
  private OutputReceiver receiver;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    System.setProperty("spring.kafka.bootstrap-servers", "localhost:9092");
  }

  @Test
  public void testReceive() throws Exception {
    int NO_OF_MESSAGES_TO_TEST = 1000;
    receiver.setLatch(new CountDownLatch(NO_OF_MESSAGES_TO_TEST));

    for(int i = 0; i < NO_OF_MESSAGES_TO_TEST; i++){
      sender.send("input", ""+ i, "Message " + i);
    }
    // wait for processing to complete
    receiver.getLatch().await(10000, TimeUnit.SECONDS);

    assertThat(receiver.getLatch().getCount()).isEqualTo(0);

    assertThat(receiver.receivedSet.size()).isEqualTo(NO_OF_MESSAGES_TO_TEST);

    assertThat(receiver.duplicated.size()).isEqualTo(0);
  }
}
