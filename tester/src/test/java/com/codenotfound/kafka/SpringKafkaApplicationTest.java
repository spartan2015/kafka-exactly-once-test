package com.codenotfound.kafka;

import com.codenotfound.kafka.consumer.OutputReceiver;
import com.codenotfound.kafka.producer.Sender;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class SpringKafkaApplicationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Sender sender;

    @Autowired
    private OutputReceiver receiver;

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        System.setProperty("spring.kafka.bootstrap-servers", "localhost:9092");
    }

    @Test
    public void test() throws Exception {
        System.out.println("With transactions");
        long durationTransactions =  executeTest("inputTransactions");
        System.out.println("");
        Thread.sleep(3 * 1000);
        System.out.println("Without transactions");
        long durationNoTransactions = executeTest("inputNoTransactions");
        System.out.println();
        System.out.println("Without transactions is " + (durationTransactions / durationNoTransactions) + " times faster");
    }

    private long executeTest(String topic) throws InterruptedException {
        int NO_OF_MESSAGES_TO_TEST = 10_000;
        receiver.setLatch(new CountDownLatch(NO_OF_MESSAGES_TO_TEST));

        System.out.println("Start " + LocalDateTime.now());
        long start = System.currentTimeMillis();

        AtomicInteger at = new AtomicInteger();
        IntStream.range(0, NO_OF_MESSAGES_TO_TEST).forEach(i -> {

//            kafkaTemplate.executeInTransaction(kafkaTemplate->{

            kafkaTemplate.send(topic, "" + i, "Message " + i);
//                return null;
//            });
            at.incrementAndGet();
        });
        System.out.println("We sent " + at.intValue() + " messages");


        System.out.println("all sent in duration " + ((System.currentTimeMillis() - start)) + " ms");

        int same = 0;
        int lastReceived = 0;

        while (true) {

            if (lastReceived == receiver.receivedSet.size()) {
                same++;
            }
            lastReceived = receiver.receivedSet.size();

            System.out.println("received so far:" + receiver.receivedSet.size() + " messages");
            System.out.println("duration so far " + ((System.currentTimeMillis() - start)) + " ms");
            if (same == 3) {
                //System.out.println(receiver.receivedSet);
                AtomicInteger notFound = new AtomicInteger();
                IntStream.range(0, NO_OF_MESSAGES_TO_TEST).forEach(i -> {
                    if (!receiver.receivedSet.contains(i+"")){
//                        System.out.println("Not found: " + i );
                        notFound.incrementAndGet();
                    }
                });
                System.out.println("Lost messages: " + notFound.intValue());
                break;
            }

            if (receiver.getLatch().await(10, TimeUnit.SECONDS)) {
                break;
            }

        }


        System.out.println("received:" + receiver.receivedSet.size()+ " messages");
        System.out.println("duplicates:" + receiver.duplicated.size()+ " messages");
        long finalDuration = ((System.currentTimeMillis() - start));
        System.out.println("duration " + finalDuration  + " ms");

        return finalDuration;
    }
}
