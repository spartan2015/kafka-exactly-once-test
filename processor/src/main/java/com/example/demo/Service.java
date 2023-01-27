package com.example.demo;

import brave.kafka.streams.KafkaStreamsTracing;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;

@org.springframework.stereotype.Service
public class Service {
    private static final Logger LOG = LoggerFactory.getLogger(Service.class);

    @Autowired
    private Tracer tracer;

    @Autowired
    KafkaStreamsTracing kafkaStreamsTracing;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream("inputTransactions", Consumed.with(Serdes.String(), Serdes.String()));

        messageStream.process(kafkaStreamsTracing.foreach("processing",(k,v)->{

            Span span = tracer.nextSpan();
            try  {
                span.start();

                LOG.info("hello " + k + " " + v);

                span.tag("error", "true");
            } finally {
                span.end();
            }

        }));

        messageStream.to("output");
    }
}
