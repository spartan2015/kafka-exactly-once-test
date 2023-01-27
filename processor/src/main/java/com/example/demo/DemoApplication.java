package com.example.demo;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@SpringBootApplication
@Configuration
@EnableKafkaStreams
@EnableTransactionManagement
public class DemoApplication {
	private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}


	@Bean
	public NewTopic compactTopicExample() {
		return TopicBuilder.name("inputTransactions")
				.partitions(1)
				.replicas(1)
				.compact()
				.build();
	}


	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kafkaStreamsConfig(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
		Map props = new HashMap<>();
		props.put(APPLICATION_ID_CONFIG, "kafka-streams-demo-transactions");
		props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
		return new KafkaStreamsConfiguration(props);
	}

	@Bean
	public KafkaStreamsTracing kafkaStreamsTracer(@Autowired Tracing tracing) {
		return KafkaStreamsTracing.create(tracing);
	}
}
