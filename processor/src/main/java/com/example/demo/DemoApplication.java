package com.example.demo;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@Configuration
@EnableKafka
@EnableTransactionManagement
public class DemoApplication {
	private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory(ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers) {
		Map<String, Object> props = new HashMap<>();
		props.put(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		props.put(
				ConsumerConfig.GROUP_ID_CONFIG,
				"test");
		props.put(
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);

		props.put(
				ConsumerConfig.ISOLATION_LEVEL_CONFIG,
				"read_committed");
		props.put(
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				"false");
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");


		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);

		cf.addListener(new MicrometerConsumerListener<>(new SimpleMeterRegistry()));
		customizers.orderedStream().forEach(customizer -> customizer.customize(cf));
		return cf;
	}

	@Bean
	public ProducerFactory<String, String> producerFactory(ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {

		Map<String, Object> props = new HashMap<>();
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		props.put(
				ConsumerConfig.GROUP_ID_CONFIG,
				"test");
		props.put(
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);

		props.put(
				"enable.idempotence",
				"true");
		props.put("transactional.id", "my-transactional-id");
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");


		DefaultKafkaProducerFactory<String, String> cf = new DefaultKafkaProducerFactory<>(props);
		cf.addListener(new MicrometerProducerListener<>(new SimpleMeterRegistry()));
		customizers.orderedStream().forEach(customizer -> customizer.customize(cf));
		return cf;
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
		KafkaTemplate<String, String> stringStringKafkaTemplate = new KafkaTemplate<>(producerFactory);
		stringStringKafkaTemplate.setObservationEnabled(true);//trying out with
		return stringStringKafkaTemplate;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String>
	kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {


		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();

		//The following code enable observation in the consumer listener
		factory.getContainerProperties().setObservationEnabled(true);
		factory.setConsumerFactory(consumerFactory);


		return factory;
	}

	@Bean
	public KafkaTransactionManager kafkaTransactionManager(final ProducerFactory<String, String> producerFactoryTransactional) {
		return new KafkaTransactionManager<>(producerFactoryTransactional);
	}
}
