package com.example.demoWithout;

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
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

@SpringBootApplication
@Configuration
@EnableKafkaStreams
//@EnableTransactionManagement
public class DemoApplicationNoTransactions {
	private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplicationNoTransactions.class);

	public static void main(String[] args) {
		SpringApplication.run(DemoApplicationNoTransactions.class, args);
	}

	@Bean
	public NewTopic compactTopicExample() {
		return TopicBuilder.name("inputNoTransactions")
				.partitions(1)
				.replicas(1)
				.compact()
				.build();
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kafkaStreamsConfig(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
		Map props = new HashMap<>();
		props.put(APPLICATION_ID_CONFIG, "kafka-streams-demo-no-transactions");
		props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		props.put(PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE);
		return new KafkaStreamsConfiguration(props);
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
		//props.put("transactional.id", "my-transactional-id");
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

//	@Bean
//	public KafkaTransactionManager kafkaTransactionManager(final ProducerFactory<String, String> producerFactoryTransactional) {
//		return new KafkaTransactionManager<>(producerFactoryTransactional);
//	}


}
