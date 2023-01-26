package com.codenotfound.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@Configuration
@EnableTransactionManagement
public class SpringKafkaApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaApplication.class, args);
  }

  @Bean
  public NewTopic adviceTopic() {
    return new NewTopic("input", 3, (short) 1);
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092");
    props.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            "group_id");
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

    props.put(JsonDeserializer.TRUSTED_PACKAGES,
            "com.github.wenqiglantz.service.orderservice.data, com.github.wenqiglantz.service.customerservice.data");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    props.put(JsonDeserializer.TYPE_MAPPINGS,
            "customerVO:com.github.wenqiglantz.service.orderservice.data.CustomerVO"
    );

    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String>
  kafkaListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
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

//    props.put(
//            "enable.idempotence",
//            "true");
//    props.put("transactional.id", "my-transactional-id");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");


    DefaultKafkaProducerFactory<String, String> cf = new DefaultKafkaProducerFactory<>(props);
    //cf.addListener(new MicrometerProducerListener<>(new SimpleMeterRegistry()));
    customizers.orderedStream().forEach(customizer -> customizer.customize(cf));
    return cf;
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
    KafkaTemplate<String, String> stringStringKafkaTemplate = new KafkaTemplate<>(producerFactory);
    //stringStringKafkaTemplate.setObservationEnabled(true);//trying out with
    return stringStringKafkaTemplate;
  }

  /*@Bean
  public KafkaTransactionManager kafkaTransactionManager(final ProducerFactory<String, String> producerFactoryTransactional) {
    return new KafkaTransactionManager<>(producerFactoryTransactional);
  }*/

}
