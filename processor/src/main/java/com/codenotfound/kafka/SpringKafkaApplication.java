package com.codenotfound.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@Configuration
@EnableKafka
public class SpringKafkaApplication {

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
  public NewTopic input() {
    return TopicBuilder.name("input")
            .partitions(3)
            .replicas(1)
            .build();
  }

  @Bean
  public NewTopic output() {
    return TopicBuilder.name("output")
            .partitions(1)
            .replicas(1)
            .build();
  }


  public static void main(String[] args) {
    SpringApplication.run(SpringKafkaApplication.class, args);
  }
}
