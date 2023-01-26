package com.volante.innovation.validationservice;
//
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.StreamsConfig;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.KafkaStreamsConfiguration;
//import org.springframework.kafka.config.StreamsBuilderFactoryBean;
//
//import java.util.HashMap;
//import java.util.Map;

//@Configuration
public class KafkaStreamsConfig {
  /*  public static final String APP1_STREAMS_BUILDER_BEAN_NAME = "app1KafkaStreamsBuilder";
    public static final String APP2_STREAMS_BUILDER_BEAN_NAME = "app2KafkaStreamsBuilder";
    @Bean(name = APP1_STREAMS_BUILDER_BEAN_NAME)
    public StreamsBuilderFactoryBean app1KafkaStreamsBuilder() {
        Map<String, Object> app1StreamsConfigProperties = commonStreamsConfigProperties();
        app1StreamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, app1ConsumerGroupName);
        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(app1StreamsConfigProperties));
    }
    @Bean(name = APP2_STREAMS_BUILDER_BEAN_NAME)
    public StreamsBuilderFactoryBean app2KafkaStreamsBuilder() {
        Map<String, Object> app2StreamsConfigProperties = commonStreamsConfigProperties();
        app2StreamsConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, app2ConsumerGroupName);
        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(app2StreamsConfigProperties));
    }
    private Map<String, Object> commonStreamsConfigProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class.getName());
        return props;
    }*/
}