package com.kafka.config;

import com.kafka.dto.Employee;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Log4j2
@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    @Value("${spring.kafka.consumer.bootstrap-servers: localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id: group_id}")
    private String groupId;

    /**
     * JSON Consumer
     * @return
     */
    @Bean
    public ConsumerFactory<String, Employee> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new JsonDeserializer<>(Employee.class));
    }

    /**
     * ConcurrentKafkaListenerContainerFactory for JSON Consumer
     * @return
     * @param <T>
     */
    @Bean
    public <T> ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerJsonFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Employee> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRecordMessageConverter(new StringJsonMessageConverter());
        factory.setBatchListener(true);
        return factory;
    }
    /**
     * String Consumer
     * @return
     */
    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer());
    }
    /**
     * ConcurrentKafkaListenerContainerFactory for String Consumer
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerStringFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringConsumerFactory());
        factory.setBatchListener(true);
        return factory;
    }
    @Bean
    public KafkaListenerErrorHandler myTopicErrorHandler() {
        return (m, e) -> {
            log.error("Exception occurred: {},{}", e.getMessage(), e);
            return e.getMessage();
        };
    }
}
