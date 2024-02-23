package com.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Log4j2
@RequiredArgsConstructor
public class KafkaProducer<T> {
    @Value("${topic-name.string}")
    private String topic;
    @Value("${topic-name.json}")
    private String jsonTopic;
    private final KafkaTemplate<String, String> kafkaStringTemplate;
    private final KafkaTemplate<String, T> kafkaTemplate;

    /**
     * Send String Message to Kafka topic
     * @param message
     */
    public void sendStringMessage(String message) {
        kafkaStringTemplate.send(topic, message);
        log.info("Message: {} published to topic: {} ", message, topic);
    }

    /**
     * Send JSON Message to Kafka topic
     * @param employee
     */
    public void sendJsonMessage(T employee) {
        kafkaTemplate.send(jsonTopic, employee);
        log.info("Message: {} published to topic: {} ", employee, topic);
    }
}
