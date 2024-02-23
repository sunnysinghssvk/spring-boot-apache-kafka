package com.kafka.controller;

import com.kafka.dto.Employee;
import com.kafka.service.KafkaProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;



import java.util.HashMap;
import java.util.Map;

@Log4j2
@RequiredArgsConstructor
@RestController
@RequestMapping("/kafka")
public class KafkaController {
    private final KafkaProducer<Employee> kafkaProducer;

    @GetMapping(value = "/publish/string")
    public String sendStringMessageToKafkaTopic(@RequestParam("message") String message) {
        try {
            log.info("Publishing string message: {}", message);
            kafkaProducer.sendStringMessage(message);
            return "String message published Successfully!";
        } catch (Exception e) {
            log.error("Exception occurred while publishing string message: {},{}", e.getMessage(), e);
            return "Not able to publish message";
        }
    }

    @PostMapping(value = "/publish/json")
    public Map<String, Object> sendEmployeeJSONToKafkaTopic(@RequestBody Employee employee) {
        Map<String, Object> map = new HashMap<>();
        try {
            log.info("Publishing employee json message: {}", employee);
            kafkaProducer.sendJsonMessage(employee);
            map.put("message", "Employee JSON message published Successfully!");
            map.put("payload", employee);
            return map;
        } catch (Exception e) {
            log.error("Exception occurred while publishing employee json message: {},{}", e.getMessage(), e);
            map.put("message", "Not able to publish message");
            map.put("payload", employee);
            return map;
        }
    }
}
