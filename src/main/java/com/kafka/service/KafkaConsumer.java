package com.kafka.service;

import com.google.gson.JsonObject;
import com.kafka.constants.KafkaConstants;
import com.kafka.dto.Employee;
import com.kafka.entity.MessagesReceived;
import com.kafka.repository.MessagesReceivedRepo;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.sql.rowset.serial.SerialClob;
import java.sql.Clob;
import java.sql.SQLException;

@Service
@RequiredArgsConstructor
@Log4j2
public class KafkaConsumer {
    private final MessagesReceivedRepo messagesReceivedRepo;

    /**
     * Consume String Message and save it to DB
     * @param message
     */
    @KafkaListener(topics = {"${topic-name.string}"}, containerFactory = "kafkaListenerStringFactory", groupId = "group_id")
    public void consumeStringMessage(String message) {
        log.info("Consumed String message: {}", message);
        try {
            if(StringUtils.isNotBlank(message)) {
                Clob msgClob = new SerialClob(message.toCharArray());
                saveToMsgReceivedTable(msgClob);
            } else {
                log.error("Consumed JSON Message is NULL");
            }
        } catch (SQLException e) {
            log.error("Exception occurred while saving to DB: {},{}", e.getMessage(), e);
        }
    }

    /**
     * Consume JSON Message and save it to DB
     * @param employee
     */
    @KafkaListener(topics = {"${topic-name.json}"}, containerFactory = "kafkaListenerJsonFactory", groupId = "group_id")
    public void consumeJsonMessage(Employee employee) {
        log.info("Consumed Json Message: {}", employee);
        try {
            if(null != employee) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty(KafkaConstants.EMPLOYEE_NAME, employee.getName());
                jsonObject.addProperty(KafkaConstants.EMPLOYEE_AGE, employee.getAge());
                jsonObject.addProperty(KafkaConstants.EMPLOYEE_CITY, employee.getCity());
                jsonObject.addProperty(KafkaConstants.EMPLOYEE_EMAIL, employee.getEmailId());
                jsonObject.addProperty(KafkaConstants.EMPLOYEE_MOBILE_NO, employee.getMobileNo());
                jsonObject.addProperty(KafkaConstants.EMPLOYEE_IS_FULLSTACK_DEVELOPER, employee.isFullStackDeveloper());
                Clob msgClob = new SerialClob(jsonObject.toString().toCharArray());
                saveToMsgReceivedTable(msgClob);
            } else {
                log.error("Consumed JSON Message is NULL");
            }
        } catch (SQLException e) {
            log.error("Exception occurred while saving to DB: {},{}", e.getMessage(), e);
        }
    }

    /**
     * Save Message Clob to Message Received Table
     * @param msgClob
     */
    private void saveToMsgReceivedTable(Clob msgClob) {
        MessagesReceived messagesReceived = new MessagesReceived();
        messagesReceived.setMessageClob(msgClob);
        messagesReceivedRepo.save(messagesReceived);
    }
}
