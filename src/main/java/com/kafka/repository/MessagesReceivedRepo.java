package com.kafka.repository;

import com.kafka.entity.MessagesReceived;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MessagesReceivedRepo extends JpaRepository<MessagesReceived, Long> {
}