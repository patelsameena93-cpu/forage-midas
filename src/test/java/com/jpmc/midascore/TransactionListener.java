package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(String message) {

        try {
            System.out.println("Raw message received: " + message);

            Transaction tx = objectMapper.readValue(message, Transaction.class);

            System.out.println("Received Transaction:");
            System.out.println("Sender ID: " + tx.getSenderId());
            System.out.println("Recipient ID: " + tx.getRecipientId());
            System.out.println("Amount: " + tx.getAmount());
        } catch (Exception e) {
            System.out.println("Failed to process message: " + message);
            e.printStackTrace();
        }
    }
}
