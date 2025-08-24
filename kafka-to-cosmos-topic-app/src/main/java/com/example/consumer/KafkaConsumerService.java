
package com.example.consumer;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final String extractor;
    private final String extractor2;
    private final CosmosService cosmosService;

    public KafkaConsumerService(@Value("${kafka.extractor}") String extractor, @Value("${kafka.extractor2}") String extractor2,
                                CosmosService cosmosService) {
        this.extractor = extractor;
        this.extractor2 = extractor2;
        this.cosmosService = cosmosService;
    }

    @KafkaListener(topics = "${kafka.topic-name}", groupId = "topic-specific-group")
    public void consume(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Received message from {}: {}", topic, message);
        try {
            Object doc = Configuration.defaultConfiguration().jsonProvider().parse(message);
            String txnId = JsonPath.read(doc, extractor);
            String pkgId = JsonPath.read(doc, extractor2);
            log.info("txnId - pkgId  message from {}: {}", txnId, pkgId);
            cosmosService.saveTransaction(topic, txnId, message);
        } catch (Exception e) {
            log.error("Extraction or save failed", e);
        }
    }
}
