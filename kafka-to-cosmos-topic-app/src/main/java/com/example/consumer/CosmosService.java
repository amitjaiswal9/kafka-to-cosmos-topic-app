
package com.example.consumer;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class CosmosService {

    private static final Logger log = LoggerFactory.getLogger(CosmosService.class);
    private final CosmosAsyncContainer container;

    public CosmosService(@Value("${cosmos.uri}") String uri,
                         @Value("${cosmos.key}") String key,
                         @Value("${cosmos.database}") String database,
                         @Value("${cosmos.container}") String containerName) {

        CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint(uri)
                .key(key)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildAsyncClient();

        this.container = client.getDatabase(database).getContainer(containerName);
    }

    public void saveTransaction(String topic, String txnId, String rawMessage) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", UUID.randomUUID().toString());
        payload.put("transactionId", txnId);
        payload.put("topic", topic);
        payload.put("receivedAt", OffsetDateTime.now().toString());
        payload.put("raw", rawMessage);

        container.createItem(payload)
                .subscribe(response -> log.info("Saved txnId: {}", txnId),
                           error -> log.error("Cosmos write failed", error));
    }
}
