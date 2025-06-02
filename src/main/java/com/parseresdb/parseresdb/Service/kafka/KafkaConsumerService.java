package com.parseresdb.parseresdb.Service.kafka;

import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Service.DatabaseService;
import com.parseresdb.parseresdb.Service.ElasticsearchService;
import com.parseresdb.parseresdb.Service.TransformDataService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


@Service
public class KafkaConsumerService {

    private final TransformDataService transformDataService;
    private final ElasticsearchService elasticsearchService;
    private final KafkaProducerService kafkaProducerService;
    // Atomic counter for thread-safe tracking
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final DatabaseService databaseService;

    public KafkaConsumerService(TransformDataService transformDataService, ElasticsearchService elasticsearchService, KafkaProducerService kafkaProducerService, DatabaseService databaseService) {
        this.transformDataService = transformDataService;
        this.elasticsearchService = elasticsearchService;
        this.kafkaProducerService = kafkaProducerService;
        this.databaseService = databaseService;
    }

    @KafkaListener(topics = "raw-data-topic", groupId = "transform-group")
    public void consumeRawData(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageNode = objectMapper.readTree(message);
            Connection connection = objectMapper.treeToValue(messageNode.get("connection"), Connection.class);

            String connectionType = connection.getConnectiontype();
            System.out.println("DEBUG: Connection Type = " + connectionType);
            Map<String, Object> transformedData = null;
            if ("elasticsearch".equalsIgnoreCase(connectionType)) {

                JsonNode rawData = messageNode.get("source");

                System.out.println("Received from Kafka: " + rawData);

                // Process each record using transformData
                transformedData = transformDataService.transformData(rawData, connection);
            } else if ("database".equalsIgnoreCase(connectionType)) {
                JsonNode rawData = messageNode.get("source");

                System.out.println("Received from Kafka (DB): " + rawData);
                transformedData = transformDataService.transformDBData(rawData, connection);

            }
            boolean isIndexed = false;
            // Send transformed data to Elasticsearch
            if(transformedData!=null) {
                isIndexed = elasticsearchService.indexToElasticsearch(transformedData);
            }
            if (isIndexed){
                processedCount.incrementAndGet();
                System.out.println("Data was indexed successfully, count increment.");// Increment counter when data is indexed successfully
            }else {
                System.out.println("Data was not indexed successfully. Skipping count increment.");
            }

        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
        }
    }

    // Expose a method to get the processed count
    public int getProcessedCount() {
        return processedCount.get();
    }

    @KafkaListener(topics = "connection-topic", groupId = "connection-group")
    public void consumeConnectionData(String message) {
        try {
            // Deserialize JSON into Connection class
            ObjectMapper objectMapper = new ObjectMapper();
//            objectMapper.registerModule(new JavaTimeModule());
//            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            JsonNode messageNode = objectMapper.readTree(message);

            Connection connection = objectMapper.treeToValue(messageNode.get("connection"), Connection.class);
            String gte = messageNode.get("gte").asText();
            String lte = messageNode.get("lte").asText();
            String connectionType = connection.getConnectiontype();
            System.out.println("DEBUG: Connection Type = " + connectionType);
            if ("elasticsearch".equalsIgnoreCase(connectionType)) {

                // Fetch from Elasticsearch
                JsonNode rawData = elasticsearchService.fetchData(connection, gte, lte);

                JsonNode hitsArray = rawData.path("hits").path("hits");
                if (hitsArray != null && hitsArray.isArray()) {
                    for (JsonNode hit : hitsArray) {
                        JsonNode source = hit.get("_source");

                        ObjectNode enrichedMessage = objectMapper.createObjectNode();
                        enrichedMessage.putPOJO("connection", connection);
                        enrichedMessage.set("source", source);

                        System.out.println("DEBUG: Sending to Kafka -> " + enrichedMessage);
                        kafkaProducerService.sendRawData(enrichedMessage);
                    }
                } else {
                    System.out.println("DEBUG: No hits found in Elasticsearch response.");
                }

            } else if ("database".equalsIgnoreCase(connectionType)) {
                try {

                    List<Map<String, Object>> records = databaseService.fetchTableData(connection, gte, lte);

                    for (Map<String, Object> row : records) {
                        ObjectNode enrichedMessage = objectMapper.createObjectNode();

                        // 🔽 Convert timestamps to formatted strings manually
                        Map<String, Object> normalizedRow = new HashMap<>();
                        for (Map.Entry<String, Object> entry : row.entrySet()) {
                            Object value = entry.getValue();
                            if (value instanceof Timestamp) {
                                String formatted = value.toString(); // or use DateTimeFormatter if needed
                                normalizedRow.put(entry.getKey(), formatted);
                            } else {
                                normalizedRow.put(entry.getKey(), value);
                            }
                        }
                        enrichedMessage.putPOJO("connection", connection);
                        enrichedMessage.set("source", objectMapper.valueToTree(normalizedRow));

                        System.out.println("DEBUG: Sending DB record to Kafka -> " + enrichedMessage);
                        kafkaProducerService.sendRawData(enrichedMessage);
                    }

                    if (records.isEmpty()) {
                        System.out.println("DEBUG: No records found in DB table for given time range.");
                    }

                } catch (Exception e) {
                    System.err.println("ERROR: Failed to fetch or send DB records: " + e.getMessage());
                    e.printStackTrace();
                }

            } else {
                System.out.println("WARN: Unsupported connection type: " + connectionType);
            }

        } catch (Exception e) {
            System.err.println("Error processing connection message: " + e.getMessage());
            e.printStackTrace();
        }
    }

}


