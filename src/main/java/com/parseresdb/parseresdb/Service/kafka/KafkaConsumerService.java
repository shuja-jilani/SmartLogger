package com.parseresdb.parseresdb.Service.kafka;

import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Service.ElasticsearchService;
import com.parseresdb.parseresdb.Service.TransformDataService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


@Service
public class KafkaConsumerService {

    private final TransformDataService transformDataService;
    private final ElasticsearchService elasticsearchService;
    private final KafkaProducerService kafkaProducerService;
    // Atomic counter for thread-safe tracking
    private final AtomicInteger processedCount = new AtomicInteger(0);
    public KafkaConsumerService(TransformDataService transformDataService, ElasticsearchService elasticsearchService, KafkaProducerService kafkaProducerService) {
        this.transformDataService = transformDataService;
        this.elasticsearchService = elasticsearchService;
        this.kafkaProducerService = kafkaProducerService;
    }

    @KafkaListener(topics = "raw-data-topic", groupId = "transform-group")
    public void consumeRawData(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageNode = objectMapper.readTree(message);

            JsonNode rawData = messageNode.get("source");
            String pathToResourcePath = messageNode.get("pathToResourcePath").asText();

            System.out.println("Received from Kafka: " + rawData);

            // Process each record using transformData
            Map<String, Object> transformedData = transformDataService.transformData(rawData, pathToResourcePath);

            // Send transformed data to Elasticsearch
            boolean isIndexed = elasticsearchService.indexToElasticsearch(transformedData);
            if (isIndexed) {
                processedCount.incrementAndGet(); // Increment counter when data is indexed successfully
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
            JsonNode messageNode = objectMapper.readTree(message);

            Connection connection = objectMapper.treeToValue(messageNode.get("connection"), Connection.class);
            String gte = messageNode.get("gte").asText();
            String lte = messageNode.get("lte").asText();

            // Fetch data from Elasticsearch
            JsonNode rawData = elasticsearchService.fetchData(connection, gte, lte);

            // Extract `pathToResourcePath`
            JsonNode details = objectMapper.readTree(connection.getDetails());
            String pathToResourcePath = "ResourcePath"; // Default
            for (JsonNode field : details.get("fields")) {
                if ("ResourcePath".equals(field.get("field").asText())) {
                    pathToResourcePath = field.get("path").asText();
                    break;
                }
            }

            // Publish each record to Kafka along with `pathToResourcePath`
            for (JsonNode hit : rawData.get("hits").get("hits")) {
                JsonNode source = hit.get("_source");

                ObjectNode enrichedMessage = objectMapper.createObjectNode();
                enrichedMessage.set("source", source);
                enrichedMessage.put("pathToResourcePath", pathToResourcePath);
                System.out.println("DEBUG: Sending to Kafka -> " + enrichedMessage); // Debug line
                kafkaProducerService.sendRawData(enrichedMessage);
            }

            System.out.println("Processed connection: " + connection.getConnectionName() + " | Data sent to raw-data-topic");

        } catch (Exception e) {
            System.err.println("Error processing connection message: " + e.getMessage());
        }
    }

}


