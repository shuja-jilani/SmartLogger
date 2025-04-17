package com.parseresdb.parseresdb.Service.kafka;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String RAW_DATA_TOPIC = "raw-data-topic";
    private static final String CONNECTION_TOPIC = "connection-topic"; // New topic for connections
    private final ObjectMapper objectMapper = new ObjectMapper(); // JSON Mapper

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Send raw data records to Kafka
    public void sendRawData(JsonNode rawData) {
        try {
            String jsonString = objectMapper.writeValueAsString(rawData);
            kafkaTemplate.send(RAW_DATA_TOPIC, jsonString);
            System.out.println("Sent to Kafka (Raw Data): " + jsonString);
        } catch (Exception e) {
            System.err.println("Error serializing raw data JSON: " + e.getMessage());
        }
    }

    // Send connection details + gte/lte to Kafka
    public void sendConnectionData(JsonNode connectionData) {
        try {
            String jsonString = objectMapper.writeValueAsString(connectionData);
            kafkaTemplate.send(CONNECTION_TOPIC, jsonString);
            System.out.println("Sent to Kafka (Connection Data): " + jsonString);
        } catch (Exception e) {
            System.err.println("Error serializing connection data JSON: " + e.getMessage());
        }
    }
}



