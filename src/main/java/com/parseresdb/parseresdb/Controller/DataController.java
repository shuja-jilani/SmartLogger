package com.parseresdb.parseresdb.Controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ConnectionRepository;
import com.parseresdb.parseresdb.Service.DatabaseService;
import com.parseresdb.parseresdb.Service.ElasticsearchService;
import com.parseresdb.parseresdb.Service.TransformDataService;
import com.fasterxml.jackson.databind.JsonNode;
import com.parseresdb.parseresdb.Service.kafka.KafkaConsumerService;
import com.parseresdb.parseresdb.Service.kafka.KafkaProducerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/data")
public class DataController {
    private final KafkaProducerService kafkaProducerService;
    private final KafkaConsumerService kafkaConsumerService;
    private final ConnectionRepository connectionRepository;

    public DataController(KafkaProducerService kafkaProducerService, KafkaConsumerService kafkaConsumerService, ConnectionRepository connectionRepository, ElasticsearchService elasticsearchService, TransformDataService transformDataService, DatabaseService databaseService) {
        this.kafkaProducerService = kafkaProducerService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.connectionRepository = connectionRepository;
        this.elasticsearchService = elasticsearchService;
        this.transformDataService = transformDataService;
        this.databaseService = databaseService;
    }

    private final ElasticsearchService elasticsearchService;
    private final TransformDataService transformDataService;
    private final DatabaseService databaseService;


//    @GetMapping("/fetch-and-transform")
//    public Map<String, List<Map<String, Object>>> fetchAndTransformData(
//            @RequestParam String gte,
//            @RequestParam String lte) throws IOException {
//
//        // Fetch datasets grouped by connection
//        Map<String, List<String>> datasetsByConnection = databaseService.getDatasetsGroupedByConnection();
//
//        // Map to store transformed data for each connection-dataset pair
//        Map<String, List<Map<String, Object>>> transformedDataMap = new HashMap<>();
//
//        for (Map.Entry<String, List<String>> entry : datasetsByConnection.entrySet()) {
//            String connectionName = entry.getKey();
//            List<String> datasets = entry.getValue();
//
//            for (String dataset : datasets) {
//                // Fetch raw data from Elasticsearch for each dataset
//                JsonNode rawData = elasticsearchService.fetchData(connectionName, dataset, gte, lte);
//
//                // Transform the raw data
//                List<Map<String, Object>> transformedData = transformDataService.transformData(rawData);
//
//                // Store transformed data with a key like "connectionName_dataset"
//                transformedDataMap.put(connectionName + "_" + dataset, transformedData);
//            }
//        }

//        return transformedDataMap;
//    }

//    @GetMapping("/fetch-and-feed")
//    public Map<String, Object> fetchAndFeedData(@RequestParam String gte, @RequestParam String lte) throws IOException {
//        Map<String, List<String>> datasetsByConnection = databaseService.getDatasetsGroupedByConnection();
//
//        for (Map.Entry<String, List<String>> entry : datasetsByConnection.entrySet()) {
//            String connectionName = entry.getKey();
//            List<String> datasets = entry.getValue();
//
//            for (String dataset : datasets) {
//                JsonNode rawData = elasticsearchService.fetchData(connectionName, dataset, gte, lte);
//
//                for (JsonNode hit : rawData.get("hits").get("hits")) {
//                    JsonNode source = hit.get("_source");
//                    kafkaProducerService.sendRawData(source);
//                }
//
//            }
//        }
//
//
//        // Return a message indicating the request was sent
//        Map<String, Object> response = new HashMap<>();
//        response.put("message", "Data fetching and publishing started.");
//        return response;
//    }
@GetMapping("/fetch-and-publish")
public Map<String, Object> fetchAndPublish(@RequestParam String gte, @RequestParam String lte) {
    List<Connection> connections = connectionRepository.findByConnectionType("elasticsearch"); // Filter by type

    if (connections.isEmpty()) {
        return Collections.singletonMap("message", "No Elasticsearch connections found.");
    }

    for (Connection connection : connections) {
        ObjectNode message = new ObjectMapper().createObjectNode();
        message.putPOJO("connection", connection);
        message.put("gte", gte);
        message.put("lte", lte);

        kafkaProducerService.sendConnectionData(message);
    }

    return Collections.singletonMap("message", "Connections published to Kafka.");
}
@GetMapping("/fetch-and-publish-db")
public Map<String, Object> fetchAndPublishForDB(@RequestParam String gte, @RequestParam String lte) {
    List<Connection> connections = connectionRepository.findByConnectionType("database"); // Filter by type

    if (connections.isEmpty()) {
        return Collections.singletonMap("message", "No Elasticsearch connections found.");
    }

    for (Connection connection : connections) {
        ObjectNode message = new ObjectMapper().createObjectNode();
        message.putPOJO("connection", connection);
        message.put("gte", gte);
        message.put("lte", lte);

        kafkaProducerService.sendConnectionData(message);
    }

    return Collections.singletonMap("message", "Connections published to Kafka.");
}

    @GetMapping("/processed-count")
    public Map<String, Object> getProcessedCount() {
        Map<String, Object> response = new HashMap<>();
        response.put("processedCount", kafkaConsumerService.getProcessedCount());
        return response;
    }




}
