package com.parseresdb.parseresdb.Controller;

import com.fasterxml.jackson.databind.node.TextNode;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ConnectionRepository;
import com.parseresdb.parseresdb.Service.DatabaseService;
import com.parseresdb.parseresdb.Service.ElasticsearchService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/data")
public class ElasticsearchController {

    private final ElasticsearchService elasticsearchService;
    private final ConnectionRepository connectionRepository;

    public ElasticsearchController(ElasticsearchService elasticsearchService, DatabaseService databaseService, ConnectionRepository connectionRepository) {
        this.elasticsearchService = elasticsearchService;
        this.connectionRepository = connectionRepository;
    }

    @GetMapping("/fetch")
    public ResponseEntity<Map<String, JsonNode>> fetchData(
            @RequestParam String gte,
            @RequestParam String lte) throws IOException {

        List<Connection> connections = connectionRepository.findByConnectionType("elasticsearch"); // Fetch only Elasticsearch connections
        Map<String, JsonNode> responseMap = new HashMap<>();

        for (Connection connection : connections) {
            JsonNode rawData = elasticsearchService.fetchData(connection, gte, lte);
            responseMap.put(connection.getConnectionName(), rawData); // Store raw data with connection name as key
        }

        return ResponseEntity.ok(responseMap);
    }
}
