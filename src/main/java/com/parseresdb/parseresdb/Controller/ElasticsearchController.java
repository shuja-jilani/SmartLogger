package com.parseresdb.parseresdb.Controller;

import com.fasterxml.jackson.databind.node.TextNode;
import com.parseresdb.parseresdb.Service.DatabaseService;
import com.parseresdb.parseresdb.Service.ElasticsearchService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
@RequiredArgsConstructor
public class ElasticsearchController {

    private final ElasticsearchService elasticsearchService;
    private final DatabaseService databaseService;

    @GetMapping("/fetch")
    public ResponseEntity<Map<String, JsonNode>> fetchData(
            @RequestParam String connectionName,
            @RequestParam String gte,
            @RequestParam String lte) throws IOException {

        // Get datasets for the given connection
        List<String> datasets = databaseService.getDatasetsGroupedByConnection()
                .getOrDefault(connectionName, Collections.emptyList());

        if (datasets.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Collections.singletonMap("error",
                            TextNode.valueOf("No datasets found for connection: " + connectionName)));
        }

        // Map to store raw data for each dataset
        Map<String, JsonNode> rawDataByDataset = new HashMap<>();

        for (String dataset : datasets) {
            JsonNode rawData = elasticsearchService.fetchData(connectionName, dataset, gte, lte);
            rawDataByDataset.put(dataset, rawData);
        }

        return ResponseEntity.ok(rawDataByDataset);
    }
}
