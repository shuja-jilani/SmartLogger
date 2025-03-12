package com.parseresdb.parseresdb.Controller;

import com.parseresdb.parseresdb.Service.DatabaseService;
import com.parseresdb.parseresdb.Service.ElasticsearchService;
import com.parseresdb.parseresdb.Service.TransformDataService;
import com.fasterxml.jackson.databind.JsonNode;
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
public class DataController {
    public DataController(ElasticsearchService elasticsearchService, TransformDataService transformDataService, DatabaseService databaseService) {
        this.elasticsearchService = elasticsearchService;
        this.transformDataService = transformDataService;
        this.databaseService = databaseService;
    }

    private final ElasticsearchService elasticsearchService;
    private final TransformDataService transformDataService;
    private final DatabaseService databaseService;


    @GetMapping("/fetch-and-transform")
    public Map<String, List<Map<String, Object>>> fetchAndTransformData(
            @RequestParam String gte,
            @RequestParam String lte) throws IOException {

        // Fetch datasets grouped by connection
        Map<String, List<String>> datasetsByConnection = databaseService.getDatasetsGroupedByConnection();

        // Map to store transformed data for each connection-dataset pair
        Map<String, List<Map<String, Object>>> transformedDataMap = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : datasetsByConnection.entrySet()) {
            String connectionName = entry.getKey();
            List<String> datasets = entry.getValue();

            for (String dataset : datasets) {
                // Fetch raw data from Elasticsearch for each dataset
                JsonNode rawData = elasticsearchService.fetchData(connectionName, dataset, gte, lte);

                // Transform the raw data
                List<Map<String, Object>> transformedData = transformDataService.transformData(rawData);

                // Store transformed data with a key like "connectionName_dataset"
                transformedDataMap.put(connectionName + "_" + dataset, transformedData);
            }
        }

        return transformedDataMap;
    }

    @GetMapping("/fetch-and-feed")
    public Map<String, Object> fetchAndFeedData(@RequestParam String gte, @RequestParam String lte) throws IOException {
        Map<String, List<String>> datasetsByConnection = databaseService.getDatasetsGroupedByConnection();

        int totalSuccessCount = 0;

        for (Map.Entry<String, List<String>> entry : datasetsByConnection.entrySet()) {
            String connectionName = entry.getKey();
            List<String> datasets = entry.getValue();

            for (String dataset : datasets) {
                // Fetch raw data from Elasticsearch
                JsonNode rawData = elasticsearchService.fetchData(connectionName, dataset, gte, lte);

                // Transform the raw data
                List<Map<String, Object>> transformedDataList = transformDataService.transformData(rawData);

                // Push each transformed record to Elasticsearch
                for (Map<String, Object> transformedData : transformedDataList) {
                    boolean isIndexed = elasticsearchService.indexToElasticsearch(transformedData);
                    if (isIndexed) {
                        totalSuccessCount++;
                    }
                }
            }
        }

        // Return the count of successfully indexed documents
        Map<String, Object> response = new HashMap<>();
        response.put("indexedCount", totalSuccessCount);
        return response;
    }





}
