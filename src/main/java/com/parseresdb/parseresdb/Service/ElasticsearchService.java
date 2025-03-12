package com.parseresdb.parseresdb.Service;

import com.parseresdb.parseresdb.Entity.ApiMetadata;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ApiMetadataRepository;
import com.parseresdb.parseresdb.Repository.ConnectionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ElasticsearchService {
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final ConnectionRepository connectionRepository;

    @Autowired
    public ElasticsearchService(RestTemplate restTemplate, ObjectMapper objectMapper, ConnectionRepository connectionRepository, ApiMetadataRepository apiMetadataRepository) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.connectionRepository = connectionRepository;
        this.apiMetadataRepository = apiMetadataRepository;
    }

    private  ApiMetadataRepository apiMetadataRepository;

    public JsonNode fetchData(String connectionName, String dataset, String gte, String lte) throws IOException {
        // Step 1: Fetch clusterUrl from connections table
        Optional<Connection> connectionOpt = connectionRepository.findByConnectionName(connectionName);
        if (connectionOpt.isEmpty()) {
            throw new RuntimeException("Connection not found: " + connectionName);
        }

        String clusterUrl = extractClusterUrl(connectionOpt.get().getDetails());

        // Construct Elasticsearch URL
        String url = clusterUrl + "/" + dataset + "/_search";

        // Step 2: Build Elasticsearch Query
        String queryJson = "{\n" +
                "  \"size\": 1000,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"RequestTime\": {\n" +
                "              \"gte\": \"" + gte + "\",\n" +
                "              \"lte\": \"" + lte + "\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";



        // Step 3: Execute request
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(queryJson, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

        return objectMapper.readTree(response.getBody());
    }


    private String extractClusterUrl(String detailsJson) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(detailsJson);
        return jsonNode.get("clusterURL").asText();
    }
    // Helper method to send data to Elasticsearch
    public boolean indexToElasticsearch(Map<String, Object> data) {
        try {
            String esUrl = "http://localhost:9200/my_buzzhub_index4/_doc";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(data), headers);

            ResponseEntity<String> response = restTemplate.postForEntity(esUrl, request, String.class);


            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            System.err.println("Error indexing document: " + e.getMessage());
            return false;
        }
    }
}
