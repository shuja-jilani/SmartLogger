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
import java.util.stream.Collectors;

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

    public JsonNode fetchData(Connection connection, String gte, String lte) throws IOException {
        // Extract details JSON
        JsonNode details = objectMapper.readTree(connection.getDetails());
        String clusterUrl = details.get("clusterURL").asText();
        String dataset = details.get("dataset").asText();

        // Extract identifier for RequestTime
        String timeField = "RequestTime"; // Default if not found
        // Extract identifier for ResourcePath
        String resourcePathField = "ResourcePath"; // Default if not found
        for (JsonNode field : details.get("fields")) {
            if ("RequestTime".equals(field.get("field").asText())) {
                timeField = field.get("identifier").asText();

            }
            if ("ResourcePath".equals(field.get("field").asText())) {
                resourcePathField = field.get("path").asText();

            }
        }

        // Fetch enabled APIs and extract their resource paths
        List<ApiMetadata> enabledApis = apiMetadataRepository.findByConnectionNameAndStatus(connection.getConnectionName(), "enabled");
        List<String> enabledResourcePaths = enabledApis.stream()
                .map(ApiMetadata::getResourcePath)
                .collect(Collectors.toList());

        // Construct Elasticsearch Query with timeField and ResourcePath filtering
        String queryJson = "{\n" +
                "  \"size\": 1000,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"" + timeField + "\": {\n" +
                "              \"gte\": \"" + gte + "\",\n" +
                "              \"lte\": \"" + lte + "\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"terms\": {\n" +
                "            \"" + resourcePathField + "\": " + objectMapper.writeValueAsString(enabledResourcePaths) + "\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        // Step 3: Execute request
        String url = clusterUrl + "/" + dataset + "/_search";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(queryJson, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

        return objectMapper.readTree(response.getBody());
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
