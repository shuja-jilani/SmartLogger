package com.parseresdb.parseresdb.Service;

import com.parseresdb.parseresdb.Entity.ApiMetadata;
import com.parseresdb.parseresdb.Entity.ApiMetadataField;
import com.parseresdb.parseresdb.Repository.ApiMetadataFieldRepository;
import com.parseresdb.parseresdb.Repository.ApiMetadataRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class TransformDataService {

    private final DatabaseService databaseService;
    private final ApiMetadataRepository apiMetadataRepository;
    private final ObjectMapper objectMapper;
    private final ApiMetadataFieldRepository apiMetadataFieldRepository;

    public TransformDataService(DatabaseService databaseService, ApiMetadataRepository apiMetadataRepository, ObjectMapper objectMapper, ApiMetadataFieldRepository apiMetadataFieldRepository) {
        this.databaseService = databaseService;
        this.apiMetadataRepository = apiMetadataRepository;
        this.objectMapper = objectMapper;
        this.apiMetadataFieldRepository = apiMetadataFieldRepository;
    }
    public Map<String, Object> transformData(JsonNode source, String pathToResourcePath) {
        Map<String, Object> transformedData = new HashMap<>();


            // Extract resourcePath to determine the API
            Optional<JsonNode> resourcePathNode = getValueFromPath(source, pathToResourcePath);
            if (!resourcePathNode.isPresent()) {
                return null; // Skip if no resourcePath found
            }
            String resourcePath = resourcePathNode.get().asText();

            // Find the API that matches this resourcePath
            ApiMetadata matchingApi = apiMetadataRepository.findByResourcePathAndStatus(resourcePath, "enabled");
            if (matchingApi == null) {
                return null; // Skip if no matching api found
            }


            // Get fields specific to this API
            List<ApiMetadataField> fields = apiMetadataFieldRepository.findByApiMetadataId(matchingApi.getUniqueId());

            // Extract request headers and payload
            Optional<JsonNode> requestNode = getValueFromPath(source, "request");
            if (requestNode.isPresent()) {
                JsonNode request = requestNode.get();
                ObjectNode filteredRequest = objectMapper.createObjectNode();
                if (request.has("Headers")) filteredRequest.set("headers", request.get("Headers"));
                if (request.has("payload")) filteredRequest.set("payload", request.get("payload"));
                transformedData.put("request", filteredRequest);
            }

            // Extract response headers and payload
            Optional<JsonNode> responseNode = getValueFromPath(source, "response");
            if (responseNode.isPresent()) {
                JsonNode response = responseNode.get();
                ObjectNode filteredResponse = objectMapper.createObjectNode();
                if (response.has("Headers")) filteredResponse.set("headers", response.get("Headers"));
                if (response.has("payload")) filteredResponse.set("payload", response.get("payload"));
                transformedData.put("response", filteredResponse);
            }

            // Extract fields based on API-specific configuration
            List<Map<String, String>> customFields = new ArrayList<>();
            for (ApiMetadataField field : fields) {
                String path = field.getPath();
                String fieldName = field.getField();
                String keyStatus = field.getKey_status();

                Optional<JsonNode> valueNode;
                if (path.contains(".payload.")) {
                    Optional<JsonNode> payloadNode = getValueFromPath(source, "request.payload");
                    if (payloadNode.isPresent()) {
                        try {
                            JsonNode payloadJson = objectMapper.readTree(payloadNode.get().asText());
                            valueNode = getValueFromPath(payloadJson, path.replace("request.payload.", ""));
                        } catch (Exception e) {
                            valueNode = Optional.empty();
                        }
                    } else {
                        valueNode = Optional.empty();
                    }
                } else {
                    valueNode = getValueFromPath(source, path);
                }

                if (valueNode.isPresent()) {
                    String value = valueNode.get().asText();
                    if ("Custom".equalsIgnoreCase(keyStatus)) {
                        Map<String, String> customFieldEntry = new HashMap<>();
                        customFieldEntry.put("key", fieldName);
                        customFieldEntry.put("value", value);
                        customFields.add(customFieldEntry);
                    } else {
                        transformedData.put(fieldName, value);
                    }
                }
            }

            // Add CustomField list if not empty
            if (!customFields.isEmpty()) {
                transformedData.put("CustomField", customFields);
            }

            // Compute ElapsedTime
            if (transformedData.containsKey("RequestTime") && transformedData.containsKey("ResponseTime")) {
                try {
                    String requestTime = transformedData.get("RequestTime").toString();
                    String responseTime = transformedData.get("ResponseTime").toString();

                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
                    LocalDateTime requestDateTime = LocalDateTime.parse(requestTime, formatter);
                    LocalDateTime responseDateTime = LocalDateTime.parse(responseTime, formatter);

                    long elapsed = Duration.between(requestDateTime, responseDateTime).toMillis();
                    transformedData.put("ElapsedTime", elapsed);
                } catch (Exception e) {
                    transformedData.put("ElapsedTime", "Invalid Time Format: " + e.getMessage());
                }
            }


            // Add API Name
            transformedData.put("APIName", matchingApi.getApi_name());
            transformedData.put("ResourcePath", matchingApi.getResourcePath());

            // Add Role Names as JSON Array
            String roleNames = matchingApi.getRoleNames();
            if (roleNames != null && !roleNames.isEmpty()) {
                List<String> roles = Arrays.asList(roleNames.split(","));
                ArrayNode rolesArrayNode = objectMapper.createArrayNode();
                roles.forEach(role -> rolesArrayNode.add(role.trim()));
                transformedData.put("Role", rolesArrayNode);
            }

        return transformedData;
    }

    private Optional<JsonNode> getValueFromPath(JsonNode node, String path) {
        String[] keys = path.split("\\.");
        JsonNode current = node;
        for (String key : keys) {
            if (current != null && current.has(key)) {
                current = current.get(key);
            } else {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(current);
    }

}

