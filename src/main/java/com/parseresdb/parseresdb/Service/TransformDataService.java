package com.parseresdb.parseresdb.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.parseresdb.parseresdb.Entity.ApiMetadata;
import com.parseresdb.parseresdb.Entity.ApiMetadataField;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ApiMetadataFieldRepository;
import com.parseresdb.parseresdb.Repository.ApiMetadataRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
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
    private final AutoDiscovery autoDiscovery;

    public TransformDataService(DatabaseService databaseService, ApiMetadataRepository apiMetadataRepository, ObjectMapper objectMapper, ApiMetadataFieldRepository apiMetadataFieldRepository, AutoDiscovery autoDiscovery) {
        this.databaseService = databaseService;
        this.apiMetadataRepository = apiMetadataRepository;
        this.objectMapper = objectMapper;
        this.apiMetadataFieldRepository = apiMetadataFieldRepository;
        this.autoDiscovery = autoDiscovery;
    }
    public  Map<String, Object> transformData(JsonNode source, Connection connection) throws JsonProcessingException {
        Map<String, Object> transformedData = new HashMap<>();
        String contentType = "date"; // default
        String requestPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"; // fallback pattern

        // Extract `pathToResourcePath`
        JsonNode details = objectMapper.readTree(connection.getDetails());
        String pathToResourcePath = "ResourcePath"; // Default
        for (JsonNode field : details.get("fields")) {
            if ("ResourcePath".equals(field.get("field").asText())) {
                pathToResourcePath = field.get("path").asText();
            }
            if ("RequestTime".equals(field.get("field").asText())) {
                contentType = field.get("contentType").asText();
            }
        }
        // Override pattern from 'patterns' section if available and contentType is 'date'
        if ("date".equalsIgnoreCase(contentType) && details.has("patterns") && details.get("patterns").has("RequestTime")) {
            requestPattern = details.get("patterns").get("RequestTime").asText();
        }
            // Extract resourcePath to determine the API
            Optional<JsonNode> resourcePathNode = getValueFromPath(source, pathToResourcePath);
            if (!resourcePathNode.isPresent()) {
                return null; // Skip if no resourcePath found
            }
            String resourcePath = resourcePathNode.get().asText();

        // Find the API that matches this resourcePath
        // If API is not found, trigger auto-discovery
        ApiMetadata matchingApi = apiMetadataRepository.findByResourcePath(resourcePath);
        if (matchingApi == null) {
            autoDiscovery.performAutoDiscovery(connection, resourcePath);

            return null; // Skip if no matching api found
        }
        if(Objects.equals(matchingApi.getStatus(), "disabled")){
            return null;
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
                    if ("date".equalsIgnoreCase(contentType)) {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(requestPattern);
                        LocalDateTime requestDateTime = LocalDateTime.parse(transformedData.get("RequestTime").toString(), formatter);
                        LocalDateTime responseDateTime = LocalDateTime.parse(transformedData.get("ResponseTime").toString(), formatter);

                        long elapsed = Duration.between(requestDateTime, responseDateTime).toMillis();
                        transformedData.put("ElapsedTime", elapsed);

                    }else if ("epoch".equalsIgnoreCase(contentType)) {
                        long requestEpoch = Long.parseLong(transformedData.get("RequestTime").toString());
                        long responseEpoch = Long.parseLong(transformedData.get("ResponseTime").toString());
                        long elapsed = responseEpoch - requestEpoch;
                        transformedData.put("ElapsedTime", elapsed);

                        // Format epoch to ISO
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                        transformedData.put("RequestTime", dateFormat.format(new Date(requestEpoch)));
                        transformedData.put("ResponseTime", dateFormat.format(new Date(responseEpoch)));
                    }
                } catch (Exception e) {
                    transformedData.put("ElapsedTime", "Invalid Time Format: " + e.getMessage());
                }
            }


            // Add API Name
            transformedData.put("APIName", matchingApi.getApi_name());
            transformedData.put("ResourcePath", matchingApi.getResourcePath()); //Adding the resource path 

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

    public Map<String, Object> transformDBData(JsonNode source, Connection connection) throws JsonProcessingException {
        Map<String, Object> transformedData = new HashMap<>();
        String contentType = "date"; // default
        String requestPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"; // fallback pattern

        // Parse connection details JSON
        JsonNode details = objectMapper.readTree(connection.getDetails());

        // Default to 'ResourcePath' if not found
        String resourcePathIdentifier = "ResourcePath";

        for (JsonNode field : details.get("fields")) {
            if ("ResourcePath".equals(field.get("field").asText())) {
                resourcePathIdentifier = field.get("identifier").asText();  // This is the column name in DB
            }
            if ("RequestTime".equals(field.get("field").asText())) {
                contentType = field.get("contentType").asText();
            }
        }

        // Override request pattern if available
        if ("date".equalsIgnoreCase(contentType) && details.has("patterns") && details.get("patterns").has("RequestTime")) {
            requestPattern = details.get("patterns").get("RequestTime").asText();
        }

        // Extract resourcePath from input JSON
        JsonNode resourcePathNode = source.get(resourcePathIdentifier);
        if (resourcePathNode == null || resourcePathNode.isNull()) {
            return null; // Skip if resourcePath not present
        }
        String resourcePath = resourcePathNode.asText();

        // Try finding existing metadata
        ApiMetadata matchingApi = apiMetadataRepository.findByResourcePath(resourcePath);
        if (matchingApi == null) {
            autoDiscovery.performAutoDiscovery(connection, resourcePath);
            return null;
        }

        // Skip if API is disabled
        if ("disabled".equalsIgnoreCase(matchingApi.getStatus())) {
            return null;
        }

        // Fetch API metadata fields
        List<ApiMetadataField> fields = apiMetadataFieldRepository.findByApiMetadataId(matchingApi.getUniqueId());

        // Construct request block
        ObjectNode requestBlock = objectMapper.createObjectNode();
        if (source.has("RequestHeaders")) {
            JsonNode requestHeaders = objectMapper.readTree(source.get("RequestHeaders").asText());
            requestBlock.set("Headers", requestHeaders);
        }
        if (source.has("RequestPayload")) {
            requestBlock.put("payload", source.get("RequestPayload").asText());
        }
        transformedData.put("request", requestBlock);

        // Construct response block
        ObjectNode responseBlock = objectMapper.createObjectNode();
        if (source.has("ResponseHeaders")) {
            JsonNode responseHeaders = objectMapper.readTree(source.get("ResponseHeaders").asText());
            responseBlock.set("Headers", responseHeaders);
        }
        if (source.has("ResponsePayload")) {
            responseBlock.put("payload", source.get("ResponsePayload").asText());
        }
        transformedData.put("response", responseBlock);

// Extract fields based on API-specific configuration
        List<Map<String, String>> customFields = new ArrayList<>();

        for (ApiMetadataField field : fields) {
            String identifier = field.getIdentifier();  // DB column name
            String fieldName = field.getField();        // Output field name
            String keyStatus = field.getKey_status();   // Custom or regular

            Optional<JsonNode> valueNode = getValueFromPath(source, identifier);

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
                if ("date".equalsIgnoreCase(contentType)) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(requestPattern);
                    // 1. Parsing: convert String (from transformedData) → LocalDateTime object
                    LocalDateTime requestDateTime = LocalDateTime.parse(transformedData.get("RequestTime").toString(), formatter); //for this to work the RequestTime must match the formatter pattern
                    LocalDateTime responseDateTime = LocalDateTime.parse(transformedData.get("ResponseTime").toString(), formatter);

                    // 2. Re-formatting: convert LocalDateTime → ISO string with `T`
                    DateTimeFormatter isoFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
                    transformedData.put("RequestTime", requestDateTime.format(isoFormatter)); //
                    transformedData.put("ResponseTime", responseDateTime.format(isoFormatter));

                    // 3. Elapsed time computation
                    long elapsed = Duration.between(requestDateTime, responseDateTime).toMillis();
                    transformedData.put("ElapsedTime", elapsed);

                    //You parse to perform logic (elapsed time) and format to conform to Elasticsearch's expected format.

                }else if ("epoch".equalsIgnoreCase(contentType)) {
                    long requestEpoch = Long.parseLong(transformedData.get("RequestTime").toString());
                    long responseEpoch = Long.parseLong(transformedData.get("ResponseTime").toString());
                    long elapsed = responseEpoch - requestEpoch;
                    transformedData.put("ElapsedTime", elapsed);

                    // Format epoch to ISO
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                    transformedData.put("RequestTime", dateFormat.format(new Date(requestEpoch)));
                    transformedData.put("ResponseTime", dateFormat.format(new Date(responseEpoch)));
                }
            } catch (Exception e) {
                transformedData.put("ElapsedTime", "Invalid Time Format: " + e.getMessage());
            }
        }


        // Add API Name
        transformedData.put("APIName", matchingApi.getApi_name());
        transformedData.put("ResourcePath", matchingApi.getResourcePath()); //Adding the resource path

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

    public Map<String, Object> transformDBDataForQuery(JsonNode source, Connection connection) throws JsonProcessingException {
        Map<String, Object> transformedData = new HashMap<>();

        // Default format for timestamp conversion — to match Elasticsearch later
        String requestPattern = "yyyy-MM-dd HH:mm:ss.SSS";

        // Parse connection details
        JsonNode details = objectMapper.readTree(connection.getDetails());

        // --- Step 1: Extract ResourcePath directly ---
        JsonNode resourcePathNode = source.get("ResourcePath"); //source.get("ResourcePath") works directly because of our alias in SQL.
        if (resourcePathNode == null || resourcePathNode.isNull()) {
            System.err.println("DEBUG: ResourcePath is missing, skipping record.");
            return null;
        }
        String resourcePath = resourcePathNode.asText();

        // --- Step 2: Check API existence in DB ---
        ApiMetadata matchingApi = apiMetadataRepository.findByResourcePath(resourcePath);
        if (matchingApi == null) {
            // Auto-discover this API if not found
            autoDiscovery.performAutoDiscovery(connection, resourcePath);
            System.out.println("DEBUG: ResourcePath not found in DB, autoDiscovery triggered.");
            return null;
        }
        // Skip if API is disabled
        if ("disabled".equalsIgnoreCase(matchingApi.getStatus())) {
            return null;
        }


        // Step 3: Build Request Block
        ObjectNode requestBlock = objectMapper.createObjectNode();
        if (source.has("RequestHeaders") && !source.get("RequestHeaders").isNull()) {
            JsonNode requestHeaders = objectMapper.readTree(source.get("RequestHeaders").asText());
            requestBlock.set("Headers", requestHeaders);
        }
        if (source.has("RequestPayload") && !source.get("RequestPayload").isNull()) {
            requestBlock.put("payload", source.get("RequestPayload").asText());
        }
        transformedData.put("request", requestBlock);

        // Step 4: Build Response Block
        ObjectNode responseBlock = objectMapper.createObjectNode();
        if (source.has("ResponseHeaders") && !source.get("ResponseHeaders").isNull()) {
            JsonNode responseHeaders = objectMapper.readTree(source.get("ResponseHeaders").asText());
            responseBlock.set("Headers", responseHeaders);
        }
        if (source.has("ResponsePayload") && !source.get("ResponsePayload").isNull()) {
            responseBlock.put("payload", source.get("ResponsePayload").asText());
        }
        transformedData.put("response", responseBlock);

        // Step 5: Static fields from API metadata
        transformedData.put("APIName", matchingApi.getApi_name());
        transformedData.put("ResourcePath", matchingApi.getResourcePath());

        // Add Role Names as array
        String roleNames = matchingApi.getRoleNames();
        if (roleNames != null && !roleNames.isEmpty()) {
            List<String> roles = Arrays.asList(roleNames.split(","));
            ArrayNode rolesArrayNode = objectMapper.createArrayNode();
            roles.forEach(role -> rolesArrayNode.add(role.trim()));
            transformedData.put("Role", rolesArrayNode);
        }

        // Step 6: Direct mappings from source
        transformedData.put("TransactionID", source.get("TransactionID").asText());
        transformedData.put("Status", source.get("Status").asText(""));
        transformedData.put("Host", source.get("Host").asText(""));

        // Step 7: Timestamp parsing, reformatting, and elapsed time
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(requestPattern);
            String reqStr = source.get("RequestTime").asText();
            String resStr = source.get("ResponseTime").asText();

            LocalDateTime requestDateTime = LocalDateTime.parse(reqStr, formatter);
            LocalDateTime responseDateTime = LocalDateTime.parse(resStr, formatter);

            // Reformat to include 'T' for Elasticsearch
            DateTimeFormatter isoFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
            transformedData.put("RequestTime", requestDateTime.format(isoFormatter));
            transformedData.put("ResponseTime", responseDateTime.format(isoFormatter));

            // Compute ElapsedTime
            long elapsed = Duration.between(requestDateTime, responseDateTime).toMillis();
            transformedData.put("ElapsedTime", elapsed);
        } catch (Exception e) {
            System.err.println("Timestamp parsing error: " + e.getMessage());
            transformedData.put("RequestTime", null);
            transformedData.put("ResponseTime", null);
            transformedData.put("ElapsedTime", null);
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

