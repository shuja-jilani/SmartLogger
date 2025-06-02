package com.parseresdb.parseresdb.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.parseresdb.parseresdb.Entity.Connection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class AutoDiscovery {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final DatabaseService databaseService;

    public AutoDiscovery(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, DatabaseService databaseService) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.databaseService = databaseService;
    }

    public void performAutoDiscovery(Connection connection, String resourcePath) {
        try {
            // Extract top-level connection fields
            String connectionName = connection.getConnectionName();
            String connectionType = connection.getConnectiontype();

            // Parse JSON inside `details` column
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode details = objectMapper.readTree(connection.getDetails());

            // Decide between dataset or tableName based on connection type
            String dataset;
            if ("database".equalsIgnoreCase(connectionType)) {
                dataset = details.get("tableName").asText();
            } else {
                dataset = details.get("dataset").asText();
            }

            // Generate a unique ID for the new API entry
            UUID uniqueId = UUID.randomUUID();

            // Default role names
            String roleNames = "Admin";

            // Default status
            String status = "disabled";

            // Insert into api_metadata table
            databaseService.insertApiMetadata(uniqueId, connectionName, dataset, resourcePath, roleNames, status, resourcePath);
            // Insert fields into api_metadata_field table
            JsonNode fields = details.get("fields");
            if (fields != null && fields.isArray()) {
                for (JsonNode fieldNode : fields) {
                    databaseService.insertApiMetadataField(
                            uniqueId,
                            fieldNode.get("field").asText(),
                            fieldNode.get("identifier").asText(),
                            fieldNode.get("datatype").asText(),
                            fieldNode.get("contentType").asText(),
                            fieldNode.get("keyStatus").asText(),
                            fieldNode.get("path").asText()
                    );
                }
            }
            System.out.println("New API discovered and added to database: " + resourcePath);
        } catch (Exception e) {
            System.err.println("Error during API auto-discovery: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

