package com.parseresdb.parseresdb.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.parseresdb.parseresdb.Entity.ApiMetadata;
import com.parseresdb.parseresdb.Entity.ApiMetadataField;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ApiMetadataFieldRepository;
import com.parseresdb.parseresdb.Repository.ApiMetadataRepository;
import com.parseresdb.parseresdb.Repository.ConnectionRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.*;

@Service
public class DatabaseService {

    private final ConnectionRepository connectionRepository;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public DatabaseService(JdbcTemplate jdbcTemplate, ConnectionRepository connectionRepository, ApiMetadataRepository apiMetadataRepository, ApiMetadataFieldRepository apiMetadataFieldRepository) {
        this.connectionRepository = connectionRepository;
        this.apiMetadataRepository = apiMetadataRepository;
        this.apiMetadataFieldRepository = apiMetadataFieldRepository;
        this.jdbcTemplate = jdbcTemplate;

    }

    private final ApiMetadataRepository apiMetadataRepository;
    private final ApiMetadataFieldRepository apiMetadataFieldRepository;

//    /**
//     * Fetches all API metadata fields by iterating over all connections.
//     */
//    public Map<UUID, List<ApiMetadataField>> getFieldsGroupedByApi() {
//        List<Connection> connections = connectionRepository.findAll();
//        if (connections.isEmpty()) {
//            return Collections.emptyMap(); // Return empty map if no connections found
//        }
//
//        Map<UUID, List<ApiMetadataField>> fieldsByApi = new HashMap<>();
//
//        for (Connection connection : connections) {
//            List<ApiMetadata> metadataList = apiMetadataRepository.findByConnectionNameAndStatus(connection.getConnectionName(),"enabled");
//            if (metadataList.isEmpty()) {
//                continue; // Skip if no metadata found
//            }
//
//            for (ApiMetadata metadata : metadataList) {
//                List<ApiMetadataField> fields = apiMetadataFieldRepository.findByApiMetadataId(metadata.getUniqueId());
//                fieldsByApi.put(metadata.getUniqueId(), fields);
//            }
//        }
//
//        return fieldsByApi;
//    }

    public boolean checkIfApiExists(String apiName) {
        String sql = "SELECT COUNT(*) FROM api_metadata WHERE api_name = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, apiName);
        return count != null && count > 0;
    }

    public void insertApiMetadata(UUID uniqueId, String connectionName, String dataset, String apiName, String roleNames, String status, String resourcePath) {
        String sql = "INSERT INTO api_metadata (unique_id, connection_name, dataset, api_name, role_names, status, resource_path) VALUES (?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, uniqueId, connectionName, dataset, apiName, roleNames, status, resourcePath);
    }

    public void insertApiMetadataField(UUID apiMetadataId, String fieldName, String identifier, String datatype, String contentType, String keyStatus, String path) {
        String sql = "INSERT INTO api_metadata_field (api_metadata_id, field, identifier, datatype, content_type, key_status, path) VALUES (?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, apiMetadataId, fieldName, identifier, datatype, contentType, keyStatus, path);
    }

    //    public Map<String, List<String>> getDatasetsGroupedByConnection() {
//        List<Connection> connections = connectionRepository.findAll();
//        if (connections.isEmpty()) {
//            return Collections.emptyMap(); // Return empty map if no connections found
//        }
//
//        Map<String, List<String>> datasetsByConnection = new HashMap<>();
//
//        for (Connection connection : connections) {
//            List<ApiMetadata> metadataList = apiMetadataRepository.findByConnectionNameAndStatus(connection.getConnectionName(), "enabled");
//            if (metadataList.isEmpty()) {
//                continue; // Skip if no metadata found
//            }
//
//            Set<String> datasets = new HashSet<>();
//            for (ApiMetadata metadata : metadataList) {
//                if (metadata.getDataset() != null) {
//                    datasets.add(metadata.getDataset());
//                }
//            }
//
//            datasetsByConnection.put(connection.getConnectionName(), new ArrayList<>(datasets));
//        }
//
//        return datasetsByConnection;
//    }
    public List<Map<String, Object>> fetchTableData(Connection connection, String gte, String lte) throws Exception {
        JsonNode details = objectMapper.readTree(connection.getDetails());

        // Extract DB connection info
        String host = details.get("host").asText();
        int port = details.get("port").asInt();
        String dbName = details.get("databaseName").asText();
        String user = details.get("userName").asText();
        String password = details.get("password").asText();
        String tableName = details.get("tableName").asText();

        // Build JDBC URL
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, dbName);

        // Set up DataSource and JdbcTemplate
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setDriverClassName("org.postgresql.Driver");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);


        // Check for presence of 'fields' or fallback to 'sqlQuery'
        if (details.has("fields") && details.get("fields").isArray() && details.get("fields").size() > 0) {
            //  Use dynamic time-based SQL (based on RequestTime field)
            String requestTimeColumn = "RequestTime"; // default
            for (JsonNode field : details.get("fields")) {
                if ("RequestTime".equals(field.get("field").asText())) {
                    requestTimeColumn = field.get("identifier").asText();
                    break;
                }
            }

            String sql = String.format("SELECT * FROM \"%s\" WHERE \"%s\" >= ? AND \"%s\" <= ?",
                    tableName, requestTimeColumn, requestTimeColumn);

            Timestamp gteTimestamp = Timestamp.valueOf(gte.replace("T", " "));
            Timestamp lteTimestamp = Timestamp.valueOf(lte.replace("T", " "));

            return jdbcTemplate.queryForList(sql, gteTimestamp, lteTimestamp);

        } else if (details.has("sqlQuery") && !details.get("sqlQuery").isNull()) {
            // Use SQL query from connection details
            String sql = details.get("sqlQuery").asText();

            // Convert gte/lte to Timestamp for use in parameterized SQL
            Timestamp gteTimestamp = Timestamp.valueOf(gte.replace("T", " "));
            Timestamp lteTimestamp = Timestamp.valueOf(lte.replace("T", " "));

            //  Check if query contains '?' placeholders (naive but safe check)
            int placeholderCount = sql.length() - sql.replace("?", "").length();
            if (placeholderCount >= 2) {
                // Query has parameter placeholders, bind gte and lte
                return jdbcTemplate.queryForList(sql, gteTimestamp, lteTimestamp);
            } else {
                // If no placeholders — execute raw SQL without params
                return jdbcTemplate.queryForList(sql);
            }
        }
        else {
            throw new IllegalStateException("No valid 'fields' or 'sqlQuery' found in connection details.");
        }
    }
}
