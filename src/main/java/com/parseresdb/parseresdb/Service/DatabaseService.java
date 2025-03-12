package com.parseresdb.parseresdb.Service;

import com.parseresdb.parseresdb.Entity.ApiMetadata;
import com.parseresdb.parseresdb.Entity.ApiMetadataField;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ApiMetadataFieldRepository;
import com.parseresdb.parseresdb.Repository.ApiMetadataRepository;
import com.parseresdb.parseresdb.Repository.ConnectionRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DatabaseService {

    private final ConnectionRepository connectionRepository;
    private final JdbcTemplate jdbcTemplate;

    public DatabaseService(JdbcTemplate jdbcTemplate,ConnectionRepository connectionRepository, ApiMetadataRepository apiMetadataRepository, ApiMetadataFieldRepository apiMetadataFieldRepository) {
        this.connectionRepository = connectionRepository;
        this.apiMetadataRepository = apiMetadataRepository;
        this.apiMetadataFieldRepository = apiMetadataFieldRepository;
        this.jdbcTemplate = jdbcTemplate;

    }

    private final ApiMetadataRepository apiMetadataRepository;
    private final ApiMetadataFieldRepository apiMetadataFieldRepository;

    /**
     * Fetches all API metadata fields by iterating over all connections.
     */
    public Map<UUID, List<ApiMetadataField>> getFieldsGroupedByApi() {
        List<Connection> connections = connectionRepository.findAll();
        if (connections.isEmpty()) {
            return Collections.emptyMap(); // Return empty map if no connections found
        }

        Map<UUID, List<ApiMetadataField>> fieldsByApi = new HashMap<>();

        for (Connection connection : connections) {
            List<ApiMetadata> metadataList = apiMetadataRepository.findByConnectionNameAndStatus(connection.getConnectionName(),"enabled");
            if (metadataList.isEmpty()) {
                continue; // Skip if no metadata found
            }

            for (ApiMetadata metadata : metadataList) {
                List<ApiMetadataField> fields = apiMetadataFieldRepository.findByApiMetadataId(metadata.getUniqueId());
                fieldsByApi.put(metadata.getUniqueId(), fields);
            }
        }

        return fieldsByApi;
    }

    public boolean checkIfApiExists(String apiName) {
        String sql = "SELECT COUNT(*) FROM api_metadata WHERE api_name = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, apiName);
        return count != null && count > 0;
    }

    public void insertApiMetadata(String uniqueId, String connectionName, String apiName, String roleNames, String status) {
        String sql = "INSERT INTO api_metadata (unique_id, connection_name, api_name, role_names, status) VALUES (?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, uniqueId, connectionName, apiName, roleNames, status);
    }
    public void insertApiMetadataField(String apiMetadataId, String fieldName, String identifier, String datatype, String contentType, String keyStatus, String path) {
        String sql = "INSERT INTO api_metadata_field (api_metadata_id, field, identifier, datatype, content_type, key_status, path) VALUES (?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, apiMetadataId, fieldName, identifier, datatype, contentType, keyStatus, path);
    }
    public Map<String, List<String>> getDatasetsGroupedByConnection() {
        List<Connection> connections = connectionRepository.findAll();
        if (connections.isEmpty()) {
            return Collections.emptyMap(); // Return empty map if no connections found
        }

        Map<String, List<String>> datasetsByConnection = new HashMap<>();

        for (Connection connection : connections) {
            List<ApiMetadata> metadataList = apiMetadataRepository.findByConnectionNameAndStatus(connection.getConnectionName(), "enabled");
            if (metadataList.isEmpty()) {
                continue; // Skip if no metadata found
            }

            Set<String> datasets = new HashSet<>();
            for (ApiMetadata metadata : metadataList) {
                if (metadata.getDataset() != null) {
                    datasets.add(metadata.getDataset());
                }
            }

            datasetsByConnection.put(connection.getConnectionName(), new ArrayList<>(datasets));
        }

        return datasetsByConnection;
    }

}
