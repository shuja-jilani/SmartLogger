package com.parseresdb.parseresdb.Controller;

import com.parseresdb.parseresdb.Entity.ApiMetadata;
import com.parseresdb.parseresdb.Entity.ApiMetadataField;
import com.parseresdb.parseresdb.Entity.Connection;
import com.parseresdb.parseresdb.Repository.ApiMetadataFieldRepository;
import com.parseresdb.parseresdb.Repository.ApiMetadataRepository;
import com.parseresdb.parseresdb.Repository.ConnectionRepository;
import com.parseresdb.parseresdb.Service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/test")
public class TestController {

    @Autowired
    private ConnectionRepository connectionRepository;

    @Autowired
    private ApiMetadataRepository apiMetadataRepository;

    @Autowired
    private ApiMetadataFieldRepository apiMetadataFieldRepository;

    // 1️⃣ Fetch all connections
    @GetMapping("/connections")
    public List<Connection> getAllConnections() {
        return connectionRepository.findAll();
    }

    // 2️⃣ Fetch API metadata for a specific connection name
    @GetMapping("/metadata/{connectionName}")
    public ResponseEntity<List<ApiMetadata>> getApiMetadata(@PathVariable String connectionName) {
        List<ApiMetadata> metadata = apiMetadataRepository.findByConnectionNameAndStatus(connectionName,"enabled");
        return metadata != null ? ResponseEntity.ok(metadata) : ResponseEntity.notFound().build();
    }

    // 3️⃣ Fetch all API metadata fields for a given uniqueId
    @GetMapping("/metadata-fields/{uniqueId}")
    public List<ApiMetadataField> getApiMetadataFields(@PathVariable UUID uniqueId) {
        return apiMetadataFieldRepository.findByApiMetadataId(uniqueId);
    }

    @Autowired
    private DatabaseService databaseService;
    // 🔥 New Endpoint: Fetch fields using DatabaseService
//    @GetMapping("/fetch-fields")
//    public ResponseEntity<Map<UUID, List<ApiMetadataField>>> fetchFields() {
//        Map<UUID, List<ApiMetadataField>> fieldsGroupedByApi = databaseService.getFieldsGroupedByApi();
//        return fieldsGroupedByApi.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(fieldsGroupedByApi);
//    }

//    @GetMapping("/datasets")
//    public ResponseEntity<Map<String, List<String>>> getDatasetsGroupedByConnection() {
//        Map<String, List<String>> datasetsByConnection = databaseService.getDatasetsGroupedByConnection();
//        return ResponseEntity.ok(datasetsByConnection);
//    }


}


