package com.parseresdb.parseresdb.Entity;

import jakarta.persistence.*;

import java.util.UUID;

@Entity
@Table(name = "api_metadata_field")
public class ApiMetadataField {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "api_metadata_id")
    private UUID apiMetadataId;

    private String field;
    private String identifier;
    private String path;
    private String datatype;
    private String contentType;
    private String key_status;

    public String getKey_status() {
        return key_status;
    }

    public void setKey_status(String key_status) {
        this.key_status = key_status;
    }

    @Transient  // This field is not persisted in the database
    private boolean wmAPIGateway;

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public UUID getApiMetadataId() { return apiMetadataId; }
    public void setApiMetadataId(UUID apiMetadataId) { this.apiMetadataId = apiMetadataId; }

    public String getField() { return field; }
    public void setField(String field) { this.field = field; }

    public String getIdentifier() { return identifier; }
    public void setIdentifier(String identifier) { this.identifier = identifier; }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }

    public String getDatatype() { return datatype; }
    public void setDatatype(String datatype) { this.datatype = datatype; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public boolean isWMAPIGateway() { return wmAPIGateway; }
    public void setWMAPIGateway(boolean wmAPIGateway) { this.wmAPIGateway = wmAPIGateway; }
}
