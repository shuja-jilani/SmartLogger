package com.parseresdb.parseresdb.Entity;

import jakarta.persistence.*;

import java.util.UUID;

@Entity
@Table(name = "api_metadata")
public class ApiMetadata {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID uniqueId;

    public String getApi_name() {
        return api_name;
    }

    public void setApi_name(String api_name) {
        this.api_name = api_name;
    }

    private String api_name;

    public UUID getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(UUID uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    @Column(name = "connection_name")
    private String connectionName;
    @Column(name = "dataset")
    private String dataset;

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }
    @Column(name = "role_names")
    private String roleNames;
    @Column(name = "resource_path")
    private String resourcePath;
    @Column(name = "status")  // Add this column
    private String status;
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public String getRoleNames() {
        return roleNames;
    }

    public void setRoleNames(String roleNames) {
        this.roleNames = roleNames;
    }
}
