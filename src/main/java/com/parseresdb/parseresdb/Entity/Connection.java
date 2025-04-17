package com.parseresdb.parseresdb.Entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "connections")
public class Connection {
    @Id
    @Column(name = "connectionname")
    private String connectionName;
@Column(name = "connectiontype")
    private String connectionType;

    public String getConnectiontype() {
        return connectionType;
    }

    public void setConnectiontype(String connectiontype) {
        this.connectionType = connectiontype;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Column(name = "details", length = 2000) // Store JSON as text
    private String details;
    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

}
