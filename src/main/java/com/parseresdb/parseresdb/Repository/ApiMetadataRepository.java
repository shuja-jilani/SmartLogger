package com.parseresdb.parseresdb.Repository;

import com.parseresdb.parseresdb.Entity.ApiMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ApiMetadataRepository extends JpaRepository<ApiMetadata, UUID> {
    List<ApiMetadata> findByConnectionNameAndStatus(String connectionName, String status);
    ApiMetadata findByResourcePath(String resourcePath);
}