package com.parseresdb.parseresdb.Repository;

import com.parseresdb.parseresdb.Entity.ApiMetadataField;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ApiMetadataFieldRepository extends JpaRepository<ApiMetadataField, Long> {
    List<ApiMetadataField> findByApiMetadataId(UUID apiMetadataId);
}
