package com.parseresdb.parseresdb.Repository;

import com.parseresdb.parseresdb.Entity.Connection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ConnectionRepository extends JpaRepository<Connection, String> {

    List<Connection> findByConnectionType(String connectiontype); // Fetch only specific connection types

    Optional<Connection> findByConnectionName(String connectionName);
}



