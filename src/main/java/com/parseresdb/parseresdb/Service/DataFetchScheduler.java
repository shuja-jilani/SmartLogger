package com.parseresdb.parseresdb.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Service
public class DataFetchScheduler {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .withZone(ZoneId.of("Asia/Riyadh")); // Adjust timezone if needed

    public DataFetchScheduler(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

//    @Scheduled(fixedRate = 120000) // Runs every 2 minutes
    public void publishConnectionsAutomatically() {
        Instant now = Instant.now();
        Instant twoMinutesAgo = now.minusSeconds(120);

        String gte = FORMATTER.format(twoMinutesAgo);
        String lte = FORMATTER.format(now);

        String esUrl = "http://localhost:8082/fetch-and-publish?gte=" + gte + "&lte=" + lte;
        String dbUrl = "http://localhost:8082/fetch-and-publish-db?gte=" + gte + "&lte=" + lte;

        try {
            // Call ES publishing endpoint
            String esResponse = restTemplate.getForObject(esUrl, String.class);
            if (esResponse != null) {
                JsonNode jsonNode = objectMapper.readTree(esResponse);
                String message = jsonNode.has("message") ? jsonNode.get("message").asText() : "No ES message.";
                System.out.println("Auto Publish (ES): " + message + " | URL: " + esUrl);
            }

            // Call DB publishing endpoint
            String dbResponse = restTemplate.getForObject(dbUrl, String.class);
            if (dbResponse != null) {
                JsonNode jsonNode = objectMapper.readTree(dbResponse);
                String message = jsonNode.has("message") ? jsonNode.get("message").asText() : "No DB message.";
                System.out.println("Auto Publish (DB): " + message + " | URL: " + dbUrl);
            }

        } catch (Exception e) {
            System.err.println("Error during scheduled publish: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
