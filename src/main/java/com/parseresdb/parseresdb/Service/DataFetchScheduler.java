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

    @Scheduled(fixedRate = 120000) // Runs every 2 minutes (120000ms)
    public void fetchAndFeedDataAutomatically() {
        Instant now = Instant.now(); // Current time
        Instant twoMinutesAgo = now.minusSeconds(120); // Subtract 2 minutes

        String gte = FORMATTER.format(twoMinutesAgo); // Format as ISO 8601
        String lte = FORMATTER.format(now);

        String url = "http://localhost:8082/api/data/fetch-and-feed?gte=" + gte + "&lte=" + lte;

        try {
            String jsonResponse = restTemplate.getForObject(url, String.class);

            if (jsonResponse != null) {
                JsonNode jsonNode = objectMapper.readTree(jsonResponse);
                int indexedCount = jsonNode.has("indexedCount") ? jsonNode.get("indexedCount").asInt() : 0;
                System.out.println("Auto Fetch & Feed Triggered: " + indexedCount + " records processed." + " For url : " + url);
            } else {
                System.out.println("Auto Fetch & Feed Triggered: No response received.");
            }
        } catch (Exception e) {
            System.err.println("Error while calling fetch-and-feed: " + e.getMessage());
        }
    }
}
