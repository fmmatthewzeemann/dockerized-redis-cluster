package com.fostermoore.redis.chaos.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fostermoore.redis.chaos.service.RedisClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/chaos")
public class ChaosTestController {

    private static final Logger logger = LoggerFactory.getLogger(ChaosTestController.class);

    @Autowired
    private RedisClusterService redisService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executorService = Executors.newFixedThreadPool(20);

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getClusterHealth() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            boolean isHealthy = redisService.isClusterHealthy();
            Map<String, String> clusterInfo = redisService.getClusterInfo();
            List<Map<String, Object>> nodes = redisService.getClusterNodes();
            
            response.put("healthy", isHealthy);
            response.put("clusterInfo", clusterInfo);
            response.put("nodes", nodes);
            response.put("nodeCount", nodes.size());
            response.put("timestamp", Instant.now());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("healthy", false);
            response.put("error", e.getMessage());
            response.put("timestamp", Instant.now());
            return ResponseEntity.status(503).body(response);
        }
    }

    @PostMapping("/performance-test")
    public ResponseEntity<Map<String, Object>> runPerformanceTest(
            @RequestParam(defaultValue = "1000") int operations,
            @RequestParam(defaultValue = "10") int threads,
            @RequestParam(defaultValue = "false") boolean async) {
        
        logger.info("Starting performance test: {} operations, {} threads, async={}", 
            operations, threads, async);
        
        if (async) {
            CompletableFuture<Map<String, Object>> future = CompletableFuture.supplyAsync(() -> 
                executePerformanceTest(operations, threads), executorService);
            
            Map<String, Object> response = new HashMap<>();
            response.put("message", "Performance test started asynchronously");
            response.put("operations", operations);
            response.put("threads", threads);
            response.put("startTime", Instant.now());
            
            return ResponseEntity.accepted().body(response);
        } else {
            Map<String, Object> results = executePerformanceTest(operations, threads);
            return ResponseEntity.ok(results);
        }
    }

    @PostMapping("/stress-test")
    public ResponseEntity<Map<String, Object>> runStressTest(
            @RequestParam(defaultValue = "5000") int operations,
            @RequestParam(defaultValue = "20") int concurrentUsers) {
        
        logger.info("Starting stress test: {} operations, {} concurrent users", 
            operations, concurrentUsers);
        
        Instant startTime = Instant.now();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int user = 0; user < concurrentUsers; user++) {
            final int userId = user;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (int op = 0; op < operations / concurrentUsers; op++) {
                    String key = String.format("stress_user_%d_op_%d", userId, op);
                    String value = "stress_test_data_" + op;
                    
                    Instant opStart = Instant.now();
                    try {
                        redisService.setWithRetry(key, value);
                        Object retrieved = redisService.getWithRetry(key);
                        
                        if (value.equals(retrieved)) {
                            successCount.incrementAndGet();
                        } else {
                            errorCount.incrementAndGet();
                        }
                        
                        totalResponseTime.addAndGet(Duration.between(opStart, Instant.now()).toMillis());
                        
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        logger.debug("Stress test error for key {}: {}", key, e.getMessage());
                    }
                }
            }, executorService);
            
            futures.add(future);
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        Duration totalTime = Duration.between(startTime, Instant.now());
        
        Map<String, Object> results = new HashMap<>();
        results.put("totalOperations", operations);
        results.put("concurrentUsers", concurrentUsers);
        results.put("successCount", successCount.get());
        results.put("errorCount", errorCount.get());
        results.put("successRate", (double) successCount.get() / operations * 100);
        results.put("totalTimeMs", totalTime.toMillis());
        results.put("throughputOpsPerSec", operations / (totalTime.toMillis() / 1000.0));
        results.put("averageResponseTimeMs", totalResponseTime.get() / (double) operations);
        results.put("timestamp", Instant.now());
        
        // Cleanup
        cleanupStressTestKeys(concurrentUsers, operations / concurrentUsers);
        
        return ResponseEntity.ok(results);
    }

    @PostMapping("/failover-test")
    public ResponseEntity<Map<String, Object>> runFailoverTest(
            @RequestParam(defaultValue = "100") int keyCount,
            @RequestParam(defaultValue = "5") int retryAttempts) {
        
        logger.info("Starting failover test: {} keys, {} retry attempts", keyCount, retryAttempts);
        
        Map<String, Object> results = new HashMap<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        List<String> failedKeys = Collections.synchronizedList(new ArrayList<>());
        
        Instant startTime = Instant.now();
        
        for (int i = 0; i < keyCount; i++) {
            String key = "failover_test_" + i;
            String value = "failover_value_" + i;
            
            try {
                redisService.setWithRetry(key, value);
                Object retrieved = redisService.getWithRetry(key);
                
                if (value.equals(retrieved)) {
                    successCount.incrementAndGet();
                } else {
                    failureCount.incrementAndGet();
                    failedKeys.add(key);
                }
            } catch (Exception e) {
                failureCount.incrementAndGet();
                failedKeys.add(key + " (error: " + e.getMessage() + ")");
                logger.warn("Failover test failure for key {}: {}", key, e.getMessage());
            }
        }
        
        Duration totalTime = Duration.between(startTime, Instant.now());
        
        results.put("totalKeys", keyCount);
        results.put("successCount", successCount.get());
        results.put("failureCount", failureCount.get());
        results.put("successRate", (double) successCount.get() / keyCount * 100);
        results.put("totalTimeMs", totalTime.toMillis());
        results.put("failedKeys", failedKeys);
        results.put("clusterHealthy", redisService.isClusterHealthy());
        results.put("timestamp", Instant.now());
        
        // Cleanup successful keys
        for (int i = 0; i < keyCount; i++) {
            try {
                redisService.delete("failover_test_" + i);
            } catch (Exception e) {
                // Cleanup errors are acceptable
            }
        }
        
        return ResponseEntity.ok(results);
    }

    @PostMapping("/data-integrity-test")
    public ResponseEntity<Map<String, Object>> runDataIntegrityTest(
            @RequestParam(defaultValue = "1000") int recordCount) {
        
        logger.info("Starting data integrity test with {} records", recordCount);
        
        Map<String, Object> results = new HashMap<>();
        List<String> integrityErrors = new ArrayList<>();
        AtomicInteger corruptedRecords = new AtomicInteger(0);
        
        Instant startTime = Instant.now();
        
        Map<String, String> originalData = new HashMap<>();
        
        // Phase 1: Write test data
        for (int i = 0; i < recordCount; i++) {
            String key = "integrity_test_" + i;
            String value = generateComplexTestData(i);
            originalData.put(key, value);
            
            try {
                redisService.setWithRetry(key, value);
            } catch (Exception e) {
                integrityErrors.add("Failed to write key " + key + ": " + e.getMessage());
            }
        }
        
        // Phase 2: Verify data integrity
        for (Map.Entry<String, String> entry : originalData.entrySet()) {
            try {
                Object retrieved = redisService.getWithRetry(entry.getKey());
                if (!entry.getValue().equals(retrieved)) {
                    corruptedRecords.incrementAndGet();
                    integrityErrors.add("Data corruption in key " + entry.getKey() + 
                        ": expected '" + entry.getValue() + "', got '" + retrieved + "'");
                }
            } catch (Exception e) {
                corruptedRecords.incrementAndGet();
                integrityErrors.add("Failed to read key " + entry.getKey() + ": " + e.getMessage());
            }
        }
        
        Duration totalTime = Duration.between(startTime, Instant.now());
        
        results.put("totalRecords", recordCount);
        results.put("corruptedRecords", corruptedRecords.get());
        results.put("integrityRate", (double) (recordCount - corruptedRecords.get()) / recordCount * 100);
        results.put("totalTimeMs", totalTime.toMillis());
        results.put("errors", integrityErrors.size() > 10 ? 
            integrityErrors.subList(0, 10) : integrityErrors);
        results.put("timestamp", Instant.now());
        
        // Cleanup
        originalData.keySet().forEach(key -> {
            try {
                redisService.delete(key);
            } catch (Exception e) {
                // Cleanup errors are acceptable
            }
        });
        
        return ResponseEntity.ok(results);
    }

    @PostMapping("/memory-test")
    public ResponseEntity<Map<String, Object>> runMemoryTest(
            @RequestParam(defaultValue = "100") int largeObjectCount,
            @RequestParam(defaultValue = "10240") int objectSizeBytes) {
        
        logger.info("Starting memory test: {} objects, {} bytes each", 
            largeObjectCount, objectSizeBytes);
        
        Map<String, Object> results = new HashMap<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        Instant startTime = Instant.now();
        String largeData = "x".repeat(objectSizeBytes);
        
        for (int i = 0; i < largeObjectCount; i++) {
            String key = "memory_test_" + i;
            
            try {
                redisService.setWithRetry(key, largeData);
                Object retrieved = redisService.getWithRetry(key);
                
                if (largeData.equals(retrieved)) {
                    successCount.incrementAndGet();
                } else {
                    failureCount.incrementAndGet();
                }
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.warn("Memory test failure for key {}: {}", key, e.getMessage());
            }
        }
        
        Duration totalTime = Duration.between(startTime, Instant.now());
        
        results.put("largeObjectCount", largeObjectCount);
        results.put("objectSizeBytes", objectSizeBytes);
        results.put("totalDataSizeBytes", largeObjectCount * objectSizeBytes);
        results.put("successCount", successCount.get());
        results.put("failureCount", failureCount.get());
        results.put("successRate", (double) successCount.get() / largeObjectCount * 100);
        results.put("totalTimeMs", totalTime.toMillis());
        results.put("clusterHealthy", redisService.isClusterHealthy());
        results.put("timestamp", Instant.now());
        
        // Cleanup
        for (int i = 0; i < largeObjectCount; i++) {
            try {
                redisService.delete("memory_test_" + i);
            } catch (Exception e) {
                // Cleanup errors are acceptable
            }
        }
        
        return ResponseEntity.ok(results);
    }

    @DeleteMapping("/cleanup")
    public ResponseEntity<Map<String, Object>> cleanup() {
        logger.info("Starting cleanup of all test data");
        
        Map<String, Object> results = new HashMap<>();
        int cleanedUp = 0;
        
        String[] patterns = {
            "stress_user_", "failover_test_", "integrity_test_", 
            "memory_test_", "perf_test_", "chaos_test_"
        };
        
        for (String pattern : patterns) {
            for (int i = 0; i < 10000; i++) {
                String key = pattern + i;
                try {
                    if (redisService.exists(key)) {
                        redisService.delete(key);
                        cleanedUp++;
                    }
                } catch (Exception e) {
                    // Continue cleanup even if some keys fail
                }
            }
        }
        
        results.put("keysCleanedUp", cleanedUp);
        results.put("timestamp", Instant.now());
        
        return ResponseEntity.ok(results);
    }

    private Map<String, Object> executePerformanceTest(int operations, int threads) {
        Instant startTime = Instant.now();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (int i = 0; i < operations / threads; i++) {
                    String key = String.format("perf_test_%d_%d", threadId, i);
                    String value = "performance_test_value_" + i;
                    
                    Instant opStart = Instant.now();
                    try {
                        redisService.setWithRetry(key, value);
                        Object retrieved = redisService.getWithRetry(key);
                        
                        if (value.equals(retrieved)) {
                            successCount.incrementAndGet();
                        } else {
                            failureCount.incrementAndGet();
                        }
                        
                        totalResponseTime.addAndGet(Duration.between(opStart, Instant.now()).toMillis());
                        
                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                    }
                }
            }, executorService);
            
            futures.add(future);
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        Duration totalTime = Duration.between(startTime, Instant.now());
        
        Map<String, Object> results = new HashMap<>();
        results.put("totalOperations", operations);
        results.put("threads", threads);
        results.put("successCount", successCount.get());
        results.put("failureCount", failureCount.get());
        results.put("successRate", (double) successCount.get() / operations * 100);
        results.put("totalTimeMs", totalTime.toMillis());
        results.put("throughputOpsPerSec", operations / (totalTime.toMillis() / 1000.0));
        results.put("averageResponseTimeMs", totalResponseTime.get() / (double) operations);
        results.put("timestamp", Instant.now());
        
        // Cleanup
        cleanupPerformanceTestKeys(threads, operations / threads);
        
        return results;
    }

    private void cleanupPerformanceTestKeys(int threads, int opsPerThread) {
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < opsPerThread; i++) {
                try {
                    redisService.delete(String.format("perf_test_%d_%d", t, i));
                } catch (Exception e) {
                    // Cleanup failures are acceptable
                }
            }
        }
    }

    private void cleanupStressTestKeys(int users, int opsPerUser) {
        for (int u = 0; u < users; u++) {
            for (int i = 0; i < opsPerUser; i++) {
                try {
                    redisService.delete(String.format("stress_user_%d_op_%d", u, i));
                } catch (Exception e) {
                    // Cleanup failures are acceptable
                }
            }
        }
    }

    private String generateComplexTestData(int index) {
        Map<String, Object> complexData = new HashMap<>();
        complexData.put("id", index);
        complexData.put("timestamp", Instant.now().toString());
        complexData.put("data", "test_data_" + index + "_" + "x".repeat(100));
        complexData.put("metadata", Map.of(
            "type", "chaos_test",
            "version", "1.0",
            "checksum", "checksum_" + index
        ));
        
        try {
            return objectMapper.writeValueAsString(complexData);
        } catch (Exception e) {
            return "fallback_data_" + index;
        }
    }
}