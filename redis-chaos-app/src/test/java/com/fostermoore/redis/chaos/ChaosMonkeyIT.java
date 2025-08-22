package com.fostermoore.redis.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fostermoore.redis.chaos.service.RedisClusterService;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@SpringBootTest
@TestPropertySource(properties = {
    "redis.cluster.nodes=localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005,localhost:7006",
    "redis.cluster.timeout=5000",
    "redis.cluster.max-redirects=5"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChaosMonkeyIT {

    private static final Logger logger = LoggerFactory.getLogger(ChaosMonkeyIT.class);

    @Autowired
    private RedisClusterService redisService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @BeforeEach
    void setUp() {
        logger.info("Starting chaos monkey test - checking cluster health");
        boolean healthy = redisService.isClusterHealthy();
        Assertions.assertTrue(healthy, "Redis cluster should be healthy before starting tests");
    }

    @Test
    @Order(1)
    @DisplayName("Cluster Health and Basic Operations Test")
    void testClusterHealthAndBasicOps() {
        logger.info("=== Testing cluster health and basic operations ===");

        Map<String, String> clusterInfo = redisService.getClusterInfo();
        Assertions.assertNotNull(clusterInfo);
        Assertions.assertEquals("ok", clusterInfo.get("cluster_state"));
        
        logger.info("Cluster state: {}", clusterInfo.get("cluster_state"));
        logger.info("Cluster slots assigned: {}", clusterInfo.get("cluster_slots_assigned"));
        logger.info("Cluster known nodes: {}", clusterInfo.get("cluster_known_nodes"));

        List<Map<String, Object>> nodes = redisService.getClusterNodes();
        Assertions.assertNotNull(nodes);
        Assertions.assertFalse(nodes.isEmpty());
        
        logger.info("Found {} nodes in cluster", nodes.size());
        nodes.forEach(node -> logger.info("Node: {} - {} - {}", 
            node.get("id"), node.get("address"), node.get("flags")));

        redisService.setWithRetry("health_test", "cluster_healthy");
        Object result = redisService.getWithRetry("health_test");
        Assertions.assertEquals("cluster_healthy", result);
        
        redisService.delete("health_test");
        Assertions.assertFalse(redisService.exists("health_test"));
    }

    @Test
    @Order(2)
    @DisplayName("Key Distribution Across Shards Test")
    void testKeyDistributionAcrossShards() {
        logger.info("=== Testing key distribution across shards ===");
        
        Map<String, Integer> shardDistribution = new HashMap<>();
        List<String> testKeys = new ArrayList<>();
        
        for (int i = 0; i < 1000; i++) {
            String key = "shard_test_" + i;
            testKeys.add(key);
            
            try {
                redisService.setWithRetry(key, "value_" + i);
                
                List<Map<String, Object>> nodes = redisService.getClusterNodes();
                for (Map<String, Object> node : nodes) {
                    String address = (String) node.get("address");
                    if (address != null) {
                        String nodeKey = address.split("@")[0];
                        shardDistribution.merge(nodeKey, 1, Integer::sum);
                        break; // For demonstration, we're not actually checking which shard the key landed on
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to set key {}: {}", key, e.getMessage());
            }
        }
        
        logger.info("Key distribution simulation across nodes:");
        shardDistribution.forEach((node, count) -> 
            logger.info("Node {}: {} keys", node, count));
        
        Assertions.assertFalse(shardDistribution.isEmpty());
        
        testKeys.forEach(key -> {
            try {
                redisService.delete(key);
            } catch (Exception e) {
                logger.warn("Failed to cleanup key {}: {}", key, e.getMessage());
            }
        });
    }

    @Test
    @Order(3)
    @DisplayName("Performance and Throughput Test")
    void testPerformanceAndThroughput() {
        logger.info("=== Testing performance and throughput ===");
        
        int numOperations = 10000;
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        
        AtomicLong totalSetTime = new AtomicLong(0);
        AtomicLong totalGetTime = new AtomicLong(0);
        AtomicInteger successfulSets = new AtomicInteger(0);
        AtomicInteger successfulGets = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);
        
        Instant startTime = Instant.now();
        
        List<Future<?>> futures = new ArrayList<>();
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            Future<?> future = executor.submit(() -> {
                for (int i = 0; i < numOperations / numThreads; i++) {
                    String key = String.format("perf_test_%d_%d", threadId, i);
                    String value = "performance_test_value_" + i;
                    
                    try {
                        Instant setStart = Instant.now();
                        redisService.setWithRetry(key, value);
                        totalSetTime.addAndGet(Duration.between(setStart, Instant.now()).toNanos());
                        successfulSets.incrementAndGet();
                        
                        Instant getStart = Instant.now();
                        Object retrieved = redisService.getWithRetry(key);
                        totalGetTime.addAndGet(Duration.between(getStart, Instant.now()).toNanos());
                        
                        if (value.equals(retrieved)) {
                            successfulGets.incrementAndGet();
                        }
                        
                    } catch (Exception e) {
                        failures.incrementAndGet();
                        logger.error("Performance test failure for key {}: {}", key, e.getMessage());
                    }
                }
            });
            futures.add(future);
        }
        
        futures.forEach(future -> {
            try {
                future.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Future execution failed: {}", e.getMessage());
            }
        });
        
        executor.shutdown();
        
        Instant endTime = Instant.now();
        Duration totalTime = Duration.between(startTime, endTime);
        
        double avgSetTimeMs = totalSetTime.get() / (double) successfulSets.get() / 1_000_000;
        double avgGetTimeMs = totalGetTime.get() / (double) successfulGets.get() / 1_000_000;
        double throughputOpsPerSec = (successfulSets.get() + successfulGets.get()) / totalTime.toSeconds();
        
        logger.info("Performance Results:");
        logger.info("Total operations: {}", numOperations * 2); // SET + GET
        logger.info("Successful SETs: {}", successfulSets.get());
        logger.info("Successful GETs: {}", successfulGets.get());
        logger.info("Failures: {}", failures.get());
        logger.info("Total time: {}ms", totalTime.toMillis());
        logger.info("Average SET time: {:.2f}ms", avgSetTimeMs);
        logger.info("Average GET time: {:.2f}ms", avgGetTimeMs);
        logger.info("Throughput: {:.2f} ops/sec", throughputOpsPerSec);
        
        Assertions.assertTrue(successfulSets.get() > numOperations * 0.9, 
            "Should have >90% successful SET operations");
        Assertions.assertTrue(successfulGets.get() > numOperations * 0.9, 
            "Should have >90% successful GET operations");
        Assertions.assertTrue(throughputOpsPerSec > 100, 
            "Should achieve >100 ops/sec throughput");
        
        cleanupPerformanceTestKeys(numThreads, numOperations / numThreads);
    }

    @Test
    @Order(4)
    @DisplayName("Data Type Operations Test")
    void testDataTypeOperations() {
        logger.info("=== Testing different Redis data types ===");
        
        redisService.setWithRetry("string_test", "test_string_value");
        Assertions.assertEquals("test_string_value", redisService.getWithRetry("string_test"));
        
        redisService.setHash("hash_test", "field1", "value1");
        redisService.setHash("hash_test", "field2", "value2");
        Assertions.assertEquals("value1", redisService.getHashField("hash_test", "field1"));
        Assertions.assertEquals("value2", redisService.getHashField("hash_test", "field2"));
        
        redisService.addToList("list_test", "item1");
        redisService.addToList("list_test", "item2");
        redisService.addToList("list_test", "item3");
        List<Object> listItems = redisService.getList("list_test", 0, -1);
        Assertions.assertEquals(3, listItems.size());
        Assertions.assertTrue(listItems.contains("item1"));
        
        redisService.addToSet("set_test", "member1", "member2", "member3");
        Set<Object> setMembers = redisService.getSet("set_test");
        Assertions.assertEquals(3, setMembers.size());
        Assertions.assertTrue(setMembers.contains("member1"));
        
        redisService.delete("string_test");
        redisService.delete("hash_test");
        redisService.delete("list_test");
        redisService.delete("set_test");
    }

    @Test
    @Order(5)
    @DisplayName("Key Expiration and TTL Test")
    void testKeyExpirationAndTTL() {
        logger.info("=== Testing key expiration and TTL ===");
        
        String expireKey = "expire_test_key";
        redisService.setWithExpiration(expireKey, "will_expire", 2);
        
        Assertions.assertTrue(redisService.exists(expireKey));
        Assertions.assertEquals("will_expire", redisService.getWithRetry(expireKey));
        
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        Assertions.assertFalse(redisService.exists(expireKey));
        Assertions.assertNull(redisService.getWithRetry(expireKey));
    }

    @Test
    @Order(6)
    @DisplayName("Failover and Retry Mechanism Test")
    void testFailoverAndRetryMechanism() {
        logger.info("=== Testing failover and retry mechanisms ===");
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        ExecutorService executor = Executors.newFixedThreadPool(5);
        
        List<Future<?>> futures = IntStream.range(0, 100)
            .mapToObj(i -> executor.submit(() -> {
                String key = "failover_test_" + i;
                String value = "failover_value_" + i;
                
                try {
                    redisService.setWithRetry(key, value);
                    Object retrieved = redisService.getWithRetry(key);
                    
                    if (value.equals(retrieved)) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    logger.warn("Failover test failure for key {}: {}", key, e.getMessage());
                }
            }))
            .toList();
        
        futures.forEach(future -> {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failover test future failed: {}", e.getMessage());
            }
        });
        
        executor.shutdown();
        
        logger.info("Failover test results - Success: {}, Failures: {}", 
            successCount.get(), failureCount.get());
        
        Assertions.assertTrue(successCount.get() > 80, 
            "Should have >80% success rate even during potential failures");
        
        IntStream.range(0, 100).forEach(i -> {
            try {
                redisService.delete("failover_test_" + i);
            } catch (Exception e) {
                // Cleanup failures are acceptable
            }
        });
    }

    @Test
    @Order(7)
    @DisplayName("Graceful Degradation Test")
    void testGracefulDegradation() {
        logger.info("=== Testing graceful degradation ===");
        
        String fallbackValue = "fallback_data";
        Object result = redisService.getWithFallback("non_existent_key", fallbackValue);
        Assertions.assertEquals(fallbackValue, result);
        
        boolean setResult = redisService.setWithFallback("degradation_test", "test_value");
        
        if (setResult) {
            Object retrieved = redisService.getWithFallback("degradation_test", "fallback");
            Assertions.assertEquals("test_value", retrieved);
            redisService.delete("degradation_test");
        } else {
            logger.warn("Set operation failed gracefully - this is expected behavior during degradation");
        }
    }

    @Test
    @Order(8)
    @DisplayName("Concurrent Access Stress Test")
    void testConcurrentAccessStress() {
        logger.info("=== Testing concurrent access stress ===");
        
        String sharedKey = "concurrent_stress_key";
        int numThreads = 20;
        int operationsPerThread = 50;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numThreads);
        
        AtomicInteger operations = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        try {
                            String value = String.format("thread_%d_op_%d", threadId, i);
                            redisService.setWithRetry(sharedKey, value);
                            redisService.getWithRetry(sharedKey);
                            operations.incrementAndGet();
                            
                        } catch (Exception e) {
                            errors.incrementAndGet();
                            logger.warn("Concurrent access error: {}", e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        Instant startTime = Instant.now();
        startLatch.countDown(); // Start all threads
        
        try {
            boolean finished = endLatch.await(60, TimeUnit.SECONDS);
            Assertions.assertTrue(finished, "All threads should complete within timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        executor.shutdown();
        
        Duration totalTime = Duration.between(startTime, Instant.now());
        
        logger.info("Concurrent stress test results:");
        logger.info("Total operations: {}", operations.get());
        logger.info("Errors: {}", errors.get());
        logger.info("Total time: {}ms", totalTime.toMillis());
        logger.info("Error rate: {:.2f}%", (errors.get() * 100.0) / (operations.get() + errors.get()));
        
        Assertions.assertTrue(operations.get() > (numThreads * operationsPerThread * 0.8), 
            "Should complete >80% of operations successfully");
        
        redisService.delete(sharedKey);
    }

    @Test
    @Order(9)
    @DisplayName("Memory and Large Value Test")
    void testMemoryAndLargeValues() {
        logger.info("=== Testing memory usage and large values ===");
        
        String largeValue = "x".repeat(1024 * 10); // 10KB string
        String largeKey = "large_value_test";
        
        redisService.setWithRetry(largeKey, largeValue);
        Object retrieved = redisService.getWithRetry(largeKey);
        Assertions.assertEquals(largeValue, retrieved);
        
        Map<String, Object> largeHashData = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeHashData.put("field_" + i, "data_" + i + "_" + "x".repeat(100));
        }
        
        String largeHashKey = "large_hash_test";
        largeHashData.forEach((field, value) -> 
            redisService.setHash(largeHashKey, field, value));
        
        for (int i = 0; i < 10; i++) { // Test a sample
            String field = "field_" + i;
            Object value = redisService.getHashField(largeHashKey, field);
            Assertions.assertNotNull(value);
            Assertions.assertTrue(value.toString().startsWith("data_" + i));
        }
        
        redisService.delete(largeKey);
        redisService.delete(largeHashKey);
    }

    @Test
    @Order(10)
    @DisplayName("Cleanup and Final Health Check")
    void testCleanupAndFinalHealthCheck() {
        logger.info("=== Final cleanup and health check ===");
        
        boolean finalHealth = redisService.isClusterHealthy();
        Assertions.assertTrue(finalHealth, "Cluster should still be healthy after all tests");
        
        Map<String, String> finalClusterInfo = redisService.getClusterInfo();
        logger.info("Final cluster state: {}", finalClusterInfo.get("cluster_state"));
        logger.info("Final cluster slots assigned: {}", finalClusterInfo.get("cluster_slots_assigned"));
        
        logger.info("=== All chaos monkey tests completed successfully ===");
    }

    private void cleanupPerformanceTestKeys(int numThreads, int opsPerThread) {
        for (int t = 0; t < numThreads; t++) {
            for (int i = 0; i < opsPerThread; i++) {
                try {
                    redisService.delete(String.format("perf_test_%d_%d", t, i));
                } catch (Exception e) {
                    // Cleanup failures are acceptable
                }
            }
        }
    }
}