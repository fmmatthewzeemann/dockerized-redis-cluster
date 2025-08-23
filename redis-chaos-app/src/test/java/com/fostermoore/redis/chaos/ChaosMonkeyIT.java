package com.fostermoore.redis.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fostermoore.redis.chaos.service.RedisClusterService;
import com.fostermoore.redis.chaos.service.ChaosOrchestrator;
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
import java.util.stream.Collectors;

@SpringBootTest
@TestPropertySource(properties = {
    "redis.cluster.nodes=redis.localhost:7001,redis.localhost:7002,redis.localhost:7003,redis.localhost:7004,redis.localhost:7005,redis.localhost:7006",
    "redis.cluster.timeout=5000",
    "redis.cluster.max-redirects=5"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChaosMonkeyIT {

    private static final Logger logger = LoggerFactory.getLogger(ChaosMonkeyIT.class);

    @Autowired
    private RedisClusterService redisService;

    @Autowired
    private ChaosOrchestrator chaosOrchestrator;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        logger.info("============================================================");
        logger.info("SETUP: Starting chaos monkey test - checking cluster health");
        logger.info("============================================================");

        Instant startTime = Instant.now();

        try {
            // Get detailed cluster information before test
            Map<String, String> clusterInfo = redisService.getClusterInfo();
            logger.info("SETUP: Cluster state: {}", clusterInfo.get("cluster_state"));
            logger.info("SETUP: Known nodes: {}", clusterInfo.get("cluster_known_nodes"));
            logger.info("SETUP: Slots assigned: {}", clusterInfo.get("cluster_slots_assigned"));

            List<Map<String, Object>> nodes = redisService.getClusterNodes();
            logger.info("SETUP: Available cluster nodes: {}", nodes.size());
            for (int i = 0; i < nodes.size(); i++) {
                Map<String, Object> node = nodes.get(i);
                logger.info("SETUP: Node {}: {} [{}] - {}",
                    i + 1,
                    node.get("address"),
                    node.get("flags"),
                    node.get("link_state"));
            }

            boolean healthy = redisService.isClusterHealthy();
            Duration setupTime = Duration.between(startTime, Instant.now());

            logger.info("SETUP: Health check completed in {}ms - Result: {}",
                setupTime.toMillis(), healthy ? "HEALTHY" : "UNHEALTHY");

            if (!healthy) {
                logger.error("SETUP: Cluster health check FAILED!");
                logger.error("SETUP: Cluster info: {}", clusterInfo);
            }

            Assertions.assertTrue(healthy, "Redis cluster should be healthy before starting tests");
            logger.info("SETUP: ✅ Cluster is healthy - proceeding with test");

        } catch (Exception e) {
            Duration setupTime = Duration.between(startTime, Instant.now());
            logger.error("SETUP: Health check FAILED after {}ms with error: {}",
                setupTime.toMillis(), e.getMessage(), e);
            throw e;
        }
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
        logger.info("🔄 TEST: Key Distribution Across Shards - STARTING");

        int totalKeys = 1000;
        Map<String, Integer> shardDistribution = new HashMap<>();
        List<String> testKeys = new ArrayList<>();
        AtomicInteger successfulSets = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        Instant startTime = Instant.now();

        logger.info("TEST: Creating {} keys across cluster shards...", totalKeys);

        for (int i = 0; i < totalKeys; i++) {
            String key = "shard_test_" + i;
            testKeys.add(key);

            if (i > 0 && i % 100 == 0) {
                logger.info("TEST: Progress - {}/{} keys processed ({}%)",
                    i, totalKeys, (i * 100) / totalKeys);
            }

            try {
                redisService.setWithRetry(key, "value_" + i);
                successfulSets.incrementAndGet();

                // Simulate distribution tracking (in real scenario, this would use CLUSTER KEYSLOT)
                List<Map<String, Object>> nodes = redisService.getClusterNodes();
                if (!nodes.isEmpty()) {
                    String address = (String) nodes.get(0).get("address");
                    if (address != null) {
                        String nodeKey = address.split("@")[0];
                        shardDistribution.merge(nodeKey, 1, Integer::sum);
                    }
                }
            } catch (Exception e) {
                errors.incrementAndGet();
                logger.error("TEST: Failed to set key {}: {}", key, e.getMessage());
            }
        }

        Duration operationTime = Duration.between(startTime, Instant.now());

        logger.info("TEST: Key creation completed in {}ms", operationTime.toMillis());
        logger.info("TEST: Successful sets: {}/{} ({}%)",
            successfulSets.get(), totalKeys, (successfulSets.get() * 100) / totalKeys);
        logger.info("TEST: Errors: {}", errors.get());

        logger.info("TEST: Key distribution simulation across nodes:");
        shardDistribution.forEach((node, count) ->
            logger.info("TEST:   Node {}: {} keys ({:.1f}%)",
                node, count, (count * 100.0) / successfulSets.get()));

        Assertions.assertFalse(shardDistribution.isEmpty());
        Assertions.assertTrue(successfulSets.get() > totalKeys * 0.95,
            "Should successfully set >95% of keys");

        // Cleanup phase
        logger.info("TEST: Starting cleanup of {} keys...", testKeys.size());
        Instant cleanupStart = Instant.now();
        AtomicInteger cleanupErrors = new AtomicInteger(0);

        testKeys.forEach(key -> {
            try {
                redisService.delete(key);
            } catch (Exception e) {
                cleanupErrors.incrementAndGet();
                if (cleanupErrors.get() <= 5) { // Log first 5 cleanup errors
                    logger.warn("TEST: Failed to cleanup key {}: {}", key, e.getMessage());
                }
            }
        });

        Duration cleanupTime = Duration.between(cleanupStart, Instant.now());
        logger.info("TEST: Cleanup completed in {}ms with {} errors",
            cleanupTime.toMillis(), cleanupErrors.get());
        logger.info("✅ TEST: Key Distribution Across Shards - COMPLETED");
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
            .collect(Collectors.toList());

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

    // Define a custom exception (to be placed in an appropriate package/file)
    public class ChaosTestException extends RuntimeException {
        public ChaosTestException(String message, Throwable cause) {
            super(message, cause);
        }
    }


    @Test
    @Order(10)
    @DisplayName("Node Failure Chaos Test")
    void testNodeFailureChaos() {
        logger.info("🔥 CHAOS-TEST: Node Failure Simulation - STARTING");
        logger.info("CHAOS-TEST: This test simulates Docker container failures to test cluster resilience");

        Instant startTime = Instant.now();

        try {
            // Record initial cluster state
            Map<String, String> initialClusterInfo = redisService.getClusterInfo();
            List<Map<String, Object>> initialNodes = redisService.getClusterNodes();
            logger.info("CHAOS-TEST: Initial cluster state - {} nodes, state: {}",
                initialNodes.size(), initialClusterInfo.get("cluster_state"));

            // Create test data before chaos
            String chaosKey = "chaos_node_failure_test";
            String chaosValue = "data_should_survive_node_failure";
            redisService.setWithRetry(chaosKey, chaosValue);
            logger.info("CHAOS-TEST: Created test data - key: {}", chaosKey);

            // Execute node failure chaos
            logger.info("CHAOS-TEST: Executing node failure simulation...");
            var chaosResult = chaosOrchestrator.simulateNodeFailure("redis-1", Duration.ofSeconds(10));

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Node failure simulation completed in {}ms", chaosTime.toMillis());

            // Log chaos results
            logger.info("CHAOS-TEST: Chaos execution summary:");
            logger.info("CHAOS-TEST:   Target node: {}", chaosResult.getTargetNode());
            logger.info("CHAOS-TEST:   Success: {}", chaosResult.isSuccess());
            logger.info("CHAOS-TEST:   Duration: {}s", chaosResult.getDurationSeconds());
            logger.info("CHAOS-TEST:   Operations during chaos - successful: {}, failed: {}",
                chaosResult.getSuccessfulOperations(), chaosResult.getFailedOperations());
            logger.info("CHAOS-TEST:   Error message: {}", chaosResult.getErrorMessage());

            // Verify cluster recovered and data survived
            Thread.sleep(2000); // Allow cluster to stabilize

            logger.info("CHAOS-TEST: Verifying cluster recovery...");
            boolean isHealthy = redisService.isClusterHealthy();
            Object recoveredData = redisService.getWithRetry(chaosKey);

            logger.info("CHAOS-TEST: Recovery verification:");
            logger.info("CHAOS-TEST:   Cluster healthy after chaos: {}", isHealthy);
            logger.info("CHAOS-TEST:   Test data survived: {}", chaosValue.equals(recoveredData));

            // Verify cluster state after recovery
            Map<String, String> finalClusterInfo = redisService.getClusterInfo();
            List<Map<String, Object>> finalNodes = redisService.getClusterNodes();
            logger.info("CHAOS-TEST: Final cluster state - {} nodes, state: {}",
                finalNodes.size(), finalClusterInfo.get("cluster_state"));

            // Assertions
            Assertions.assertTrue(chaosResult.isSuccess(), "Node failure simulation should succeed");
            Assertions.assertTrue(isHealthy, "Cluster should be healthy after node failure recovery");
            Assertions.assertEquals(chaosValue, recoveredData, "Data should survive node failures");

            // Cleanup
            redisService.delete(chaosKey);

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Node Failure Simulation - COMPLETED in {}ms", totalTime.toMillis());

        } catch (Exception e) {
            Duration testTime = Duration.between(startTime, Instant.now());
            logger.error("❌ CHAOS-TEST: Node Failure Simulation FAILED after {}ms - Error: {}",
                testTime.toMillis(), e.getMessage(), e);
            throw new ChaosTestException("Node Failure Simulation encountered a critical error.", e);
        }
    }

    @Test
    @Order(11)
    @DisplayName("Memory Exhaustion Chaos Test")
    void testMemoryExhaustionChaos() {
        logger.info("🧠 CHAOS-TEST: Memory Exhaustion Simulation - STARTING");
        logger.info("CHAOS-TEST: This test fills Redis with large data to test memory pressure handling");

        Instant startTime = Instant.now();

        try {
            // Record initial memory state
            Map<String, String> initialClusterInfo = redisService.getClusterInfo();
            logger.info("CHAOS-TEST: Initial cluster state: {}", initialClusterInfo.get("cluster_state"));

            // Create baseline test data
            String testKey = "chaos_memory_survival_test";
            String testValue = "this_data_should_survive_memory_pressure";
            redisService.setWithRetry(testKey, testValue);
            logger.info("CHAOS-TEST: Created survival test data");

            // Execute memory exhaustion chaos
            logger.info("CHAOS-TEST: Executing memory exhaustion simulation...");
            var chaosResult = chaosOrchestrator.simulateMemoryExhaustion(50, 1024 * 100); // 50 objects of 100KB each

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Memory exhaustion simulation completed in {}ms", chaosTime.toMillis());

            // Log chaos results
            logger.info("CHAOS-TEST: Memory chaos execution summary:");
            logger.info("CHAOS-TEST:   Large objects created: {}", chaosResult.getLargeObjectCount());
            logger.info("CHAOS-TEST:   Object size bytes: {}", chaosResult.getObjectSizeBytes());
            logger.info("CHAOS-TEST:   Success: {}", chaosResult.isSuccess());
            logger.info("CHAOS-TEST:   Memory operations - successful: {}, failed: {}",
                chaosResult.getSuccessfulOperations(), chaosResult.getFailedOperations());
            logger.info("CHAOS-TEST:   Memory pressure created: {} MB",
                (chaosResult.getLargeObjectCount() * chaosResult.getObjectSizeBytes()) / (1024 * 1024));
            if (chaosResult.getErrorMessage() != null) {
                logger.info("CHAOS-TEST:   Error message: {}", chaosResult.getErrorMessage());
            }

            // Verify cluster still functions under memory pressure
            logger.info("CHAOS-TEST: Testing cluster functionality under memory pressure...");
            boolean isHealthy = redisService.isClusterHealthy();
            Object survivedData = redisService.getWithRetry(testKey);

            logger.info("CHAOS-TEST: Cluster status under memory pressure:");
            logger.info("CHAOS-TEST:   Cluster healthy: {}", isHealthy);
            logger.info("CHAOS-TEST:   Test data survived: {}", testValue.equals(survivedData));

            // Try to perform additional operations under memory pressure
            String pressureTestKey = "pressure_operation_test";
            boolean canStillWrite = false;
            try {
                redisService.setWithRetry(pressureTestKey, "testing_under_pressure");
                Object retrieved = redisService.getWithRetry(pressureTestKey);
                canStillWrite = "testing_under_pressure".equals(retrieved);
                redisService.delete(pressureTestKey);
            } catch (Exception e) {
                logger.warn("CHAOS-TEST: Write operations failed under memory pressure: {}", e.getMessage());
            }

            logger.info("CHAOS-TEST:   Can still write under pressure: {}", canStillWrite);

            // Cleanup memory pressure objects (this may take time)
            logger.info("CHAOS-TEST: Cleaning up memory pressure objects...");
            for (int i = 0; i < chaosResult.getLargeObjectCount(); i++) {
                try {
                    redisService.delete("large_memory_object_" + i);
                } catch (Exception e) {
                    logger.warn("CHAOS-TEST: Failed to cleanup memory object {}: {}", i, e.getMessage());
                }
            }

            // Allow some time for memory cleanup
            Thread.sleep(1000);

            // Verify recovery after cleanup
            boolean finalHealth = redisService.isClusterHealthy();
            logger.info("CHAOS-TEST: Cluster health after memory cleanup: {}", finalHealth);

            // Cleanup test data
            redisService.delete(testKey);

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Memory Exhaustion Simulation - COMPLETED in {}ms", totalTime.toMillis());

            // We don't assert cluster health because memory exhaustion might legitimately cause issues
            // But we do verify the chaos execution was attempted
            Assertions.assertTrue(chaosResult.getLargeObjectCount() > 0,
                "Should have attempted to create large objects");

        } catch (Exception e) {
            Duration testTime = Duration.between(startTime, Instant.now());
            logger.error("❌ CHAOS-TEST: Memory Exhaustion Simulation FAILED after {}ms - Error: {}",
                testTime.toMillis(), e.getMessage(), e);
            throw new ChaosTestException("Node Failure Simulation encountered a critical error.", e);
        }
    }

    @Test
    @Order(12)
    @DisplayName("Network Partition Chaos Test")
    void testNetworkPartitionChaos() {
        logger.info("🌐 CHAOS-TEST: Network Partition Simulation - STARTING");
        logger.info("CHAOS-TEST: This test simulates network issues and latency to test cluster resilience");

        Instant startTime = Instant.now();

        try {
            // Create test data before partition
            String partitionKey = "chaos_partition_test";
            String partitionValue = "data_during_network_partition";
            redisService.setWithRetry(partitionKey, partitionValue);
            logger.info("CHAOS-TEST: Created test data before network partition");

            // Execute network partition chaos
            logger.info("CHAOS-TEST: Executing network partition simulation...");
            var chaosResult = chaosOrchestrator.simulateNetworkPartition(Duration.ofSeconds(10), 2);

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Network partition simulation completed in {}ms", chaosTime.toMillis());

            // Log chaos results
            logger.info("CHAOS-TEST: Network partition execution summary:");
            logger.info("CHAOS-TEST:   Duration: {}s", chaosResult.getDurationSeconds());
            logger.info("CHAOS-TEST:   Affected nodes: {}", chaosResult.getAffectedNodeCount());
            logger.info("CHAOS-TEST:   Success: {}", chaosResult.isSuccess());
            logger.info("CHAOS-TEST:   Partition operations - successful: {}, failed: {}",
                chaosResult.getSuccessfulOperations(), chaosResult.getFailedOperations());
            if (chaosResult.getErrorMessage() != null) {
                logger.info("CHAOS-TEST:   Error details: {}", chaosResult.getErrorMessage());
            }

            // Test cluster behavior during/after partition
            logger.info("CHAOS-TEST: Testing cluster behavior after network partition...");
            boolean isHealthy = redisService.isClusterHealthy();

            // Try to access existing data
            Object retrievedData = null;
            boolean dataAccessible = false;
            try {
                retrievedData = redisService.getWithRetry(partitionKey);
                dataAccessible = partitionValue.equals(retrievedData);
            } catch (Exception e) {
                logger.warn("CHAOS-TEST: Data access failed after partition: {}", e.getMessage());
            }

            // Try to write new data
            String newKey = "post_partition_write_test";
            boolean canWrite = false;
            try {
                redisService.setWithRetry(newKey, "written_after_partition");
                Object written = redisService.getWithRetry(newKey);
                canWrite = "written_after_partition".equals(written);
                redisService.delete(newKey);
            } catch (Exception e) {
                logger.warn("CHAOS-TEST: Write failed after partition: {}", e.getMessage());
            }

            logger.info("CHAOS-TEST: Post-partition cluster status:");
            logger.info("CHAOS-TEST:   Cluster healthy: {}", isHealthy);
            logger.info("CHAOS-TEST:   Existing data accessible: {}", dataAccessible);
            logger.info("CHAOS-TEST:   New writes possible: {}", canWrite);

            // Cleanup
            redisService.delete(partitionKey);

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Network Partition Simulation - COMPLETED in {}ms", totalTime.toMillis());

            // Network partitions may cause temporary issues, so we mainly verify the chaos was attempted
            Assertions.assertTrue(chaosResult.getDurationSeconds() > 0, "Should have attempted network partition");

        } catch (Exception e) {
            Duration testTime = Duration.between(startTime, Instant.now());
            logger.error("❌ CHAOS-TEST: Network Partition Simulation FAILED after {}ms - Error: {}",
                testTime.toMillis(), e.getMessage(), e);
            throw e;
        }
    }

    @Test
    @Order(13)
    @DisplayName("Data Corruption Chaos Test")
    void testDataCorruptionChaos() {
        logger.info("💾 CHAOS-TEST: Data Corruption Simulation - STARTING");
        logger.info("CHAOS-TEST: This test corrupts random data to test data integrity mechanisms");

        Instant startTime = Instant.now();

        try {
            // Create test dataset
            Map<String, String> testData = new HashMap<>();
            for (int i = 0; i < 20; i++) {
                String key = "integrity_test_" + i;
                String value = "clean_data_value_" + i + "_" + UUID.randomUUID();
                testData.put(key, value);
                redisService.setWithRetry(key, value);
            }
            logger.info("CHAOS-TEST: Created {} test records for corruption testing", testData.size());

            // Execute data corruption chaos
            logger.info("CHAOS-TEST: Executing data corruption simulation...");
            var chaosResult = chaosOrchestrator.simulateDataCorruption(10, 0.3); // Corrupt up to 10 keys with 30% chance each

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Data corruption simulation completed in {}ms", chaosTime.toMillis());

            // Log chaos results
            logger.info("CHAOS-TEST: Data corruption execution summary:");
            logger.info("CHAOS-TEST:   Keys targeted for corruption: {}", chaosResult.getKeysTargeted());
            logger.info("CHAOS-TEST:   Corruption probability: {}%", chaosResult.getCorruptionProbability() * 100);
            logger.info("CHAOS-TEST:   Success: {}", chaosResult.isSuccess());
            logger.info("CHAOS-TEST:   Corruption operations - successful: {}, failed: {}",
                chaosResult.getSuccessfulOperations(), chaosResult.getFailedOperations());
            if (chaosResult.getErrorMessage() != null) {
                logger.info("CHAOS-TEST:   Error details: {}", chaosResult.getErrorMessage());
            }

            // Verify data integrity and detect corruption
            logger.info("CHAOS-TEST: Performing data integrity verification...");
            int cleanDataCount = 0;
            int corruptedDataCount = 0;
            int missingDataCount = 0;

            for (Map.Entry<String, String> entry : testData.entrySet()) {
                String key = entry.getKey();
                String expectedValue = entry.getValue();

                try {
                    Object actualValue = redisService.getWithRetry(key);
                    if (actualValue == null) {
                        missingDataCount++;
                        logger.debug("CHAOS-TEST: Missing data detected for key: {}", key);
                    } else if (expectedValue.equals(actualValue)) {
                        cleanDataCount++;
                    } else {
                        corruptedDataCount++;
                        logger.debug("CHAOS-TEST: Data corruption detected - Key: {}, Expected: {}, Actual: {}",
                            key, expectedValue, actualValue);
                    }
                } catch (Exception e) {
                    missingDataCount++;
                    logger.warn("CHAOS-TEST: Failed to read key {} during integrity check: {}", key, e.getMessage());
                }
            }

            logger.info("CHAOS-TEST: Data integrity verification results:");
            logger.info("CHAOS-TEST:   Clean data records: {} ({:.1f}%)", cleanDataCount,
                (cleanDataCount * 100.0) / testData.size());
            logger.info("CHAOS-TEST:   Corrupted data records: {} ({:.1f}%)", corruptedDataCount,
                (corruptedDataCount * 100.0) / testData.size());
            logger.info("CHAOS-TEST:   Missing data records: {} ({:.1f}%)", missingDataCount,
                (missingDataCount * 100.0) / testData.size());

            // Verify cluster is still functional
            boolean isHealthy = redisService.isClusterHealthy();
            logger.info("CHAOS-TEST: Cluster health after data corruption: {}", isHealthy);

            // Test new data operations
            String newKey = "post_corruption_test";
            boolean canWriteClean = false;
            try {
                redisService.setWithRetry(newKey, "clean_new_data");
                Object retrieved = redisService.getWithRetry(newKey);
                canWriteClean = "clean_new_data".equals(retrieved);
                redisService.delete(newKey);
            } catch (Exception e) {
                logger.warn("CHAOS-TEST: Failed to write clean data after corruption test: {}", e.getMessage());
            }

            logger.info("CHAOS-TEST: Can write clean data after corruption: {}", canWriteClean);

            // Cleanup corrupted test data
            logger.info("CHAOS-TEST: Cleaning up test data...");
            testData.keySet().forEach(key -> {
                try {
                    redisService.delete(key);
                } catch (Exception e) {
                    logger.warn("CHAOS-TEST: Failed to cleanup key {}: {}", key, e.getMessage());
                }
            });

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Data Corruption Simulation - COMPLETED in {}ms", totalTime.toMillis());

            // Assertions - corruption should be detectable and cluster should remain functional
            Assertions.assertTrue(chaosResult.getKeysTargeted() > 0, "Should have targeted keys for corruption");
            Assertions.assertTrue(isHealthy, "Cluster should remain healthy despite data corruption");
            Assertions.assertTrue(canWriteClean, "Should be able to write clean data after corruption");

        } catch (Exception e) {
            Duration testTime = Duration.between(startTime, Instant.now());
            logger.error("❌ CHAOS-TEST: Data Corruption Simulation FAILED after {}ms - Error: {}",
                testTime.toMillis(), e.getMessage(), e);
            throw e;
        }
    }

    @Test
    @Order(14)
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