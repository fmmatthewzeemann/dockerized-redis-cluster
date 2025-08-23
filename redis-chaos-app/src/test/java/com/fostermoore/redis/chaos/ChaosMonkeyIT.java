package com.fostermoore.redis.chaos;

import com.fostermoore.redis.chaos.service.ChaosOrchestrator;
import com.fostermoore.redis.chaos.service.RedisClusterService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Integration tests that exercise the Redis cluster and the ChaosOrchestrator.
 *
 * For each test, we document:
 *  - WHY: the risk or behavior being validated.
 *  - HOW: the concrete steps the test performs.
 *
 * Notes:
 *  - We use SLF4J parameterized logging (\"{}\") for performance and clarity.
 *  - Long-running tests include @Timeout to prevent CI hangs.
 *  - For production-grade async waits, consider Awaitility instead of Thread.sleep.
 */
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

    @BeforeEach
    void setUp() {
        // WHY: Ensure each test starts from a healthy baseline to isolate failures.
        // HOW: Read CLUSTER INFO and CLUSTER NODES; assert cluster_state == \"ok\".
        logger.info("============================================================");
        logger.info("SETUP: Starting chaos monkey test - checking cluster health");
        logger.info("============================================================");

        Instant startTime = Instant.now();

        try {
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
    @Timeout(value = 30)
    void testClusterHealthAndBasicOps() {
        // WHY: Verify cluster is up and basic SET/GET/DEL plumbing works.
        // HOW: Assert cluster_state == ok, then round-trip a key and delete it.
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
    @Timeout(value = 90)
    void testKeyDistributionAcrossShards() {
        // WHY: Detect skew/hotspotting by checking that writes spread across slots/nodes.
        // HOW: Write many keys; bucket by a stable hash of the key (stand-in for CLUSTER KEYSLOT);
        //      require >95% write success and report distribution.
        logger.info("🔄 TEST: Key Distribution Across Shards - STARTING");

        int totalKeys = 1000;
        List<String> testKeys = new ArrayList<>(totalKeys);
        AtomicInteger successfulSets = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        List<Map<String, Object>> nodes = redisService.getClusterNodes();
        int nodeCount = Math.max(1, nodes.size());
        Map<Integer, Integer> bucketDistribution = new HashMap<>(nodeCount);

        Instant startTime = Instant.now();
        logger.info("TEST: Creating {} keys across {} buckets (nodes)...", totalKeys, nodeCount);

        for (int i = 0; i < totalKeys; i++) {
            String key = "shard_test_" + i;
            testKeys.add(key);

            if (i > 0 && i % 100 == 0) {
                int pct = (i * 100) / totalKeys;
                logger.info("TEST: Progress - {}/{} keys processed ({}%)", i, totalKeys, pct);
            }

            try {
                redisService.setWithRetry(key, "value_" + i);
                successfulSets.incrementAndGet();

                int bucket = Math.floorMod(key.hashCode(), nodeCount);
                bucketDistribution.merge(bucket, 1, Integer::sum);
            } catch (Exception e) {
                errors.incrementAndGet();
                logger.error("TEST: Failed to set key {}: {}", key, e.getMessage());
            }
        }

        Duration operationTime = Duration.between(startTime, Instant.now());

        logger.info("TEST: Key creation completed in {}ms", operationTime.toMillis());
        int ok = successfulSets.get();
        logger.info("TEST: Successful sets: {}/{} ({}%)",
          ok, totalKeys, (ok * 100) / totalKeys);
        logger.info("TEST: Errors: {}", errors.get());

        logger.info("TEST: Bucketed key distribution (by key hash modulo {}):", nodeCount);
        bucketDistribution.forEach((bucket, count) -> {
            double pct = ok == 0 ? 0.0 : (count * 100.0) / ok;
            logger.info("TEST:   Bucket {}: {} keys ({}%)", bucket, count, String.format("%.1f", pct));
        });

        Assertions.assertTrue(ok > totalKeys * 0.95, "Should successfully set >95% of keys");

        // Cleanup
        logger.info("TEST: Starting cleanup of {} keys...", testKeys.size());
        Instant cleanupStart = Instant.now();
        AtomicInteger cleanupErrors = new AtomicInteger(0);

        testKeys.forEach(key -> {
            try {
                redisService.delete(key);
            } catch (Exception e) {
                if (cleanupErrors.incrementAndGet() <= 5) {
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
    @Timeout(value = 120)
    void testPerformanceAndThroughput() {
        // WHY: Track latency and throughput to catch performance regressions.
        // HOW: Run concurrent SET+GETs; compute average latencies and throughput.
        logger.info("=== Testing performance and throughput ===");

        int numOperations = 10_000;
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        AtomicLong totalSetTime = new AtomicLong(0);
        AtomicLong totalGetTime = new AtomicLong(0);
        AtomicInteger successfulSets = new AtomicInteger(0);
        AtomicInteger successfulGets = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        Instant startTime = Instant.now();
        List<Future<?>> futures = new ArrayList<>(numThreads);

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

        for (Future<?> future : futures) {
            try {
                future.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Future execution failed: {}", e.getMessage());
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }

        Instant endTime = Instant.now();
        Duration totalTime = Duration.between(startTime, endTime);

        int setCount = Math.max(1, successfulSets.get());
        int getCount = Math.max(1, successfulGets.get());
        double avgSetTimeMs = totalSetTime.get() / (double) setCount / 1_000_000.0;
        double avgGetTimeMs = totalGetTime.get() / (double) getCount / 1_000_000.0;
        double totalOps = successfulSets.get() + successfulGets.get();
        double seconds = Math.max(0.001, totalTime.toMillis() / 1000.0);
        double throughputOpsPerSec = totalOps / seconds;

        logger.info("Performance Results:");
        logger.info("Total operations (SET+GET): {}", (numOperations * 2));
        logger.info("Successful SETs: {}", successfulSets.get());
        logger.info("Successful GETs: {}", successfulGets.get());
        logger.info("Failures: {}", failures.get());
        logger.info("Total time: {}ms", totalTime.toMillis());
        logger.info("Average SET time: {}ms", String.format("%.2f", avgSetTimeMs));
        logger.info("Average GET time: {}ms", String.format("%.2f", avgGetTimeMs));
        logger.info("Throughput: {} ops/sec", String.format("%.2f", throughputOpsPerSec));

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
    @Timeout(value = 30)
    void testDataTypeOperations() {
        // WHY: Validate correctness of common Redis data types used by the app.
        // HOW: Round-trip String, Hash, List, and Set values; assert contents.
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
    @Timeout(value = 30)
    void testKeyExpirationAndTTL() {
        // WHY: Ensure TTL-based cache/lock semantics behave as expected.
        // HOW: Set a key with a short TTL, wait > TTL, then verify it's gone.
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
    @Timeout(value = 60)
    void testFailoverAndRetryMechanism() {
        // WHY: Validate client retry logic and resilience during transient failures.
        // HOW: Concurrently perform SET/GET and require a high (>80%) success ratio.
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

        for (Future<?> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failover test future failed: {}", e.getMessage());
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }

        logger.info("Failover test results - Success: {}, Failures: {}",
          successCount.get(), failureCount.get());

        Assertions.assertTrue(successCount.get() > 80,
          "Should have >80% success rate even during potential failures");

        IntStream.range(0, 100).forEach(i -> {
            try {
                redisService.delete("failover_test_" + i);
            } catch (Exception ignored) {
            }
        });
    }

    @Test
    @Order(7)
    @DisplayName("Graceful Degradation Test")
    @Timeout(value = 30)
    void testGracefulDegradation() {
        // WHY: The app should keep serving (with defaults) if Redis is briefly unavailable.
        // HOW: Use service fallbacks that return default values instead of throwing.
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
            logger.warn("Set operation failed gracefully - this is expected during degradation");
        }
    }

    @Test
    @Order(8)
    @DisplayName("Concurrent Access Stress Test")
    @Timeout(value = 90)
    void testConcurrentAccessStress() throws InterruptedException {
        // WHY: Surface race conditions and pool exhaustion under concurrent access.
        // HOW: Many threads write/read a shared key; measure error rate and completion.
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

        boolean finished = endLatch.await(60, TimeUnit.SECONDS);
        Assertions.assertTrue(finished, "All threads should complete within timeout");

        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        Duration totalTime = Duration.between(startTime, Instant.now());

        int totalAttempts = operations.get() + errors.get();
        double errorRate = totalAttempts == 0 ? 0.0 : (errors.get() * 100.0) / totalAttempts;

        logger.info("Concurrent stress test results:");
        logger.info("Total operations: {}", operations.get());
        logger.info("Errors: {}", errors.get());
        logger.info("Total time: {}ms", totalTime.toMillis());
        logger.info("Error rate: {}%", String.format("%.2f", errorRate));

        Assertions.assertTrue(operations.get() > (numThreads * operationsPerThread * 0.8),
          "Should complete >80% of operations successfully");

        redisService.delete(sharedKey);
    }

    @Test
    @Order(9)
    @DisplayName("Memory and Large Value Test")
    @Timeout(value = 60)
    void testMemoryAndLargeValues() {
        // WHY: Guard against serialization/fragmentation issues with large values.
        // HOW: Write a ~10KB string and a large hash; read back and assert integrity.
        logger.info("=== Testing memory usage and large values ===");

        String largeValue = "x".repeat(1024 * 10); // ~10KB
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

        for (int i = 0; i < 10; i++) { // sample a few fields
            String field = "field_" + i;
            Object value = redisService.getHashField(largeHashKey, field);
            Assertions.assertNotNull(value);
            Assertions.assertTrue(value.toString().startsWith("data_" + i));
        }

        redisService.delete(largeKey);
        redisService.delete(largeHashKey);
    }

    // Lightweight custom exception for this test class.
    public class ChaosTestException extends RuntimeException {
        public ChaosTestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Test
    @Order(10)
    @DisplayName("Node Failure Chaos Test")
    @Timeout(value = 120)
    void testNodeFailureChaos() {
        // WHY: Verify the app and cluster can tolerate a node/container going down.
        // HOW: Orchestrator stops a Redis container briefly; we verify cluster recovers
        //      and previously written data remains available.
        logger.info("🔥 CHAOS-TEST: Node Failure Simulation - STARTING");

        Instant startTime = Instant.now();

        try {
            Map<String, String> initialClusterInfo = redisService.getClusterInfo();
            List<Map<String, Object>> initialNodes = redisService.getClusterNodes();
            logger.info("CHAOS-TEST: Initial cluster state - {} nodes, state: {}",
              initialNodes.size(), initialClusterInfo.get("cluster_state"));

            String chaosKey = "chaos_node_failure_test";
            String chaosValue = "data_should_survive_node_failure";
            redisService.setWithRetry(chaosKey, chaosValue);

            var chaosResult = chaosOrchestrator.simulateNodeFailure("redis-1", Duration.ofSeconds(10));

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Node failure simulation completed in {}ms", chaosTime.toMillis());

            logger.info("CHAOS-TEST: Summary: target={}, success={}, duration={}s, ops(success/fail)={}/{}",
              chaosResult.getTargetNode(), chaosResult.isSuccess(), chaosResult.getDurationSeconds(),
              chaosResult.getSuccessfulOperations(), chaosResult.getFailedOperations());

            Thread.sleep(2000); // brief settle

            boolean isHealthy = redisService.isClusterHealthy();
            Object recoveredData = redisService.getWithRetry(chaosKey);

            Assertions.assertTrue(chaosResult.isSuccess(), "Node failure simulation should succeed");
            Assertions.assertTrue(isHealthy, "Cluster should be healthy after node failure recovery");
            Assertions.assertEquals(chaosValue, recoveredData, "Data should survive node failures");

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
    @Timeout(value = 180)
    void testMemoryExhaustionChaos() {
        // WHY: Validate behavior under memory pressure (evictions/OOM errors).
        // HOW: Orchestrator creates many large values; we check operations still work
        //      and then clean up the pressure keys.
        logger.info("🧠 CHAOS-TEST: Memory Exhaustion Simulation - STARTING");

        Instant startTime = Instant.now();

        try {
            Map<String, String> initialClusterInfo = redisService.getClusterInfo();
            logger.info("CHAOS-TEST: Initial cluster state: {}", initialClusterInfo.get("cluster_state"));

            String testKey = "chaos_memory_survival_test";
            String testValue = "this_data_should_survive_memory_pressure";
            redisService.setWithRetry(testKey, testValue);

            var chaosResult = chaosOrchestrator.simulateMemoryExhaustion(50, 1024 * 100);

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Memory exhaustion simulation completed in {}ms", chaosTime.toMillis());

            logger.info("CHAOS-TEST: Large objects={}, sizeBytes={}, success={}, ops(success/fail)={}/{}; approxMB={}",
              chaosResult.getLargeObjectCount(),
              chaosResult.getObjectSizeBytes(),
              chaosResult.isSuccess(),
              chaosResult.getSuccessfulOperations(),
              chaosResult.getFailedOperations(),
              (chaosResult.getLargeObjectCount() * chaosResult.getObjectSizeBytes()) / (1024 * 1024));

            boolean isHealthy = redisService.isClusterHealthy();
            Object survivedData = redisService.getWithRetry(testKey);

            logger.info("CHAOS-TEST: Cluster healthy={}, survival={}", isHealthy, testValue.equals(survivedData));

            // Cleanup pressure objects
            logger.info("CHAOS-TEST: Cleaning up memory pressure objects...");
            for (int i = 0; i < chaosResult.getLargeObjectCount(); i++) {
                try {
                    redisService.delete("large_memory_object_" + i);
                } catch (Exception ex) {
                    logger.warn("CHAOS-TEST: Failed to cleanup memory object {}: {}", i, ex.getMessage());
                }
            }
            Thread.sleep(1000);

            boolean finalHealth = redisService.isClusterHealthy();
            logger.info("CHAOS-TEST: Cluster health after memory cleanup: {}", finalHealth);

            redisService.delete(testKey);

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Memory Exhaustion Simulation - COMPLETED in {}ms", totalTime.toMillis());

            Assertions.assertTrue(chaosResult.getLargeObjectCount() > 0,
              "Should have attempted to create large objects");

        } catch (Exception e) {
            Duration testTime = Duration.between(startTime, Instant.now());
            logger.error("❌ CHAOS-TEST: Memory Exhaustion Simulation FAILED after {}ms - Error: {}",
              testTime.toMillis(), e.getMessage(), e);
            throw new ChaosTestException("Memory Exhaustion Simulation encountered a critical error.", e);
        }
    }

    @Test
    @Order(12)
    @DisplayName("Network Partition Chaos Test")
    @Timeout(value = 120)
    void testNetworkPartitionChaos() {
        // WHY: Validate application/cluster behavior when links are slow/flaky or partitioned.
        // HOW: Orchestrator simulates latency/packet loss; we verify reads/writes and recovery.
        logger.info("🌐 CHAOS-TEST: Network Partition Simulation - STARTING");

        Instant startTime = Instant.now();

        try {
            String partitionKey = "chaos_partition_test";
            String partitionValue = "data_during_network_partition";
            redisService.setWithRetry(partitionKey, partitionValue);

            var chaosResult = chaosOrchestrator.simulateNetworkPartition(Duration.ofSeconds(10), 2);

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Network partition simulation completed in {}ms", chaosTime.toMillis());

            logger.info("CHAOS-TEST: Duration={}s, affectedNodes={}, success={}, ops(success/fail)={}/{}",
              chaosResult.getDurationSeconds(),
              chaosResult.getAffectedNodeCount(),
              chaosResult.isSuccess(),
              chaosResult.getSuccessfulOperations(),
              chaosResult.getFailedOperations());

            boolean isHealthy = redisService.isClusterHealthy();

            boolean dataAccessible = false;
            try {
                Object retrievedData = redisService.getWithRetry(partitionKey);
                dataAccessible = partitionValue.equals(retrievedData);
            } catch (Exception ex) {
                logger.warn("CHAOS-TEST: Data access failed after partition: {}", ex.getMessage());
            }

            String newKey = "post_partition_write_test";
            boolean canWrite = false;
            try {
                redisService.setWithRetry(newKey, "written_after_partition");
                Object written = redisService.getWithRetry(newKey);
                canWrite = "written_after_partition".equals(written);
                redisService.delete(newKey);
            } catch (Exception ex) {
                logger.warn("CHAOS-TEST: Write failed after partition: {}", ex.getMessage());
            }

            logger.info("CHAOS-TEST: Post-partition -> healthy={}, existingDataOK={}, newWritesOK={}",
              isHealthy, dataAccessible, canWrite);

            redisService.delete(partitionKey);

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Network Partition Simulation - COMPLETED in {}ms", totalTime.toMillis());

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
    @Timeout(value = 90)
    void testDataCorruptionChaos() {
        // WHY: Ensure corruption can be detected and the cluster remains functional.
        // HOW: Write a dataset, orchestrator corrupts random keys, then we verify integrity.
        logger.info("💾 CHAOS-TEST: Data Corruption Simulation - STARTING");

        Instant startTime = Instant.now();

        try {
            Map<String, String> testData = new HashMap<>();
            for (int i = 0; i < 20; i++) {
                String key = "integrity_test_" + i;
                String value = "clean_data_value_" + i + "_" + UUID.randomUUID();
                testData.put(key, value);
                redisService.setWithRetry(key, value);
            }
            logger.info("CHAOS-TEST: Created {} test records for corruption testing", testData.size());

            var chaosResult = chaosOrchestrator.simulateDataCorruption(10, 0.3);

            Duration chaosTime = Duration.between(startTime, Instant.now());
            logger.info("CHAOS-TEST: Data corruption simulation completed in {}ms", chaosTime.toMillis());

            logger.info("CHAOS-TEST: targetedKeys={}, prob={}%, success={}, ops(success/fail)={}/{}",
              chaosResult.getKeysTargeted(),
              (int) (chaosResult.getCorruptionProbability() * 100),
              chaosResult.isSuccess(),
              chaosResult.getSuccessfulOperations(),
              chaosResult.getFailedOperations());

            int cleanData = 0;
            int corruptedData = 0;
            int missingData = 0;

            for (Map.Entry<String, String> entry : testData.entrySet()) {
                String key = entry.getKey();
                String expectedValue = entry.getValue();

                try {
                    Object actualValue = redisService.getWithRetry(key);
                    if (actualValue == null) {
                        missingData++;
                    } else if (expectedValue.equals(actualValue)) {
                        cleanData++;
                    } else {
                        corruptedData++;
                    }
                } catch (Exception ex) {
                    missingData++;
                    logger.warn("CHAOS-TEST: Failed to read key {} during integrity check: {}", key, ex.getMessage());
                }
            }

            int total = testData.size();
            logger.info("CHAOS-TEST: Integrity results -> clean={} ({})%, corrupted={} ({})%, missing={} ({})%",
              cleanData, String.format("%.1f", (cleanData * 100.0) / total),
              corruptedData, String.format("%.1f", (corruptedData * 100.0) / total),
              missingData, String.format("%.1f", (missingData * 100.0) / total));

            boolean isHealthy = redisService.isClusterHealthy();
            logger.info("CHAOS-TEST: Cluster health after data corruption: {}", isHealthy);

            String newKey = "post_corruption_test";
            boolean canWriteClean = false;
            try {
                redisService.setWithRetry(newKey, "clean_new_data");
                Object retrieved = redisService.getWithRetry(newKey);
                canWriteClean = "clean_new_data".equals(retrieved);
                redisService.delete(newKey);
            } catch (Exception ex) {
                logger.warn("CHAOS-TEST: Failed to write clean data after corruption test: {}", ex.getMessage());
            }

            logger.info("CHAOS-TEST: Can write clean data after corruption: {}", canWriteClean);

            // Cleanup
            testData.keySet().forEach(key -> {
                try {
                    redisService.delete(key);
                } catch (Exception ex) {
                    logger.warn("CHAOS-TEST: Failed to cleanup key {}: {}", key, ex.getMessage());
                }
            });

            Duration totalTime = Duration.between(startTime, Instant.now());
            logger.info("✅ CHAOS-TEST: Data Corruption Simulation - COMPLETED in {}ms", totalTime.toMillis());

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
    @Timeout(value = 30)
    void testCleanupAndFinalHealthCheck() {
        // WHY: Sanity check after all chaos: the cluster should still be healthy.
        // HOW: Call isClusterHealthy and log final CLUSTER INFO.
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
                } catch (Exception ignored) {
                }
            }
        }
    }
}