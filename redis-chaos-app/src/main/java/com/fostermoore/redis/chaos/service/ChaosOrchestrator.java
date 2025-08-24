package com.fostermoore.redis.chaos.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class ChaosOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(ChaosOrchestrator.class);

    @Autowired
    private RedisClusterService redisService;

    private final ExecutorService chaosExecutor = Executors.newCachedThreadPool();
    private final Map<String, CompletableFuture<Void>> activeChaosEvents = new ConcurrentHashMap<>();
    private final AtomicBoolean chaosMode = new AtomicBoolean(false);

    /**
     * Simulate node failure by stopping a Redis container
     */
    public NodeFailureResult simulateNodeFailure(String nodeId, Duration duration) {
        return simulateNodeFailure(nodeId, (int) duration.getSeconds());
    }
    
    public NodeFailureResult simulateNodeFailure(String nodeId, int durationSeconds) {
        logger.info("🔥 CHAOS: Starting node failure simulation for node: {}", nodeId);
        
        Instant startTime = Instant.now();
        NodeFailureResult result = new NodeFailureResult("NODE_FAILURE", nodeId);
        
        try {
            // Get cluster state before chaos
            Map<String, String> beforeState = redisService.getClusterInfo();
            List<Map<String, Object>> beforeNodes = redisService.getClusterNodes();
            result.setBeforeState(Map.of(
                "clusterInfo", beforeState,
                "nodeCount", beforeNodes.size(),
                "clusterState", beforeState.get("cluster_state")
            ));
            
            // Find Redis container to stop (simulate by container name pattern)
            String containerName = "redis-" + extractNodeNumber(nodeId);
            logger.info("CHAOS: Attempting to stop container: {}", containerName);
            
            // Stop the Redis container
            ProcessResult stopResult = executeCommand("docker", "stop", containerName);
            if (stopResult.exitCode == 0) {
                logger.info("CHAOS: ✅ Successfully stopped Redis container: {}", containerName);
                result.addEvent("Container stopped successfully");
            } else {
                logger.error("CHAOS: ❌ Failed to stop container: {}", stopResult.stderr);
                result.addEvent("Failed to stop container: " + stopResult.stderr);
                result.setSuccess(false);
                return result;
            }
            
            // Wait and monitor cluster during outage
            Thread.sleep(2000); // Allow time for cluster to detect failure
            
            Map<String, String> duringState = null;
            try {
                duringState = redisService.getClusterInfo();
                result.addEvent("Cluster state during failure: " + duringState.get("cluster_state"));
                logger.info("CHAOS: Cluster state during failure: {}", duringState.get("cluster_state"));
            } catch (Exception e) {
                result.addEvent("Cluster unreachable during failure: " + e.getMessage());
                logger.warn("CHAOS: Cluster partially unreachable during failure: {}", e.getMessage());
            }
            
            // Test application resilience during outage
            AtomicInteger successfulOps = new AtomicInteger(0);
            AtomicInteger failedOps = new AtomicInteger(0);
            
            logger.info("CHAOS: Testing application resilience during node failure...");
            for (int i = 0; i < 50; i++) {
                try {
                    String key = "chaos_test_" + i;
                    redisService.setWithRetry(key, "resilience_test_" + i);
                    Object retrieved = redisService.getWithRetry(key);
                    if (retrieved != null) {
                        successfulOps.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    if (i < 5) { // Log first few failures
                        logger.debug("CHAOS: Operation failed during outage: {}", e.getMessage());
                    }
                }
            }
            
            result.addEvent(String.format("During outage - Successful ops: %d, Failed ops: %d", 
                successfulOps.get(), failedOps.get()));
            
            // Wait for specified duration
            if (durationSeconds > 2) {
                logger.info("CHAOS: Waiting {} more seconds before recovery...", durationSeconds - 2);
                Thread.sleep((durationSeconds - 2) * 1000);
            }
            
            // Restart the container
            logger.info("CHAOS: Restarting Redis container: {}", containerName);
            ProcessResult startResult = executeCommand("docker", "start", containerName);
            if (startResult.exitCode == 0) {
                logger.info("CHAOS: ✅ Successfully restarted Redis container: {}", containerName);
                result.addEvent("Container restarted successfully");
            } else {
                logger.error("CHAOS: ❌ Failed to restart container: {}", startResult.stderr);
                result.addEvent("Failed to restart container: " + startResult.stderr);
            }
            
            // Wait for cluster to stabilize
            logger.info("CHAOS: Waiting for cluster to stabilize...");
            Thread.sleep(5000);
            
            // Verify cluster recovery
            int maxRecoveryAttempts = 10;
            boolean recovered = false;
            for (int attempt = 1; attempt <= maxRecoveryAttempts; attempt++) {
                try {
                    Map<String, String> afterState = redisService.getClusterInfo();
                    List<Map<String, Object>> afterNodes = redisService.getClusterNodes();
                    
                    if ("ok".equals(afterState.get("cluster_state")) && 
                        afterNodes.size() >= beforeNodes.size()) {
                        recovered = true;
                        result.setAfterState(Map.of(
                            "clusterInfo", afterState,
                            "nodeCount", afterNodes.size(),
                            "clusterState", afterState.get("cluster_state"),
                            "recoveryAttempt", attempt
                        ));
                        logger.info("CHAOS: ✅ Cluster recovered after {} attempts", attempt);
                        break;
                    } else {
                        logger.info("CHAOS: Recovery attempt {}/{} - State: {}, Nodes: {}", 
                            attempt, maxRecoveryAttempts, afterState.get("cluster_state"), afterNodes.size());
                        Thread.sleep(2000);
                    }
                } catch (Exception e) {
                    logger.warn("CHAOS: Recovery attempt {}/{} failed: {}", attempt, maxRecoveryAttempts, e.getMessage());
                    Thread.sleep(2000);
                }
            }
            
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ Cluster fully recovered");
                logger.info("CHAOS: Node failure simulation completed successfully in {}ms", 
                    result.getDuration().toMillis());
            } else {
                result.addEvent("❌ Cluster failed to recover within timeout");
                logger.error("CHAOS: Node failure simulation completed but cluster did not recover");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Chaos simulation failed: " + e.getMessage());
            logger.error("CHAOS: Node failure simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate memory exhaustion by filling Redis with large amounts of data
     */
    public MemoryExhaustionResult simulateMemoryExhaustion(int largeObjectCount, int objectSizeBytes) {
        logger.info("🧠 CHAOS: Starting memory exhaustion simulation - {} objects of {} bytes each", largeObjectCount, objectSizeBytes);
        
        Instant startTime = Instant.now();
        MemoryExhaustionResult result = new MemoryExhaustionResult("MEMORY_EXHAUSTION", "cluster", largeObjectCount, objectSizeBytes);
        
        try {
            // Get baseline memory usage
            Map<String, String> beforeState = redisService.getClusterInfo();
            result.setBeforeState(Map.of("clusterState", beforeState.get("cluster_state")));
            
            String largeValue = "X".repeat(objectSizeBytes);
            logger.info("CHAOS: Generating {} objects of {} bytes each to consume memory", largeObjectCount, objectSizeBytes);
            
            // Fill Redis with large data
            AtomicInteger keysCreated = new AtomicInteger(0);
            AtomicInteger errors = new AtomicInteger(0);
            
            for (int i = 0; i < largeObjectCount && errors.get() < 50; i++) {
                try {
                    String key = "large_memory_object_" + i;
                    redisService.setWithRetry(key, largeValue);
                    keysCreated.incrementAndGet();
                    
                    if (i > 0 && i % 10 == 0) {
                        logger.info("CHAOS: Created {} memory-consuming objects ({:.1f}MB estimated)", 
                            i, (i * objectSizeBytes) / (1024.0 * 1024.0));
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    if (errors.get() <= 5) {
                        logger.warn("CHAOS: Memory exhaustion error {}: {}", errors.get(), e.getMessage());
                    }
                    
                    if (e.getMessage().contains("memory") || e.getMessage().contains("OOM")) {
                        logger.info("CHAOS: ✅ Successfully triggered memory pressure - {}", e.getMessage());
                        result.addEvent("Memory pressure triggered: " + e.getMessage());
                        break;
                    }
                }
            }
            
            result.addEvent(String.format("Created %d objects consuming ~%.1fMB, %d errors", 
                keysCreated.get(), (keysCreated.get() * objectSizeBytes) / (1024.0 * 1024.0), errors.get()));
            result.setSuccessfulOperations(keysCreated.get());
            result.setFailedOperations(errors.get());
            
            // Test system behavior under memory pressure
            logger.info("CHAOS: Testing system behavior under memory pressure...");
            AtomicInteger readSuccesses = new AtomicInteger(0);
            AtomicInteger readFailures = new AtomicInteger(0);
            
            for (int i = 0; i < 100; i++) {
                try {
                    String testKey = "memory_test_" + i;
                    redisService.setWithRetry(testKey, "small_value");
                    Object retrieved = redisService.getWithRetry(testKey);
                    if (retrieved != null) {
                        readSuccesses.incrementAndGet();
                    }
                } catch (Exception e) {
                    readFailures.incrementAndGet();
                }
            }
            
            result.addEvent(String.format("Under memory pressure - Read successes: %d, failures: %d", 
                readSuccesses.get(), readFailures.get()));
            
            boolean recovered = readSuccesses.get() > readFailures.get();
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ Memory exhaustion simulation completed successfully");
                logger.info("CHAOS: Memory exhaustion simulation completed successfully");
            } else {
                result.addEvent("❌ System struggled significantly under memory pressure");
                logger.warn("CHAOS: Memory exhaustion simulation completed with significant issues");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Memory exhaustion simulation failed: " + e.getMessage());
            logger.error("CHAOS: Memory exhaustion simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }
    


    /**
     * Simulate network partition by introducing latency and packet loss
     */  
    public NetworkPartitionResult simulateNetworkPartition(Duration duration, int affectedNodeCount) {
        return simulateNetworkPartition((int) duration.getSeconds(), affectedNodeCount);
    }
    
    public NetworkPartitionResult simulateNetworkPartition(int durationSeconds, int affectedNodeCount) {
        logger.info("🌐 CHAOS: Starting network partition simulation - affecting {} nodes for {}s", 
            affectedNodeCount, durationSeconds);
        
        Instant startTime = Instant.now();
        NetworkPartitionResult result = new NetworkPartitionResult("NETWORK_PARTITION", "cluster", durationSeconds, affectedNodeCount);
        
        try {
            // Get cluster state before partition
            Map<String, String> beforeState = redisService.getClusterInfo();
            List<Map<String, Object>> beforeNodes = redisService.getClusterNodes();
            result.setBeforeState(Map.of(
                "clusterState", beforeState.get("cluster_state"),
                "nodeCount", beforeNodes.size()
            ));
            
            // Simulate network issues by introducing artificial delays and timeouts
            logger.info("CHAOS: Introducing network latency and connection issues...");
            
            // Test cluster behavior with simulated network issues
            AtomicInteger slowOperations = new AtomicInteger(0);
            AtomicInteger failedOperations = new AtomicInteger(0);
            AtomicLong totalLatency = new AtomicLong(0);
            
            // Create a scenario where operations are slower/failing
            for (int i = 0; i < 100; i++) {
                try {
                    Instant opStart = Instant.now();
                    
                    // Simulate network delay based on affected node count
                    if (Math.random() < (affectedNodeCount / 6.0)) { // Higher chance if more nodes affected
                        // Simulate packet loss by introducing delays
                        Thread.sleep((int)(Math.random() * 1000 + 500)); // 500-1500ms delay
                    }
                    
                    String key = "network_chaos_" + i;
                    redisService.setWithRetry(key, "network_test_" + i);
                    Object retrieved = redisService.getWithRetry(key);
                    
                    long latency = Duration.between(opStart, Instant.now()).toMillis();
                    totalLatency.addAndGet(latency);
                    
                    if (latency > 500) {
                        slowOperations.incrementAndGet();
                    }
                    
                    if (retrieved == null) {
                        failedOperations.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    failedOperations.incrementAndGet();
                    if (failedOperations.get() <= 5) {
                        logger.debug("CHAOS: Network partition caused operation failure: {}", e.getMessage());
                    }
                }
            }
            
            result.setSuccessfulOperations(100 - failedOperations.get());
            result.setFailedOperations(failedOperations.get());
            
            double avgLatency = totalLatency.get() / 100.0;
            result.addEvent(String.format("Network issues - Slow ops: %d, Failed ops: %d, Avg latency: %.1fms", 
                slowOperations.get(), failedOperations.get(), avgLatency));
            
            // Wait for partition duration
            logger.info("CHAOS: Maintaining network partition for {} seconds...", durationSeconds);
            Thread.sleep(durationSeconds * 1000);
            
            // Test cluster state during partition
            try {
                Map<String, String> duringState = redisService.getClusterInfo();
                result.addEvent("Cluster state during partition: " + duringState.get("cluster_state"));
            } catch (Exception e) {
                result.addEvent("Cluster unreachable during partition: " + e.getMessage());
            }
            
            // Recovery phase
            logger.info("CHAOS: Network partition ended, testing recovery...");
            Thread.sleep(2000);
            
            // Test recovery
            AtomicInteger recoverySuccesses = new AtomicInteger(0);
            for (int i = 0; i < 50; i++) {
                try {
                    String key = "recovery_test_" + i;
                    redisService.setWithRetry(key, "recovery_value_" + i);
                    Object retrieved = redisService.getWithRetry(key);
                    if (retrieved != null) {
                        recoverySuccesses.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Expected during recovery
                }
            }
            
            Map<String, String> afterState = redisService.getClusterInfo();
            result.setAfterState(Map.of(
                "clusterState", afterState.get("cluster_state"),
                "recoverySuccessRate", (recoverySuccesses.get() * 100) / 50
            ));
            
            boolean recovered = "ok".equals(afterState.get("cluster_state")) && recoverySuccesses.get() > 40;
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ Network partition recovery successful");
                logger.info("CHAOS: Network partition simulation completed successfully");
            } else {
                result.addEvent("❌ Cluster did not fully recover from network partition");
                logger.warn("CHAOS: Network partition simulation completed with issues");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Network partition simulation failed: " + e.getMessage());
            logger.error("CHAOS: Network partition simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }
    
    public ChaosResult simulateNetworkPartition(int durationSeconds, double packetLossPercent) {
        logger.info("🌐 CHAOS: Starting network partition simulation - {}% packet loss for {}s", 
            packetLossPercent, durationSeconds);
        
        Instant startTime = Instant.now();
        ChaosResult result = new ChaosResult("NETWORK_PARTITION", "cluster");
        
        try {
            // Get cluster state before partition
            Map<String, String> beforeState = redisService.getClusterInfo();
            List<Map<String, Object>> beforeNodes = redisService.getClusterNodes();
            result.setBeforeState(Map.of(
                "clusterState", beforeState.get("cluster_state"),
                "nodeCount", beforeNodes.size()
            ));
            
            // Simulate network issues by introducing artificial delays and timeouts
            logger.info("CHAOS: Introducing network latency and connection issues...");
            
            // Test cluster behavior with simulated network issues
            AtomicInteger slowOperations = new AtomicInteger(0);
            AtomicInteger failedOperations = new AtomicInteger(0);
            AtomicLong totalLatency = new AtomicLong(0);
            
            // Create a scenario where operations are slower/failing
            for (int i = 0; i < 100; i++) {
                try {
                    Instant opStart = Instant.now();
                    
                    // Simulate network delay
                    if (Math.random() < packetLossPercent / 100.0) {
                        // Simulate packet loss by introducing delays
                        Thread.sleep((int)(Math.random() * 1000 + 500)); // 500-1500ms delay
                    }
                    
                    String key = "network_chaos_" + i;
                    redisService.setWithRetry(key, "network_test_" + i);
                    Object retrieved = redisService.getWithRetry(key);
                    
                    long latency = Duration.between(opStart, Instant.now()).toMillis();
                    totalLatency.addAndGet(latency);
                    
                    if (latency > 500) {
                        slowOperations.incrementAndGet();
                    }
                    
                    if (retrieved == null) {
                        failedOperations.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    failedOperations.incrementAndGet();
                    if (failedOperations.get() <= 5) {
                        logger.debug("CHAOS: Network partition caused operation failure: {}", e.getMessage());
                    }
                }
            }
            
            double avgLatency = totalLatency.get() / 100.0;
            result.addEvent(String.format("Network issues - Slow ops: %d, Failed ops: %d, Avg latency: %.1fms", 
                slowOperations.get(), failedOperations.get(), avgLatency));
            
            // Wait for partition duration
            logger.info("CHAOS: Maintaining network partition for {} seconds...", durationSeconds);
            Thread.sleep(durationSeconds * 1000);
            
            // Test cluster state during partition
            try {
                Map<String, String> duringState = redisService.getClusterInfo();
                result.addEvent("Cluster state during partition: " + duringState.get("cluster_state"));
            } catch (Exception e) {
                result.addEvent("Cluster unreachable during partition: " + e.getMessage());
            }
            
            // Recovery phase
            logger.info("CHAOS: Network partition ended, testing recovery...");
            Thread.sleep(2000);
            
            // Test recovery
            AtomicInteger recoverySuccesses = new AtomicInteger(0);
            for (int i = 0; i < 50; i++) {
                try {
                    String key = "recovery_test_" + i;
                    redisService.setWithRetry(key, "recovery_value_" + i);
                    Object retrieved = redisService.getWithRetry(key);
                    if (retrieved != null) {
                        recoverySuccesses.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Expected during recovery
                }
            }
            
            Map<String, String> afterState = redisService.getClusterInfo();
            result.setAfterState(Map.of(
                "clusterState", afterState.get("cluster_state"),
                "recoverySuccessRate", (recoverySuccesses.get() * 100) / 50
            ));
            
            boolean recovered = "ok".equals(afterState.get("cluster_state")) && recoverySuccesses.get() > 40;
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ Network partition recovery successful");
                logger.info("CHAOS: Network partition simulation completed successfully");
            } else {
                result.addEvent("❌ Cluster did not fully recover from network partition");
                logger.warn("CHAOS: Network partition simulation completed with issues");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Network partition simulation failed: " + e.getMessage());
            logger.error("CHAOS: Network partition simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate latency injection into Redis operations
     */
    public LatencyInjectionResult simulateLatencyInjection(int durationSeconds, int latencyMs, double probability) {
        logger.info("🐌 CHAOS: Starting latency injection - {}ms latency with {:.1f}% probability for {}s", 
            latencyMs, probability * 100, durationSeconds);
        
        Instant startTime = Instant.now();
        LatencyInjectionResult result = new LatencyInjectionResult("LATENCY_INJECTION", "application", 
            durationSeconds, latencyMs, probability);
        
        try {
            // Set up latency injection flag
            AtomicInteger injectedOps = new AtomicInteger(0);
            AtomicInteger normalOps = new AtomicInteger(0);
            AtomicInteger failures = new AtomicInteger(0);
            
            // Test operations with latency injection
            CompletableFuture<Void> testFuture = CompletableFuture.runAsync(() -> {
                Random random = new Random();
                long endTime = System.currentTimeMillis() + (durationSeconds * 1000);
                
                while (System.currentTimeMillis() < endTime) {
                    try {
                        // Inject latency based on probability
                        if (random.nextDouble() < probability) {
                            Thread.sleep(latencyMs);
                            injectedOps.incrementAndGet();
                        } else {
                            normalOps.incrementAndGet();
                        }
                        
                        // Perform Redis operation
                        String testKey = "latency_test_" + System.currentTimeMillis();
                        redisService.setWithRetry(testKey, "latency_test_value");
                        redisService.getWithRetry(testKey);
                        redisService.delete(testKey);
                        
                        Thread.sleep(50); // Brief pause between operations
                        
                    } catch (Exception e) {
                        failures.incrementAndGet();
                        if (failures.get() <= 5) {
                            logger.debug("CHAOS: Latency injection caused failure: {}", e.getMessage());
                        }
                    }
                }
            });
            
            testFuture.get();
            
            result.setSuccessfulOperations(injectedOps.get() + normalOps.get());
            result.setFailedOperations(failures.get());
            result.addEvent(String.format("Latency injected into %d operations, %d normal operations, %d failures", 
                injectedOps.get(), normalOps.get(), failures.get()));
            
            boolean success = (injectedOps.get() + normalOps.get()) > failures.get();
            result.setSuccess(success);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (success) {
                result.addEvent("✅ Latency injection simulation completed successfully");
                logger.info("CHAOS: Latency injection completed - injected: {}, normal: {}, failures: {}", 
                    injectedOps.get(), normalOps.get(), failures.get());
            } else {
                result.addEvent("❌ Latency injection caused excessive failures");
                logger.warn("CHAOS: Latency injection completed with high failure rate");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Latency injection simulation failed: " + e.getMessage());
            logger.error("CHAOS: Latency injection simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate exception injection into application operations
     */
    public ExceptionInjectionResult simulateExceptionInjection(int durationSeconds, double probability, String exceptionType) {
        logger.info("💥 CHAOS: Starting exception injection - {} with {:.1f}% probability for {}s", 
            exceptionType, probability * 100, durationSeconds);
        
        Instant startTime = Instant.now();
        ExceptionInjectionResult result = new ExceptionInjectionResult("EXCEPTION_INJECTION", "application", 
            durationSeconds, probability, exceptionType);
        
        try {
            AtomicInteger exceptionsThrown = new AtomicInteger(0);
            AtomicInteger normalOps = new AtomicInteger(0);
            AtomicInteger handledExceptions = new AtomicInteger(0);
            
            CompletableFuture<Void> testFuture = CompletableFuture.runAsync(() -> {
                Random random = new Random();
                long endTime = System.currentTimeMillis() + (durationSeconds * 1000);
                
                while (System.currentTimeMillis() < endTime) {
                    try {
                        // Inject exception based on probability
                        if (random.nextDouble() < probability) {
                            exceptionsThrown.incrementAndGet();
                            
                            switch (exceptionType.toLowerCase()) {
                                case "ioexception":
                                    throw new RuntimeException("Simulated IOException", 
                                        new java.io.IOException("Chaos injection"));
                                case "runtimeexception":
                                default:
                                    throw new RuntimeException("Chaos monkey exception injection");
                            }
                        }
                        
                        // Normal operation
                        String testKey = "exception_test_" + System.currentTimeMillis();
                        redisService.setWithRetry(testKey, "exception_test_value");
                        redisService.getWithRetry(testKey);
                        redisService.delete(testKey);
                        normalOps.incrementAndGet();
                        
                        Thread.sleep(100); // Brief pause
                        
                    } catch (RuntimeException e) {
                        if (e.getMessage().contains("Chaos monkey")) {
                            handledException(e);
                            handledExceptions.incrementAndGet();
                        } else {
                            throw e; // Re-throw non-chaos exceptions
                        }
                    } catch (Exception e) {
                        logger.warn("CHAOS: Unexpected exception during injection test: {}", e.getMessage());
                    }
                }
            });
            
            testFuture.get();
            
            result.setSuccessfulOperations(normalOps.get() + handledExceptions.get());
            result.setFailedOperations(0); // All exceptions were handled
            result.addEvent(String.format("Exceptions injected: %d, Normal operations: %d, Handled exceptions: %d", 
                exceptionsThrown.get(), normalOps.get(), handledExceptions.get()));
            
            result.setSuccess(true);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Exception injection simulation completed successfully");
            
            logger.info("CHAOS: Exception injection completed - thrown: {}, handled: {}, normal: {}", 
                exceptionsThrown.get(), handledExceptions.get(), normalOps.get());
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Exception injection simulation failed: " + e.getMessage());
            logger.error("CHAOS: Exception injection simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate application crash
     */
    public ApplicationCrashResult simulateApplicationCrash(int delaySeconds) {
        logger.info("💀 CHAOS: Starting application crash simulation - crash in {}s", delaySeconds);
        
        Instant startTime = Instant.now();
        ApplicationCrashResult result = new ApplicationCrashResult("APPLICATION_CRASH", "application", delaySeconds);
        
        try {
            result.addEvent("Scheduling application crash in " + delaySeconds + " seconds");
            
            // Schedule the crash
            CompletableFuture<Void> crashFuture = CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(delaySeconds * 1000);
                    logger.error("🔥 CHAOS: SIMULATED APPLICATION CRASH - System.exit(1)");
                    result.addEvent("Application crash executed");
                    
                    // In a real scenario, this would crash the app
                    // System.exit(1);
                    
                    // For testing, we simulate the crash without actually crashing
                    throw new RuntimeException("SIMULATED APPLICATION CRASH - This is a chaos test");
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("CHAOS: Application crash simulation was interrupted");
                }
            });
            
            // Wait a bit then cancel to avoid actual crash
            Thread.sleep(Math.min(delaySeconds * 1000, 2000));
            crashFuture.cancel(true);
            
            result.setSuccess(true);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Application crash simulation completed (crash was simulated, not executed)");
            
            logger.info("CHAOS: Application crash simulation completed without actually crashing");
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Application crash simulation failed: " + e.getMessage());
            logger.error("CHAOS: Application crash simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate resource saturation (CPU/Memory)
     */
    public ResourceSaturationResult simulateResourceSaturation(String resourceType, int durationSeconds, int intensity) {
        logger.info("🔥 CHAOS: Starting resource saturation - {} at {}% for {}s", 
            resourceType, intensity, durationSeconds);
        
        Instant startTime = Instant.now();
        ResourceSaturationResult result = new ResourceSaturationResult("RESOURCE_SATURATION", "host", 
            resourceType, durationSeconds, intensity);
        
        try {
            List<CompletableFuture<Void>> saturationTasks = new ArrayList<>();
            
            if (resourceType.equals("cpu") || resourceType.equals("both")) {
                // CPU saturation
                int cpuThreads = Runtime.getRuntime().availableProcessors();
                for (int i = 0; i < cpuThreads; i++) {
                    CompletableFuture<Void> cpuTask = CompletableFuture.runAsync(() -> {
                        long endTime = System.currentTimeMillis() + (durationSeconds * 1000);
                        while (System.currentTimeMillis() < endTime) {
                            // CPU-intensive work
                            double result1 = 0;
                            for (int j = 0; j < 1000000; j++) {
                                result1 += Math.sin(j) * Math.cos(j);
                            }
                            
                            // Throttle based on intensity
                            if (intensity < 100) {
                                try {
                                    Thread.sleep((100 - intensity) / 10);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                    });
                    saturationTasks.add(cpuTask);
                }
                result.addEvent("Started CPU saturation with " + cpuThreads + " threads");
            }
            
            if (resourceType.equals("memory") || resourceType.equals("both")) {
                // Memory saturation
                CompletableFuture<Void> memoryTask = CompletableFuture.runAsync(() -> {
                    List<byte[]> memoryHog = new ArrayList<>();
                    long endTime = System.currentTimeMillis() + (durationSeconds * 1000);
                    
                    try {
                        while (System.currentTimeMillis() < endTime) {
                            // Allocate memory based on intensity
                            int allocation = (intensity * 1024 * 1024) / 100; // MB based on intensity
                            byte[] chunk = new byte[allocation];
                            memoryHog.add(chunk);
                            
                            Thread.sleep(100); // Brief pause
                            
                            // Prevent out of memory by limiting size
                            if (memoryHog.size() > 100) {
                                memoryHog.subList(0, 50).clear();
                            }
                        }
                    } catch (OutOfMemoryError e) {
                        logger.info("CHAOS: Successfully triggered memory pressure");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        memoryHog.clear();
                    }
                });
                saturationTasks.add(memoryTask);
                result.addEvent("Started memory saturation");
            }
            
            // Wait for completion or timeout
            CompletableFuture<Void> allTasks = CompletableFuture.allOf(
                saturationTasks.toArray(new CompletableFuture[0]));
            
            try {
                allTasks.get(durationSeconds + 10, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.debug("CHAOS: Resource saturation tasks completed or were interrupted");
            }
            
            // Cancel any remaining tasks
            saturationTasks.forEach(task -> task.cancel(true));
            
            result.setSuccess(true);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Resource saturation simulation completed");
            
            logger.info("CHAOS: Resource saturation simulation completed");
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Resource saturation simulation failed: " + e.getMessage());
            logger.error("CHAOS: Resource saturation simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate disk space exhaustion
     */
    public DiskExhaustionResult simulateDiskExhaustion(String targetPath, int targetSizeMB) {
        logger.info("💾 CHAOS: Starting disk exhaustion - filling {} with {}MB", targetPath, targetSizeMB);
        
        Instant startTime = Instant.now();
        DiskExhaustionResult result = new DiskExhaustionResult("DISK_EXHAUSTION", "host", targetPath, targetSizeMB);
        
        try {
            List<String> createdFiles = new ArrayList<>();
            long targetBytes = targetSizeMB * 1024L * 1024L;
            long bytesWritten = 0;
            int fileCounter = 0;
            
            byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
            java.util.Arrays.fill(buffer, (byte) 'X');
            
            while (bytesWritten < targetBytes) {
                String filename = targetPath + "/chaos_disk_fill_" + fileCounter + ".tmp";
                
                try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filename)) {
                    long remainingBytes = Math.min(targetBytes - bytesWritten, buffer.length);
                    fos.write(buffer, 0, (int) remainingBytes);
                    bytesWritten += remainingBytes;
                    createdFiles.add(filename);
                    fileCounter++;
                    
                    if (fileCounter % 10 == 0) {
                        logger.info("CHAOS: Written {} MB so far", bytesWritten / (1024 * 1024));
                    }
                    
                } catch (Exception e) {
                    if (e.getMessage().contains("No space left")) {
                        logger.info("CHAOS: ✅ Successfully exhausted disk space");
                        result.addEvent("Disk space exhausted: " + e.getMessage());
                        break;
                    } else {
                        throw e;
                    }
                }
            }
            
            result.addEvent(String.format("Created %d files totaling %.1f MB", 
                createdFiles.size(), bytesWritten / (1024.0 * 1024.0)));
            
            // Cleanup created files
            logger.info("CHAOS: Cleaning up {} created files...", createdFiles.size());
            int cleanedUp = 0;
            for (String filename : createdFiles) {
                try {
                    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(filename));
                    cleanedUp++;
                } catch (Exception e) {
                    logger.warn("CHAOS: Failed to cleanup file {}: {}", filename, e.getMessage());
                }
            }
            
            result.addEvent("Cleaned up " + cleanedUp + " files");
            result.setSuccess(true);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Disk exhaustion simulation completed");
            
            logger.info("CHAOS: Disk exhaustion simulation completed - wrote {:.1f}MB, cleaned up {} files", 
                bytesWritten / (1024.0 * 1024.0), cleanedUp);
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Disk exhaustion simulation failed: " + e.getMessage());
            logger.error("CHAOS: Disk exhaustion simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate clock manipulation / time bombs
     */
    public ClockManipulationResult simulateClockManipulation(int offsetMinutes, int durationSeconds) {
        logger.info("⏰ CHAOS: Starting clock manipulation - offset {} minutes for {}s", 
            offsetMinutes, durationSeconds);
        
        Instant startTime = Instant.now();
        ClockManipulationResult result = new ClockManipulationResult("CLOCK_MANIPULATION", "application", 
            offsetMinutes, durationSeconds);
        
        try {
            // Simulate clock manipulation by testing time-sensitive operations
            result.addEvent("Testing time-sensitive operations with simulated clock offset");
            
            // Test 1: TTL operations with simulated time shift
            String ttlKey = "clock_test_ttl";
            redisService.setWithExpiration(ttlKey, "time_sensitive_data", 5); // 5 seconds TTL
            
            // Simulate time passing
            Thread.sleep(1000);
            
            // Check if key still exists (should exist normally)
            boolean keyExists1 = redisService.exists(ttlKey);
            result.addEvent("TTL test after 1s: key exists = " + keyExists1);
            
            // Test 2: Timestamp operations
            long currentTime = System.currentTimeMillis();
            long simulatedTime = currentTime + (offsetMinutes * 60L * 1000L);
            
            String timestampKey = "clock_test_timestamp";
            redisService.setWithRetry(timestampKey, String.valueOf(simulatedTime));
            
            // Simulate checking timestamp after time manipulation
            String retrievedTimestamp = (String) redisService.getWithRetry(timestampKey);
            long parsedTime = Long.parseLong(retrievedTimestamp);
            long timeDiff = parsedTime - currentTime;
            
            result.addEvent(String.format("Timestamp test: simulated time offset = %d minutes", 
                timeDiff / (60 * 1000)));
            
            // Test 3: Time-based cache expiry simulation
            AtomicInteger expiredItems = new AtomicInteger(0);
            AtomicInteger validItems = new AtomicInteger(0);
            
            CompletableFuture<Void> timeTest = CompletableFuture.runAsync(() -> {
                long testEndTime = System.currentTimeMillis() + (durationSeconds * 1000);
                
                while (System.currentTimeMillis() < testEndTime) {
                    try {
                        String timeKey = "time_test_" + System.currentTimeMillis();
                        long itemTimestamp = System.currentTimeMillis() + (offsetMinutes * 60L * 1000L);
                        
                        redisService.setWithRetry(timeKey, String.valueOf(itemTimestamp));
                        
                        // Simulate checking if item has "expired" with clock manipulation
                        long currentTimestamp = System.currentTimeMillis();
                        if (itemTimestamp < currentTimestamp) {
                            expiredItems.incrementAndGet();
                        } else {
                            validItems.incrementAndGet();
                        }
                        
                        redisService.delete(timeKey);
                        Thread.sleep(100);
                        
                    } catch (Exception e) {
                        logger.debug("CHAOS: Time test operation failed: {}", e.getMessage());
                    }
                }
            });
            
            timeTest.get();
            
            result.addEvent(String.format("Time-based operations: %d expired, %d valid", 
                expiredItems.get(), validItems.get()));
            
            // Cleanup
            redisService.delete(ttlKey);
            redisService.delete(timestampKey);
            
            result.setSuccess(true);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Clock manipulation simulation completed");
            
            logger.info("CHAOS: Clock manipulation simulation completed - expired: {}, valid: {}", 
                expiredItems.get(), validItems.get());
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Clock manipulation simulation failed: " + e.getMessage());
            logger.error("CHAOS: Clock manipulation simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Comprehensive node failure test targeting masters and slaves with client resilience validation
     */
    public ComprehensiveNodeFailureResult simulateComprehensiveNodeFailure(int durationSeconds) {
        logger.info("🔥 CHAOS: Starting comprehensive node failure test - {} seconds duration", durationSeconds);
        
        Instant startTime = Instant.now();
        ComprehensiveNodeFailureResult result = new ComprehensiveNodeFailureResult("COMPREHENSIVE_NODE_FAILURE", "cluster", durationSeconds);
        
        try {
            // Phase 1: Analyze cluster topology
            logger.info("CHAOS: Phase 1 - Analyzing cluster topology");
            var initialNodes = redisService.getClusterNodes();
            var masterNodes = identifyMasterNodes(initialNodes);
            var slaveNodes = identifySlaveNodes(initialNodes);
            
            result.setInitialTopology(masterNodes.size(), slaveNodes.size(), initialNodes.size());
            result.addEvent(String.format("Initial topology: %d masters, %d slaves, %d total nodes", 
                masterNodes.size(), slaveNodes.size(), initialNodes.size()));
            
            // Phase 2: Single master failure test
            logger.info("CHAOS: Phase 2 - Single master failure test");
            if (!masterNodes.isEmpty()) {
                String masterNode = masterNodes.get(0).get("address").toString();
                var masterFailureResult = simulateTargetedNodeFailure(masterNode, "master", 20, true);
                result.addPhaseResult("single_master_failure", masterFailureResult);
            }
            
            Thread.sleep(3000); // Recovery time
            
            // Phase 3: Single slave failure test  
            logger.info("CHAOS: Phase 3 - Single slave failure test");
            if (!slaveNodes.isEmpty()) {
                String slaveNode = slaveNodes.get(0).get("address").toString();
                var slaveFailureResult = simulateTargetedNodeFailure(slaveNode, "slave", 15, true);
                result.addPhaseResult("single_slave_failure", slaveFailureResult);
            }
            
            Thread.sleep(3000); // Recovery time
            
            // Phase 4: Multiple node failure test (stress scenario)
            logger.info("CHAOS: Phase 4 - Multiple node failure test");
            if (masterNodes.size() >= 2) {
                var multiNodeResult = simulateMultipleNodeFailures(masterNodes.subList(0, 2), 25, true);
                result.addPhaseResult("multiple_node_failure", multiNodeResult);
            }
            
            Thread.sleep(5000); // Extended recovery time
            
            // Phase 5: Cascading failure simulation
            logger.info("CHAOS: Phase 5 - Cascading failure simulation");
            var cascadingResult = simulateCascadingNodeFailures(30);
            result.addPhaseResult("cascading_failure", cascadingResult);
            
            // Final assessment
            boolean finalHealth = redisService.isClusterHealthy();
            var finalNodes = redisService.getClusterNodes();
            
            result.setFinalHealth(finalHealth);
            result.setFinalTopology(finalNodes.size());
            result.setSuccess(finalHealth && finalNodes.size() >= initialNodes.size());
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (result.isSuccess()) {
                result.addEvent("✅ Comprehensive node failure test completed successfully");
                logger.info("CHAOS: Comprehensive node failure test completed successfully");
            } else {
                result.addEvent("❌ Comprehensive node failure test completed with issues");
                logger.warn("CHAOS: Comprehensive node failure test completed with cluster issues");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Comprehensive node failure test failed: " + e.getMessage());
            logger.error("CHAOS: Comprehensive node failure test failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Targeted node failure with detailed client resilience testing
     */
    public DetailedNodeFailureResult simulateTargetedNodeFailure(String targetAddress, String nodeType, 
                                                                 int outageSeconds, boolean testClientResilience) {
        logger.info("🎯 CHAOS: Starting targeted {} failure - {} for {}s", nodeType, targetAddress, outageSeconds);
        
        Instant startTime = Instant.now();
        DetailedNodeFailureResult result = new DetailedNodeFailureResult("TARGETED_NODE_FAILURE", 
            targetAddress, nodeType, outageSeconds);
        
        try {
            // Pre-failure state
            var beforeNodes = redisService.getClusterNodes();
            var beforeInfo = redisService.getClusterInfo();
            result.setBeforeState(beforeNodes.size(), beforeInfo.get("cluster_state"), 
                Integer.parseInt(beforeInfo.get("cluster_slots_assigned")));
            
            // Extract container name and stop it
            String containerName = extractContainerName(targetAddress);
            logger.info("CHAOS: Stopping container {} for {} node {}", containerName, nodeType, targetAddress);
            
            ProcessResult stopResult = executeCommand("docker", "stop", containerName);
            if (stopResult.exitCode != 0) {
                throw new RuntimeException("Failed to stop container: " + stopResult.stderr);
            }
            
            result.addEvent("Container " + containerName + " stopped successfully");
            
            // Test client resilience during outage
            if (testClientResilience) {
                logger.info("CHAOS: Testing client resilience during {} outage", nodeType);
                var resilienceMetrics = testClientResilienceDuringOutage(outageSeconds - 5);
                result.setResilienceMetrics(resilienceMetrics);
            }
            
            // Wait for specified outage duration
            logger.info("CHAOS: Maintaining {} outage for {} seconds", nodeType, outageSeconds);
            Thread.sleep(outageSeconds * 1000);
            
            // Restart the node
            logger.info("CHAOS: Restarting {} node {}", nodeType, targetAddress);
            ProcessResult startResult = executeCommand("docker", "start", containerName);
            if (startResult.exitCode != 0) {
                logger.error("CHAOS: Failed to restart container: {}", startResult.stderr);
                result.addEvent("Failed to restart container: " + startResult.stderr);
            } else {
                result.addEvent("Container " + containerName + " restarted successfully");
            }
            
            // Monitor recovery process
            logger.info("CHAOS: Monitoring {} recovery process", nodeType);
            var recoveryMetrics = monitorNodeRecovery(targetAddress, nodeType, 60);
            result.setRecoveryMetrics(recoveryMetrics);
            
            // Post-recovery validation
            Thread.sleep(5000);
            var afterNodes = redisService.getClusterNodes();
            var afterInfo = redisService.getClusterInfo();
            result.setAfterState(afterNodes.size(), afterInfo.get("cluster_state"),
                Integer.parseInt(afterInfo.get("cluster_slots_assigned")));
            
            boolean recovered = "ok".equals(afterInfo.get("cluster_state")) && 
                              afterNodes.size() == beforeNodes.size();
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ " + nodeType + " node failure and recovery completed successfully");
                logger.info("CHAOS: {} node failure test completed successfully", nodeType);
            } else {
                result.addEvent("❌ " + nodeType + " node failed to recover properly");
                logger.error("CHAOS: {} node failure test completed with recovery issues", nodeType);
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Targeted node failure test failed: " + e.getMessage());
            logger.error("CHAOS: Targeted node failure test failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Test multiple simultaneous node failures
     */
    public MultiNodeFailureResult simulateMultipleNodeFailures(List<Map<String, Object>> targetNodes, 
                                                               int outageSeconds, boolean testClientResilience) {
        logger.info("🔥 CHAOS: Starting multiple node failure test - {} nodes for {}s", 
            targetNodes.size(), outageSeconds);
        
        Instant startTime = Instant.now();
        MultiNodeFailureResult result = new MultiNodeFailureResult("MULTI_NODE_FAILURE", 
            "cluster", targetNodes.size(), outageSeconds);
        
        try {
            List<String> containerNames = new ArrayList<>();
            List<String> nodeAddresses = new ArrayList<>();
            
            // Stop all target nodes simultaneously
            for (Map<String, Object> node : targetNodes) {
                String address = node.get("address").toString();
                String containerName = extractContainerName(address);
                nodeAddresses.add(address);
                containerNames.add(containerName);
                
                logger.info("CHAOS: Stopping container {} for node {}", containerName, address);
                ProcessResult stopResult = executeCommand("docker", "stop", containerName);
                if (stopResult.exitCode == 0) {
                    result.addEvent("Stopped container: " + containerName);
                } else {
                    result.addEvent("Failed to stop container: " + containerName + " - " + stopResult.stderr);
                }
            }
            
            // Test cluster behavior under multi-node failure
            if (testClientResilience) {
                logger.info("CHAOS: Testing cluster resilience with {} nodes down", targetNodes.size());
                var resilienceMetrics = testClientResilienceDuringOutage(outageSeconds - 5);
                result.setResilienceMetrics(resilienceMetrics);
            }
            
            // Wait for outage duration
            Thread.sleep(outageSeconds * 1000);
            
            // Restart all nodes
            logger.info("CHAOS: Restarting all {} failed nodes", containerNames.size());
            for (String containerName : containerNames) {
                ProcessResult startResult = executeCommand("docker", "start", containerName);
                if (startResult.exitCode == 0) {
                    result.addEvent("Restarted container: " + containerName);
                } else {
                    result.addEvent("Failed to restart container: " + containerName);
                }
            }
            
            // Monitor recovery
            logger.info("CHAOS: Monitoring multi-node recovery");
            Thread.sleep(10000); // Extended recovery time for multiple nodes
            
            boolean recovered = redisService.isClusterHealthy();
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ Multi-node failure and recovery completed successfully");
                logger.info("CHAOS: Multi-node failure test completed successfully");
            } else {
                result.addEvent("❌ Multi-node recovery failed");
                logger.error("CHAOS: Multi-node failure test completed with issues");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Multi-node failure test failed: " + e.getMessage());
            logger.error("CHAOS: Multi-node failure test failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Simulate cascading node failures
     */
    public CascadingFailureResult simulateCascadingNodeFailures(int totalDurationSeconds) {
        logger.info("⚡ CHAOS: Starting cascading failure simulation - {}s total duration", totalDurationSeconds);
        
        Instant startTime = Instant.now();
        CascadingFailureResult result = new CascadingFailureResult("CASCADING_FAILURE", "cluster", totalDurationSeconds);
        
        try {
            var allNodes = redisService.getClusterNodes();
            List<String> availableContainers = allNodes.stream()
                .map(node -> extractContainerName(node.get("address").toString()))
                .collect(Collectors.toList());
            
            int failureInterval = Math.max(5, totalDurationSeconds / Math.min(3, availableContainers.size()));
            List<String> failedContainers = new ArrayList<>();
            
            // Cascade failures at intervals
            for (int i = 0; i < Math.min(3, availableContainers.size()) && 
                 Duration.between(startTime, Instant.now()).getSeconds() < totalDurationSeconds - 10; i++) {
                
                String containerName = availableContainers.get(i);
                logger.info("CHAOS: Cascading failure step {} - failing container {}", i + 1, containerName);
                
                ProcessResult stopResult = executeCommand("docker", "stop", containerName);
                if (stopResult.exitCode == 0) {
                    failedContainers.add(containerName);
                    result.addEvent("Cascading failure step " + (i + 1) + ": stopped " + containerName);
                    
                    // Test resilience after each cascade step
                    var stepMetrics = testClientResilienceDuringOutage(failureInterval - 2);
                    result.addCascadeStepMetrics(i + 1, stepMetrics);
                    
                    Thread.sleep(failureInterval * 1000);
                } else {
                    result.addEvent("Failed to stop " + containerName + " in cascade step " + (i + 1));
                }
            }
            
            // Recovery phase - restart all failed containers
            logger.info("CHAOS: Starting cascading recovery phase");
            for (String containerName : failedContainers) {
                ProcessResult startResult = executeCommand("docker", "start", containerName);
                if (startResult.exitCode == 0) {
                    result.addEvent("Recovered container: " + containerName);
                } else {
                    result.addEvent("Failed to recover container: " + containerName);
                }
                Thread.sleep(2000); // Staggered recovery
            }
            
            // Final recovery validation
            Thread.sleep(10000);
            boolean recovered = redisService.isClusterHealthy();
            result.setSuccess(recovered);
            result.setDuration(Duration.between(startTime, Instant.now()));
            
            if (recovered) {
                result.addEvent("✅ Cascading failure simulation completed with full recovery");
                logger.info("CHAOS: Cascading failure simulation completed successfully");
            } else {
                result.addEvent("❌ Cascading failure simulation completed with recovery issues");
                logger.error("CHAOS: Cascading failure simulation completed with issues");
            }
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Cascading failure simulation failed: " + e.getMessage());
            logger.error("CHAOS: Cascading failure simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    /**
     * Test client resilience during node outages with detailed retry tracking
     */
    private ClientResilienceMetrics testClientResilienceDuringOutage(int testDurationSeconds) {
        logger.info("🔍 CHAOS: Testing client resilience for {} seconds", testDurationSeconds);
        
        ClientResilienceMetrics metrics = new ClientResilienceMetrics();
        long endTime = System.currentTimeMillis() + (testDurationSeconds * 1000);
        
        ExecutorService testExecutor = Executors.newFixedThreadPool(10);
        List<Future<?>> testTasks = new ArrayList<>();
        
        // Submit continuous testing tasks
        for (int i = 0; i < 10; i++) {
            final int threadId = i;
            Future<?> task = testExecutor.submit(() -> {
                Random random = new Random();
                
                while (System.currentTimeMillis() < endTime) {
                    String testKey = "resilience_test_" + threadId + "_" + System.currentTimeMillis();
                    String testValue = "value_" + random.nextInt(10000);
                    
                    try {
                        // Test SET operation with retry tracking
                        long setStartTime = System.currentTimeMillis();
                        redisService.setWithRetry(testKey, testValue);
                        long setDuration = System.currentTimeMillis() - setStartTime;
                        
                        metrics.recordSuccessfulSet(setDuration);
                        
                        // Test GET operation with retry tracking  
                        long getStartTime = System.currentTimeMillis();
                        Object retrieved = redisService.getWithRetry(testKey);
                        long getDuration = System.currentTimeMillis() - getStartTime;
                        
                        if (testValue.equals(retrieved)) {
                            metrics.recordSuccessfulGet(getDuration);
                        } else {
                            metrics.recordDataConsistencyError();
                        }
                        
                        // Test DELETE operation
                        redisService.delete(testKey);
                        metrics.recordSuccessfulDelete();
                        
                    } catch (Exception e) {
                        metrics.recordOperationFailure(e.getMessage());
                        
                        // Log detailed retry information for first few failures
                        if (metrics.getTotalFailures() <= 5) {
                            logger.debug("CHAOS: Client resilience failure {}: {}", 
                                metrics.getTotalFailures(), e.getMessage());
                        }
                    }
                    
                    try {
                        Thread.sleep(100); // Brief pause between operations
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            testTasks.add(task);
        }
        
        // Wait for test completion
        for (Future<?> task : testTasks) {
            try {
                task.get();
            } catch (Exception e) {
                logger.warn("CHAOS: Test task failed: {}", e.getMessage());
            }
        }
        
        testExecutor.shutdown();
        try {
            if (!testExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                testExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            testExecutor.shutdownNow();
        }
        
        metrics.finalizeMetrics();
        logger.info("CHAOS: Client resilience test completed - Success rate: {:.2f}%, Avg latency: {}ms", 
            metrics.getSuccessRate(), metrics.getAverageLatency());
        
        return metrics;
    }

    /**
     * Monitor node recovery process with detailed metrics
     */
    private NodeRecoveryMetrics monitorNodeRecovery(String nodeAddress, String nodeType, int maxWaitSeconds) {
        logger.info("📊 CHAOS: Monitoring {} node {} recovery", nodeType, nodeAddress);
        
        NodeRecoveryMetrics metrics = new NodeRecoveryMetrics(nodeAddress, nodeType);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (maxWaitSeconds * 1000);
        
        boolean nodeRecovered = false;
        boolean clusterHealthy = false;
        
        while (System.currentTimeMillis() < endTime && !nodeRecovered) {
            try {
                // Check if specific node is back online
                var currentNodes = redisService.getClusterNodes();
                boolean nodeFound = currentNodes.stream()
                    .anyMatch(node -> node.get("address").toString().contains(nodeAddress.split(":")[1]));
                
                if (nodeFound) {
                    if (!nodeRecovered) {
                        long recoveryTime = System.currentTimeMillis() - startTime;
                        metrics.setNodeRecoveryTime(recoveryTime);
                        nodeRecovered = true;
                        logger.info("CHAOS: {} node {} recovered after {}ms", nodeType, nodeAddress, recoveryTime);
                    }
                }
                
                // Check overall cluster health
                boolean currentHealth = redisService.isClusterHealthy();
                if (currentHealth && !clusterHealthy) {
                    long clusterRecoveryTime = System.currentTimeMillis() - startTime;
                    metrics.setClusterRecoveryTime(clusterRecoveryTime);
                    clusterHealthy = true;
                    logger.info("CHAOS: Cluster recovered after {}ms", clusterRecoveryTime);
                }
                
                // Test basic operations during recovery
                try {
                    String testKey = "recovery_test_" + System.currentTimeMillis();
                    redisService.setWithRetry(testKey, "recovery_value");
                    Object retrieved = redisService.getWithRetry(testKey);
                    redisService.delete(testKey);
                    
                    if ("recovery_value".equals(retrieved)) {
                        metrics.recordSuccessfulOperation();
                    } else {
                        metrics.recordFailedOperation("Data consistency error");
                    }
                } catch (Exception e) {
                    metrics.recordFailedOperation(e.getMessage());
                }
                
                Thread.sleep(1000); // Check every second
                
            } catch (Exception e) {
                logger.debug("CHAOS: Recovery monitoring error: {}", e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        metrics.setFullyRecovered(nodeRecovered && clusterHealthy);
        logger.info("CHAOS: Recovery monitoring completed - Node recovered: {}, Cluster healthy: {}", 
            nodeRecovered, clusterHealthy);
        
        return metrics;
    }

    // Helper methods for node identification
    private List<Map<String, Object>> identifyMasterNodes(List<Map<String, Object>> allNodes) {
        return allNodes.stream()
            .filter(node -> {
                String flags = node.get("flags").toString();
                return flags.contains("master") && !flags.contains("fail");
            })
            .collect(Collectors.toList());
    }
    
    private List<Map<String, Object>> identifySlaveNodes(List<Map<String, Object>> allNodes) {
        return allNodes.stream()
            .filter(node -> {
                String flags = node.get("flags").toString();
                return flags.contains("slave") && !flags.contains("fail");
            })
            .collect(Collectors.toList());
    }
    
    private String extractContainerName(String nodeAddress) {
        // Extract port from address like "redis.localhost:7001"
        if (nodeAddress.contains(":700")) {
            String port = nodeAddress.substring(nodeAddress.indexOf(":700") + 1, nodeAddress.indexOf(":700") + 5);
            return "redis-" + port.substring(3); // Extract "1" from "7001"
        }
        return "redis-1"; // Default fallback
    }

    /**
     * Run comprehensive chaos scenario combining multiple chaos types
     */
    public ComprehensiveChaosResult runComprehensiveChaosScenario() {
        logger.info("🌪️  CHAOS: Starting comprehensive chaos scenario");
        
        Instant startTime = Instant.now();
        ComprehensiveChaosResult result = new ComprehensiveChaosResult("COMPREHENSIVE_CHAOS", "system");
        
        try {
            result.addEvent("Starting comprehensive chaos scenario with multiple chaos types");
            
            // Phase 1: Latency injection
            logger.info("CHAOS: Phase 1 - Latency injection");
            var latencyResult = simulateLatencyInjection(30, 200, 0.3);
            result.addPhaseResult("latency_injection", latencyResult);
            Thread.sleep(2000);
            
            // Phase 2: Memory exhaustion
            logger.info("CHAOS: Phase 2 - Memory exhaustion");
            var memoryResult = simulateMemoryExhaustion(30, 1024 * 50);
            result.addPhaseResult("memory_exhaustion", memoryResult);
            Thread.sleep(2000);
            
            // Phase 3: Network partition
            logger.info("CHAOS: Phase 3 - Network partition");
            var networkResult = simulateNetworkPartition(Duration.ofSeconds(15), 2);
            result.addPhaseResult("network_partition", networkResult);
            Thread.sleep(2000);
            
            // Phase 4: Resource saturation
            logger.info("CHAOS: Phase 4 - Resource saturation");
            var resourceResult = simulateResourceSaturation("both", 30, 70);
            result.addPhaseResult("resource_saturation", resourceResult);
            Thread.sleep(2000);
            
            // Phase 5: Data corruption
            logger.info("CHAOS: Phase 5 - Data corruption");
            var corruptionResult = simulateDataCorruption(15, 0.2);
            result.addPhaseResult("data_corruption", corruptionResult);
            
            // Final health check
            boolean finalHealth = redisService.isClusterHealthy();
            result.addEvent("Final cluster health check: " + (finalHealth ? "HEALTHY" : "UNHEALTHY"));
            
            result.setSuccess(finalHealth);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Comprehensive chaos scenario completed");
            
            logger.info("CHAOS: Comprehensive chaos scenario completed - final health: {}", finalHealth);
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Comprehensive chaos scenario failed: " + e.getMessage());
            logger.error("CHAOS: Comprehensive chaos scenario failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    // Utility method for exception handling
    private void handledException(Exception e) {
        logger.debug("CHAOS: Handled injected exception: {}", e.getMessage());
    }

    /**
     * Simulate random data corruption
     */
    public DataCorruptionResult simulateDataCorruption(int keysTargeted, double corruptionProbability) {
        logger.info("💾 CHAOS: Starting data corruption simulation - {} keys with {:.1f}% corruption probability", 
            keysTargeted, corruptionProbability * 100);
        
        Instant startTime = Instant.now();
        DataCorruptionResult result = new DataCorruptionResult("DATA_CORRUPTION", "cluster", keysTargeted, corruptionProbability);
        
        try {
            // Create test data
            logger.info("CHAOS: Creating {} test keys...", keysTargeted);
            List<String> testKeys = new ArrayList<>();
            Map<String, String> originalData = new HashMap<>();
            
            for (int i = 0; i < keysTargeted; i++) {
                String key = "corruption_test_" + i;
                String value = "original_value_" + i + "_" + UUID.randomUUID().toString();
                testKeys.add(key);
                originalData.put(key, value);
                redisService.setWithRetry(key, value);
            }
            
            result.addEvent("Created " + keysTargeted + " test keys");
            
            // Simulate data corruption by overwriting random keys based on probability
            int actualCorruptions = 0;
            logger.info("CHAOS: Corrupting random keys with {:.1f}% probability...", corruptionProbability * 100);
            
            Random random = new Random();
            Set<String> corruptedKeys = new HashSet<>();
            
            for (String key : testKeys) {
                if (random.nextDouble() < corruptionProbability) {
                    String corruptedValue = "CORRUPTED_DATA_" + System.currentTimeMillis() + "_" + actualCorruptions;
                    redisService.setWithRetry(key, corruptedValue);
                    corruptedKeys.add(key);
                    actualCorruptions++;
                }
            }
            
            result.addEvent("Actually corrupted " + actualCorruptions + " keys");
            
            // Detect data corruption
            logger.info("CHAOS: Detecting data corruption...");
            AtomicInteger detectedCorruptions = new AtomicInteger(0);
            AtomicInteger intactKeys = new AtomicInteger(0);
            AtomicInteger missingKeys = new AtomicInteger(0);
            
            for (String key : testKeys) {
                try {
                    Object retrievedValue = redisService.getWithRetry(key);
                    if (retrievedValue == null) {
                        missingKeys.incrementAndGet();
                    } else {
                        String retrievedStr = retrievedValue.toString();
                        String originalValue = originalData.get(key);
                        
                        if (!originalValue.equals(retrievedStr)) {
                            detectedCorruptions.incrementAndGet();
                            if (detectedCorruptions.get() <= 5) {
                                logger.debug("CHAOS: Detected corruption in key {}: expected {}, got {}", 
                                    key, originalValue.substring(0, Math.min(20, originalValue.length())) + "...", 
                                    retrievedStr.substring(0, Math.min(20, retrievedStr.length())) + "...");
                            }
                        } else {
                            intactKeys.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    logger.warn("CHAOS: Error checking key {}: {}", key, e.getMessage());
                }
            }
            
            result.setSuccessfulOperations(detectedCorruptions.get());
            result.setFailedOperations(missingKeys.get());
            
            result.addEvent(String.format("Data integrity check - Corrupted: %d, Intact: %d, Missing: %d", 
                detectedCorruptions.get(), intactKeys.get(), missingKeys.get()));
            
            // Cleanup
            logger.info("CHAOS: Cleaning up test data...");
            for (String key : testKeys) {
                try {
                    redisService.delete(key);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
            
            result.setSuccess(detectedCorruptions.get() > 0); // Success if we detected corruptions
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Data corruption simulation completed");
            
            logger.info("CHAOS: Data corruption simulation completed - detected {}/{} corruptions", 
                detectedCorruptions.get(), actualCorruptions);
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Data corruption simulation failed: " + e.getMessage());
            logger.error("CHAOS: Data corruption simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }
    
    public ChaosResult simulateDataCorruption(int keyCount) {
        logger.info("💾 CHAOS: Starting data corruption simulation - {} keys", keyCount);
        
        Instant startTime = Instant.now();
        ChaosResult result = new ChaosResult("DATA_CORRUPTION", "cluster");
        
        try {
            // Create test data
            logger.info("CHAOS: Creating {} test keys...", keyCount);
            List<String> testKeys = new ArrayList<>();
            Map<String, String> originalData = new HashMap<>();
            
            for (int i = 0; i < keyCount; i++) {
                String key = "corruption_test_" + i;
                String value = "original_value_" + i + "_" + UUID.randomUUID().toString();
                testKeys.add(key);
                originalData.put(key, value);
                redisService.setWithRetry(key, value);
            }
            
            result.addEvent("Created " + keyCount + " test keys");
            
            // Simulate data corruption by overwriting random keys
            int corruptionCount = keyCount / 4; // Corrupt 25% of data
            logger.info("CHAOS: Corrupting {} random keys...", corruptionCount);
            
            Random random = new Random();
            Set<String> corruptedKeys = new HashSet<>();
            
            for (int i = 0; i < corruptionCount; i++) {
                String key = testKeys.get(random.nextInt(testKeys.size()));
                String corruptedValue = "CORRUPTED_DATA_" + System.currentTimeMillis() + "_" + i;
                redisService.setWithRetry(key, corruptedValue);
                corruptedKeys.add(key);
            }
            
            result.addEvent("Corrupted " + corruptedKeys.size() + " keys");
            
            // Detect data corruption
            logger.info("CHAOS: Detecting data corruption...");
            AtomicInteger detectedCorruptions = new AtomicInteger(0);
            AtomicInteger intactKeys = new AtomicInteger(0);
            AtomicInteger missingKeys = new AtomicInteger(0);
            
            for (String key : testKeys) {
                try {
                    Object retrievedValue = redisService.getWithRetry(key);
                    if (retrievedValue == null) {
                        missingKeys.incrementAndGet();
                    } else {
                        String retrievedStr = retrievedValue.toString();
                        String originalValue = originalData.get(key);
                        
                        if (!originalValue.equals(retrievedStr)) {
                            detectedCorruptions.incrementAndGet();
                            if (detectedCorruptions.get() <= 5) {
                                logger.debug("CHAOS: Detected corruption in key {}: expected {}, got {}", 
                                    key, originalValue.substring(0, 20) + "...", 
                                    retrievedStr.substring(0, 20) + "...");
                            }
                        } else {
                            intactKeys.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    logger.warn("CHAOS: Error checking key {}: {}", key, e.getMessage());
                }
            }
            
            result.addEvent(String.format("Data integrity check - Corrupted: %d, Intact: %d, Missing: %d", 
                detectedCorruptions.get(), intactKeys.get(), missingKeys.get()));
            
            // Cleanup
            logger.info("CHAOS: Cleaning up test data...");
            for (String key : testKeys) {
                try {
                    redisService.delete(key);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
            
            result.setSuccess(detectedCorruptions.get() > 0); // Success if we detected corruptions
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("✅ Data corruption simulation completed");
            
            logger.info("CHAOS: Data corruption simulation completed - detected {}/{} corruptions", 
                detectedCorruptions.get(), corruptionCount);
            
        } catch (Exception e) {
            result.setSuccess(false);
            result.setDuration(Duration.between(startTime, Instant.now()));
            result.addEvent("Data corruption simulation failed: " + e.getMessage());
            logger.error("CHAOS: Data corruption simulation failed: {}", e.getMessage(), e);
        }
        
        return result;
    }

    // Utility methods
    private String extractNodeNumber(String nodeId) {
        // Try to extract node number from various formats
        if (nodeId.contains(":700")) {
            String port = nodeId.substring(nodeId.indexOf(":700") + 4, nodeId.indexOf(":700") + 5);
            return port;
        }
        return "1"; // Default fallback
    }

    private ProcessResult executeCommand(String... command) {
        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            Process process = pb.start();
            
            StringBuilder stdout = new StringBuilder();
            StringBuilder stderr = new StringBuilder();
            
            try (BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                 BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                
                String line;
                while ((line = stdoutReader.readLine()) != null) {
                    stdout.append(line).append("\n");
                }
                while ((line = stderrReader.readLine()) != null) {
                    stderr.append(line).append("\n");
                }
            }
            
            int exitCode = process.waitFor();
            return new ProcessResult(exitCode, stdout.toString(), stderr.toString());
            
        } catch (IOException | InterruptedException e) {
            return new ProcessResult(1, "", e.getMessage());
        }
    }

    // Inner classes
    public static class ChaosResult {
        private final String chaosType;
        private final String target;
        private final Instant startTime;
        private Duration duration;
        private boolean success;
        private Map<String, Object> beforeState;
        private Map<String, Object> afterState;
        private final List<String> events;

        public ChaosResult(String chaosType, String target) {
            this.chaosType = chaosType;
            this.target = target;
            this.startTime = Instant.now();
            this.success = true;
            this.events = new ArrayList<>();
        }

        public void addEvent(String event) {
            events.add(Instant.now() + ": " + event);
        }

        // Getters and setters
        public String getChaosType() { return chaosType; }
        public String getTarget() { return target; }
        public Instant getStartTime() { return startTime; }
        public Duration getDuration() { return duration; }
        public void setDuration(Duration duration) { this.duration = duration; }
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public Map<String, Object> getBeforeState() { return beforeState; }
        public void setBeforeState(Map<String, Object> beforeState) { this.beforeState = beforeState; }
        public Map<String, Object> getAfterState() { return afterState; }
        public void setAfterState(Map<String, Object> afterState) { this.afterState = afterState; }
        public List<String> getEvents() { return events; }
    }

    // Specialized result classes
    public static class NodeFailureResult extends ChaosResult {
        private String targetNode;
        private long durationSeconds;
        private int successfulOperations;
        private int failedOperations;
        private String errorMessage;

        public NodeFailureResult(String chaosType, String target) {
            super(chaosType, target);
            this.targetNode = target;
        }

        // Getters and setters
        public String getTargetNode() { return targetNode; }
        public long getDurationSeconds() { return durationSeconds; }
        public void setDurationSeconds(long durationSeconds) { this.durationSeconds = durationSeconds; }
        public int getSuccessfulOperations() { return successfulOperations; }
        public void setSuccessfulOperations(int successfulOperations) { this.successfulOperations = successfulOperations; }
        public int getFailedOperations() { return failedOperations; }
        public void setFailedOperations(int failedOperations) { this.failedOperations = failedOperations; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        @Override
        public void setDuration(Duration duration) {
            super.setDuration(duration);
            this.durationSeconds = duration.getSeconds();
        }
    }

    public static class MemoryExhaustionResult extends ChaosResult {
        private int largeObjectCount;
        private int objectSizeBytes;
        private int successfulOperations;
        private int failedOperations;

        public MemoryExhaustionResult(String chaosType, String target, int largeObjectCount, int objectSizeBytes) {
            super(chaosType, target);
            this.largeObjectCount = largeObjectCount;
            this.objectSizeBytes = objectSizeBytes;
        }

        // Getters and setters
        public int getLargeObjectCount() { return largeObjectCount; }
        public int getObjectSizeBytes() { return objectSizeBytes; }
        public int getSuccessfulOperations() { return successfulOperations; }
        public void setSuccessfulOperations(int successfulOperations) { this.successfulOperations = successfulOperations; }
        public int getFailedOperations() { return failedOperations; }
        public void setFailedOperations(int failedOperations) { this.failedOperations = failedOperations; }
        public String getErrorMessage() { return null; } // For compatibility
    }

    public static class NetworkPartitionResult extends ChaosResult {
        private long durationSeconds;
        private int affectedNodeCount;
        private int successfulOperations;
        private int failedOperations;

        public NetworkPartitionResult(String chaosType, String target, long durationSeconds, int affectedNodeCount) {
            super(chaosType, target);
            this.durationSeconds = durationSeconds;
            this.affectedNodeCount = affectedNodeCount;
        }

        // Getters and setters
        public long getDurationSeconds() { return durationSeconds; }
        public int getAffectedNodeCount() { return affectedNodeCount; }
        public int getSuccessfulOperations() { return successfulOperations; }
        public void setSuccessfulOperations(int successfulOperations) { this.successfulOperations = successfulOperations; }
        public int getFailedOperations() { return failedOperations; }
        public void setFailedOperations(int failedOperations) { this.failedOperations = failedOperations; }
        public String getErrorMessage() { return null; } // For compatibility
    }

    public static class DataCorruptionResult extends ChaosResult {
        private int keysTargeted;
        private double corruptionProbability;
        private int successfulOperations;
        private int failedOperations;

        public DataCorruptionResult(String chaosType, String target, int keysTargeted, double corruptionProbability) {
            super(chaosType, target);
            this.keysTargeted = keysTargeted;
            this.corruptionProbability = corruptionProbability;
        }

        // Getters and setters
        public int getKeysTargeted() { return keysTargeted; }
        public double getCorruptionProbability() { return corruptionProbability; }
        public int getSuccessfulOperations() { return successfulOperations; }
        public void setSuccessfulOperations(int successfulOperations) { this.successfulOperations = successfulOperations; }
        public int getFailedOperations() { return failedOperations; }
        public void setFailedOperations(int failedOperations) { this.failedOperations = failedOperations; }
        public String getErrorMessage() { return null; } // For compatibility
    }

    public static class LatencyInjectionResult extends ChaosResult {
        private int durationSeconds;
        private int latencyMs;
        private double probability;
        private int successfulOperations;
        private int failedOperations;

        public LatencyInjectionResult(String chaosType, String target, int durationSeconds, int latencyMs, double probability) {
            super(chaosType, target);
            this.durationSeconds = durationSeconds;
            this.latencyMs = latencyMs;
            this.probability = probability;
        }

        // Getters and setters
        public int getDurationSeconds() { return durationSeconds; }
        public int getLatencyMs() { return latencyMs; }
        public double getProbability() { return probability; }
        public int getSuccessfulOperations() { return successfulOperations; }
        public void setSuccessfulOperations(int successfulOperations) { this.successfulOperations = successfulOperations; }
        public int getFailedOperations() { return failedOperations; }
        public void setFailedOperations(int failedOperations) { this.failedOperations = failedOperations; }
    }

    public static class ExceptionInjectionResult extends ChaosResult {
        private int durationSeconds;
        private double probability;
        private String exceptionType;
        private int successfulOperations;
        private int failedOperations;

        public ExceptionInjectionResult(String chaosType, String target, int durationSeconds, double probability, String exceptionType) {
            super(chaosType, target);
            this.durationSeconds = durationSeconds;
            this.probability = probability;
            this.exceptionType = exceptionType;
        }

        // Getters and setters
        public int getDurationSeconds() { return durationSeconds; }
        public double getProbability() { return probability; }
        public String getExceptionType() { return exceptionType; }
        public int getSuccessfulOperations() { return successfulOperations; }
        public void setSuccessfulOperations(int successfulOperations) { this.successfulOperations = successfulOperations; }
        public int getFailedOperations() { return failedOperations; }
        public void setFailedOperations(int failedOperations) { this.failedOperations = failedOperations; }
    }

    public static class ApplicationCrashResult extends ChaosResult {
        private int delaySeconds;

        public ApplicationCrashResult(String chaosType, String target, int delaySeconds) {
            super(chaosType, target);
            this.delaySeconds = delaySeconds;
        }

        // Getters and setters
        public int getDelaySeconds() { return delaySeconds; }
    }

    public static class ResourceSaturationResult extends ChaosResult {
        private String resourceType;
        private int durationSeconds;
        private int intensity;

        public ResourceSaturationResult(String chaosType, String target, String resourceType, int durationSeconds, int intensity) {
            super(chaosType, target);
            this.resourceType = resourceType;
            this.durationSeconds = durationSeconds;
            this.intensity = intensity;
        }

        // Getters and setters
        public String getResourceType() { return resourceType; }
        public int getDurationSeconds() { return durationSeconds; }
        public int getIntensity() { return intensity; }
    }

    public static class DiskExhaustionResult extends ChaosResult {
        private String targetPath;
        private int targetSizeMB;

        public DiskExhaustionResult(String chaosType, String target, String targetPath, int targetSizeMB) {
            super(chaosType, target);
            this.targetPath = targetPath;
            this.targetSizeMB = targetSizeMB;
        }

        // Getters and setters
        public String getTargetPath() { return targetPath; }
        public int getTargetSizeMB() { return targetSizeMB; }
    }

    public static class ClockManipulationResult extends ChaosResult {
        private int offsetMinutes;
        private int durationSeconds;

        public ClockManipulationResult(String chaosType, String target, int offsetMinutes, int durationSeconds) {
            super(chaosType, target);
            this.offsetMinutes = offsetMinutes;
            this.durationSeconds = durationSeconds;
        }

        // Getters and setters
        public int getOffsetMinutes() { return offsetMinutes; }
        public int getDurationSeconds() { return durationSeconds; }
    }

    public static class ComprehensiveChaosResult extends ChaosResult {
        private Map<String, Object> phaseResults = new HashMap<>();

        public ComprehensiveChaosResult(String chaosType, String target) {
            super(chaosType, target);
        }

        public void addPhaseResult(String phaseName, Object result) {
            phaseResults.put(phaseName, result);
        }

        public Map<String, Object> getPhaseResults() {
            return phaseResults;
        }
    }

    // Advanced node failure result classes
    public static class ComprehensiveNodeFailureResult extends ChaosResult {
        private int durationSeconds;
        private int initialMasters;
        private int initialSlaves;
        private int initialTotalNodes;
        private boolean finalHealth;
        private int finalTotalNodes;
        private Map<String, Object> phaseResults = new HashMap<>();

        public ComprehensiveNodeFailureResult(String chaosType, String target, int durationSeconds) {
            super(chaosType, target);
            this.durationSeconds = durationSeconds;
        }

        public void setInitialTopology(int masters, int slaves, int total) {
            this.initialMasters = masters;
            this.initialSlaves = slaves;
            this.initialTotalNodes = total;
        }

        public void setFinalHealth(boolean healthy) { this.finalHealth = healthy; }
        public void setFinalTopology(int total) { this.finalTotalNodes = total; }
        public void addPhaseResult(String phase, Object result) { phaseResults.put(phase, result); }

        // Getters
        public int getDurationSeconds() { return durationSeconds; }
        public int getInitialMasters() { return initialMasters; }
        public int getInitialSlaves() { return initialSlaves; }
        public int getInitialTotalNodes() { return initialTotalNodes; }
        public boolean isFinalHealth() { return finalHealth; }
        public int getFinalTotalNodes() { return finalTotalNodes; }
        public Map<String, Object> getPhaseResults() { return phaseResults; }
    }

    public static class DetailedNodeFailureResult extends ChaosResult {
        private String nodeType;
        private int outageSeconds;
        private int beforeNodeCount;
        private String beforeClusterState;
        private int beforeSlotsAssigned;
        private int afterNodeCount;
        private String afterClusterState;
        private int afterSlotsAssigned;
        private ClientResilienceMetrics resilienceMetrics;
        private NodeRecoveryMetrics recoveryMetrics;

        public DetailedNodeFailureResult(String chaosType, String target, String nodeType, int outageSeconds) {
            super(chaosType, target);
            this.nodeType = nodeType;
            this.outageSeconds = outageSeconds;
        }

        public void setBeforeState(int nodeCount, String clusterState, int slotsAssigned) {
            this.beforeNodeCount = nodeCount;
            this.beforeClusterState = clusterState;
            this.beforeSlotsAssigned = slotsAssigned;
        }

        public void setAfterState(int nodeCount, String clusterState, int slotsAssigned) {
            this.afterNodeCount = nodeCount;
            this.afterClusterState = clusterState;
            this.afterSlotsAssigned = slotsAssigned;
        }

        public void setResilienceMetrics(ClientResilienceMetrics metrics) { this.resilienceMetrics = metrics; }
        public void setRecoveryMetrics(NodeRecoveryMetrics metrics) { this.recoveryMetrics = metrics; }

        // Getters
        public String getNodeType() { return nodeType; }
        public int getOutageSeconds() { return outageSeconds; }
        public int getBeforeNodeCount() { return beforeNodeCount; }
        public String getBeforeClusterState() { return beforeClusterState; }
        public int getBeforeSlotsAssigned() { return beforeSlotsAssigned; }
        public int getAfterNodeCount() { return afterNodeCount; }
        public String getAfterClusterState() { return afterClusterState; }
        public int getAfterSlotsAssigned() { return afterSlotsAssigned; }
        public ClientResilienceMetrics getResilienceMetrics() { return resilienceMetrics; }
        public NodeRecoveryMetrics getRecoveryMetrics() { return recoveryMetrics; }
    }

    public static class MultiNodeFailureResult extends ChaosResult {
        private int nodeCount;
        private int outageSeconds;
        private ClientResilienceMetrics resilienceMetrics;

        public MultiNodeFailureResult(String chaosType, String target, int nodeCount, int outageSeconds) {
            super(chaosType, target);
            this.nodeCount = nodeCount;
            this.outageSeconds = outageSeconds;
        }

        public void setResilienceMetrics(ClientResilienceMetrics metrics) { this.resilienceMetrics = metrics; }

        // Getters
        public int getNodeCount() { return nodeCount; }
        public int getOutageSeconds() { return outageSeconds; }
        public ClientResilienceMetrics getResilienceMetrics() { return resilienceMetrics; }
    }

    public static class CascadingFailureResult extends ChaosResult {
        private int totalDurationSeconds;
        private Map<Integer, ClientResilienceMetrics> cascadeStepMetrics = new HashMap<>();

        public CascadingFailureResult(String chaosType, String target, int totalDurationSeconds) {
            super(chaosType, target);
            this.totalDurationSeconds = totalDurationSeconds;
        }

        public void addCascadeStepMetrics(int step, ClientResilienceMetrics metrics) {
            cascadeStepMetrics.put(step, metrics);
        }

        // Getters
        public int getTotalDurationSeconds() { return totalDurationSeconds; }
        public Map<Integer, ClientResilienceMetrics> getCascadeStepMetrics() { return cascadeStepMetrics; }
    }

    // Metrics classes for detailed tracking
    public static class ClientResilienceMetrics {
        private final AtomicInteger totalSets = new AtomicInteger(0);
        private final AtomicInteger totalGets = new AtomicInteger(0);
        private final AtomicInteger totalDeletes = new AtomicInteger(0);
        private final AtomicInteger totalFailures = new AtomicInteger(0);
        private final AtomicInteger dataConsistencyErrors = new AtomicInteger(0);
        private final AtomicLong totalSetLatency = new AtomicLong(0);
        private final AtomicLong totalGetLatency = new AtomicLong(0);
        private double successRate;
        private long averageLatency;

        public void recordSuccessfulSet(long latencyMs) {
            totalSets.incrementAndGet();
            totalSetLatency.addAndGet(latencyMs);
        }

        public void recordSuccessfulGet(long latencyMs) {
            totalGets.incrementAndGet();
            totalGetLatency.addAndGet(latencyMs);
        }

        public void recordSuccessfulDelete() {
            totalDeletes.incrementAndGet();
        }

        public void recordOperationFailure(String reason) {
            totalFailures.incrementAndGet();
        }

        public void recordDataConsistencyError() {
            dataConsistencyErrors.incrementAndGet();
        }

        public void finalizeMetrics() {
            int totalOps = totalSets.get() + totalGets.get() + totalDeletes.get();
            if (totalOps > 0) {
                successRate = ((totalOps - totalFailures.get()) * 100.0) / totalOps;
                averageLatency = (totalSetLatency.get() + totalGetLatency.get()) / Math.max(1, totalSets.get() + totalGets.get());
            }
        }

        // Getters
        public int getTotalSets() { return totalSets.get(); }
        public int getTotalGets() { return totalGets.get(); }
        public int getTotalDeletes() { return totalDeletes.get(); }
        public int getTotalFailures() { return totalFailures.get(); }
        public int getDataConsistencyErrors() { return dataConsistencyErrors.get(); }
        public double getSuccessRate() { return successRate; }
        public long getAverageLatency() { return averageLatency; }
    }

    public static class NodeRecoveryMetrics {
        private final String nodeAddress;
        private final String nodeType;
        private long nodeRecoveryTime = -1;
        private long clusterRecoveryTime = -1;
        private boolean fullyRecovered = false;
        private final AtomicInteger successfulOperations = new AtomicInteger(0);
        private final AtomicInteger failedOperations = new AtomicInteger(0);

        public NodeRecoveryMetrics(String nodeAddress, String nodeType) {
            this.nodeAddress = nodeAddress;
            this.nodeType = nodeType;
        }

        public void setNodeRecoveryTime(long time) { this.nodeRecoveryTime = time; }
        public void setClusterRecoveryTime(long time) { this.clusterRecoveryTime = time; }
        public void setFullyRecovered(boolean recovered) { this.fullyRecovered = recovered; }
        public void recordSuccessfulOperation() { successfulOperations.incrementAndGet(); }
        public void recordFailedOperation(String reason) { failedOperations.incrementAndGet(); }

        // Getters
        public String getNodeAddress() { return nodeAddress; }
        public String getNodeType() { return nodeType; }
        public long getNodeRecoveryTime() { return nodeRecoveryTime; }
        public long getClusterRecoveryTime() { return clusterRecoveryTime; }
        public boolean isFullyRecovered() { return fullyRecovered; }
        public int getSuccessfulOperations() { return successfulOperations.get(); }
        public int getFailedOperations() { return failedOperations.get(); }
    }

    private static class ProcessResult {
        public final int exitCode;
        public final String stdout;
        public final String stderr;

        public ProcessResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }
    }
}