package com.fostermoore.redis.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fostermoore.redis.chaos.service.ChaosOrchestrator;
import com.fostermoore.redis.chaos.service.RedisClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Profile;

import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

/**
 * Manual Chaos Test Runner - Interactive tool for running chaos engineering tests
 * 
 * This class provides a command-line interface for manually executing various
 * chaos engineering scenarios to test Redis cluster resilience.
 * 
 * Usage: Run with profile 'manual-chaos'
 * mvn spring-boot:run -Dspring-boot.run.profiles=manual-chaos
 */
@SpringBootApplication
@Profile("manual-chaos")
public class ManualChaosTestRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ManualChaosTestRunner.class);
    
    @Autowired
    private ChaosOrchestrator chaosOrchestrator;
    
    @Autowired
    private RedisClusterService redisService;
    
    private final ObjectMapper objectMapper;
    private final Scanner scanner;
    
    public ManualChaosTestRunner() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.scanner = new Scanner(System.in);
    }

    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", "manual-chaos");
        SpringApplication.run(ManualChaosTestRunner.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("🎯 Manual Chaos Test Runner - Starting Interactive Mode");
        logger.info("====================================================");
        
        // Initial health check
        if (!performInitialHealthCheck()) {
            logger.error("❌ Initial health check failed. Please ensure Redis cluster is running.");
            return;
        }
        
        showWelcomeMessage();
        
        boolean running = true;
        while (running) {
            showMainMenu();
            String choice = getUserInput("\nEnter your choice (1-9, q to quit): ");
            
            switch (choice.toLowerCase()) {
                case "1":
                    runLatencyInjectionTest();
                    break;
                case "2":
                    runExceptionInjectionTest();
                    break;
                case "3":
                    runApplicationCrashTest();
                    break;
                case "4":
                    runResourceSaturationTest();
                    break;
                case "5":
                    runDiskExhaustionTest();
                    break;
                case "6":
                    runClockManipulationTest();
                    break;
                case "7":
                    runNodeFailureTest();
                    break;
                case "8":
                    runComprehensiveChaosScenario();
                    break;
                case "9":
                    showClusterStatus();
                    break;
                case "q":
                case "quit":
                case "exit":
                    running = false;
                    break;
                default:
                    logger.warn("Invalid choice: {}. Please try again.", choice);
            }
            
            if (running) {
                getUserInput("\nPress Enter to continue...");
            }
        }
        
        logger.info("👋 Manual Chaos Test Runner - Exiting");
    }
    
    private boolean performInitialHealthCheck() {
        try {
            logger.info("🔍 Performing initial health check...");
            boolean healthy = redisService.isClusterHealthy();
            if (healthy) {
                var clusterInfo = redisService.getClusterInfo();
                var nodes = redisService.getClusterNodes();
                logger.info("✅ Cluster is healthy - {} nodes, state: {}", 
                    nodes.size(), clusterInfo.get("cluster_state"));
                return true;
            } else {
                logger.error("❌ Cluster is not healthy");
                return false;
            }
        } catch (Exception e) {
            logger.error("❌ Health check failed: {}", e.getMessage(), e);
            return false;
        }
    }
    
    private void showWelcomeMessage() {
        logger.info("\n🚀 Welcome to the Manual Chaos Test Runner!");
        logger.info("This tool allows you to manually execute chaos engineering tests");
        logger.info("against your Redis cluster to validate resilience and fault tolerance.");
        logger.info("\n⚠️  WARNING: These tests may cause temporary service disruption.");
        logger.info("Only run in development/testing environments!");
    }
    
    private void showMainMenu() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🎯 MANUAL CHAOS TEST MENU");
        System.out.println("=".repeat(60));
        System.out.println("1. 🐌 Latency Injection Test");
        System.out.println("2. 💥 Exception Injection Test");
        System.out.println("3. 💀 Application Crash Test");
        System.out.println("4. 🔥 Resource Saturation Test (CPU/Memory)");
        System.out.println("5. 💾 Disk Space Exhaustion Test");
        System.out.println("6. ⏰ Clock Manipulation Test");
        System.out.println("7. 🔌 Node Failure Test");
        System.out.println("8. 🌪️  Comprehensive Chaos Scenario");
        System.out.println("9. 📊 Show Cluster Status");
        System.out.println("q. 👋 Quit");
        System.out.println("=".repeat(60));
    }
    
    private String getUserInput(String prompt) {
        System.out.print(prompt);
        return scanner.nextLine().trim();
    }
    
    private void runLatencyInjectionTest() {
        logger.info("🐌 MANUAL CHAOS: Starting Latency Injection Test");
        
        String durationStr = getUserInput("Enter latency duration in seconds (default: 30): ");
        int duration = parseIntWithDefault(durationStr, 30);
        
        String latencyStr = getUserInput("Enter latency amount in milliseconds (default: 500): ");
        int latencyMs = parseIntWithDefault(latencyStr, 500);
        
        String probabilityStr = getUserInput("Enter injection probability 0.0-1.0 (default: 0.3): ");
        double probability = parseDoubleWithDefault(probabilityStr, 0.3);
        
        try {
            var result = chaosOrchestrator.simulateLatencyInjection(duration, latencyMs, probability);
            logChaosResult("Latency Injection", result);
        } catch (Exception e) {
            logger.error("❌ Latency injection test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runExceptionInjectionTest() {
        logger.info("💥 MANUAL CHAOS: Starting Exception Injection Test");
        
        String durationStr = getUserInput("Enter test duration in seconds (default: 30): ");
        int duration = parseIntWithDefault(durationStr, 30);
        
        String probabilityStr = getUserInput("Enter exception probability 0.0-1.0 (default: 0.2): ");
        double probability = parseDoubleWithDefault(probabilityStr, 0.2);
        
        String exceptionType = getUserInput("Enter exception type (RuntimeException/IOException/default): ");
        if (exceptionType.isEmpty()) {
            exceptionType = "RuntimeException";
        }
        
        try {
            var result = chaosOrchestrator.simulateExceptionInjection(duration, probability, exceptionType);
            logChaosResult("Exception Injection", result);
        } catch (Exception e) {
            logger.error("❌ Exception injection test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runApplicationCrashTest() {
        logger.info("💀 MANUAL CHAOS: Starting Application Crash Test");
        
        String confirm = getUserInput("⚠️  This will attempt to crash the application! Continue? (yes/no): ");
        if (!confirm.equalsIgnoreCase("yes")) {
            logger.info("Application crash test cancelled by user");
            return;
        }
        
        String delayStr = getUserInput("Enter delay before crash in seconds (default: 5): ");
        int delay = parseIntWithDefault(delayStr, 5);
        
        try {
            var result = chaosOrchestrator.simulateApplicationCrash(delay);
            logChaosResult("Application Crash", result);
        } catch (Exception e) {
            logger.error("❌ Application crash test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runResourceSaturationTest() {
        logger.info("🔥 MANUAL CHAOS: Starting Resource Saturation Test");
        
        String typeChoice = getUserInput("Select resource type (1=CPU, 2=Memory, 3=Both, default=3): ");
        String resourceType = "both";
        switch (typeChoice) {
            case "1": resourceType = "cpu"; break;
            case "2": resourceType = "memory"; break;
            default: resourceType = "both"; break;
        }
        
        String durationStr = getUserInput("Enter test duration in seconds (default: 60): ");
        int duration = parseIntWithDefault(durationStr, 60);
        
        String intensityStr = getUserInput("Enter intensity percentage 1-100 (default: 80): ");
        int intensity = parseIntWithDefault(intensityStr, 80);
        
        try {
            var result = chaosOrchestrator.simulateResourceSaturation(resourceType, duration, intensity);
            logChaosResult("Resource Saturation", result);
        } catch (Exception e) {
            logger.error("❌ Resource saturation test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runDiskExhaustionTest() {
        logger.info("💾 MANUAL CHAOS: Starting Disk Space Exhaustion Test");
        
        String targetSizeStr = getUserInput("Enter target fill size in MB (default: 100): ");
        int targetSizeMB = parseIntWithDefault(targetSizeStr, 100);
        
        String pathInput = getUserInput("Enter target path (default: /tmp): ");
        String targetPath = pathInput.isEmpty() ? "/tmp" : pathInput;
        
        try {
            var result = chaosOrchestrator.simulateDiskExhaustion(targetPath, targetSizeMB);
            logChaosResult("Disk Exhaustion", result);
        } catch (Exception e) {
            logger.error("❌ Disk exhaustion test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runClockManipulationTest() {
        logger.info("⏰ MANUAL CHAOS: Starting Clock Manipulation Test");
        
        String offsetStr = getUserInput("Enter time offset in minutes (+/- values, default: +60): ");
        int offsetMinutes = parseIntWithDefault(offsetStr, 60);
        
        String durationStr = getUserInput("Enter test duration in seconds (default: 30): ");
        int duration = parseIntWithDefault(durationStr, 30);
        
        try {
            var result = chaosOrchestrator.simulateClockManipulation(offsetMinutes, duration);
            logChaosResult("Clock Manipulation", result);
        } catch (Exception e) {
            logger.error("❌ Clock manipulation test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runNodeFailureTest() {
        logger.info("🔌 MANUAL CHAOS: Starting Node Failure Test");
        
        String nodeId = getUserInput("Enter node to target (default: redis-1): ");
        if (nodeId.isEmpty()) {
            nodeId = "redis-1";
        }
        
        String durationStr = getUserInput("Enter outage duration in seconds (default: 10): ");
        int duration = parseIntWithDefault(durationStr, 10);
        
        try {
            var result = chaosOrchestrator.simulateNodeFailure(nodeId, Duration.ofSeconds(duration));
            logChaosResult("Node Failure", result);
        } catch (Exception e) {
            logger.error("❌ Node failure test failed: {}", e.getMessage(), e);
        }
    }
    
    private void runComprehensiveChaosScenario() {
        logger.info("🌪️  MANUAL CHAOS: Starting Comprehensive Chaos Scenario");
        
        String confirm = getUserInput("⚠️  This will run multiple chaos tests sequentially! Continue? (yes/no): ");
        if (!confirm.equalsIgnoreCase("yes")) {
            logger.info("Comprehensive chaos scenario cancelled by user");
            return;
        }
        
        try {
            var result = chaosOrchestrator.runComprehensiveChaosScenario();
            logChaosResult("Comprehensive Chaos Scenario", result);
        } catch (Exception e) {
            logger.error("❌ Comprehensive chaos scenario failed: {}", e.getMessage(), e);
        }
    }
    
    private void showClusterStatus() {
        logger.info("📊 MANUAL CHAOS: Showing Current Cluster Status");
        
        try {
            boolean healthy = redisService.isClusterHealthy();
            var clusterInfo = redisService.getClusterInfo();
            var nodes = redisService.getClusterNodes();
            
            System.out.println("\n" + "=".repeat(50));
            System.out.println("📊 REDIS CLUSTER STATUS");
            System.out.println("=".repeat(50));
            System.out.println("Overall Health: " + (healthy ? "✅ HEALTHY" : "❌ UNHEALTHY"));
            System.out.println("Cluster State: " + clusterInfo.get("cluster_state"));
            System.out.println("Known Nodes: " + clusterInfo.get("cluster_known_nodes"));
            System.out.println("Slots Assigned: " + clusterInfo.get("cluster_slots_assigned"));
            System.out.println("Active Nodes: " + nodes.size());
            
            System.out.println("\nNode Details:");
            for (int i = 0; i < nodes.size(); i++) {
                var node = nodes.get(i);
                System.out.printf("  Node %d: %s [%s] - %s%n", 
                    i + 1, node.get("address"), node.get("flags"), node.get("link_state"));
            }
            System.out.println("=".repeat(50));
            
        } catch (Exception e) {
            logger.error("❌ Failed to retrieve cluster status: {}", e.getMessage(), e);
        }
    }
    
    private void logChaosResult(String testName, Object result) {
        try {
            logger.info("📋 {} Test Results:", testName);
            logger.info("=".repeat(60));
            
            String jsonResult = objectMapper.writeValueAsString(result);
            System.out.println(jsonResult);
            
            logger.info("=".repeat(60));
        } catch (Exception e) {
            logger.error("Failed to serialize chaos result: {}", e.getMessage());
            logger.info("Raw result: {}", result.toString());
        }
    }
    
    private int parseIntWithDefault(String value, int defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid integer value '{}', using default: {}", value, defaultValue);
            return defaultValue;
        }
    }
    
    private double parseDoubleWithDefault(String value, double defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid double value '{}', using default: {}", value, defaultValue);
            return defaultValue;
        }
    }
}