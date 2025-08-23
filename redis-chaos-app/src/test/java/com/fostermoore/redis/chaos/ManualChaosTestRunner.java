package com.fostermoore.redis.chaos;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.Scanner;

/**
 * Manual test runner for chaos testing scenarios
 * Run this class to interactively test various chaos scenarios
 */
public class ManualChaosTestRunner {
    
    private static final String BASE_URL = "http://localhost:8080/chaos";
    private static final RestTemplate restTemplate = new RestTemplate();
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("🔥 Redis Chaos Monkey Test Runner 🔥");
        System.out.println("====================================");
        System.out.println("Make sure your Redis cluster and Spring Boot app are running!");
        System.out.println("Press ENTER to continue...");
        scanner.nextLine();
        
        boolean running = true;
        while (running) {
            printMenu();
            String choice = scanner.nextLine();
            
            switch (choice) {
                case "1":
                    testClusterHealth();
                    break;
                case "2":
                    testLightPerformance();
                    break;
                case "3":
                    testHeavyPerformance();
                    break;
                case "4":
                    testStressTest();
                    break;
                case "5":
                    testFailoverScenario();
                    break;
                case "6":
                    testDataIntegrity();
                    break;
                case "7":
                    testMemoryHandling();
                    break;
                case "8":
                    testConcurrentAccess();
                    break;
                case "9":
                    runFullChaosScenario();
                    break;
                case "10":
                    cleanup();
                    break;
                case "0":
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
            
            if (running) {
                System.out.println("\nPress ENTER to continue...");
                scanner.nextLine();
            }
        }
        
        scanner.close();
        System.out.println("Thanks for using Redis Chaos Monkey Test Runner! 🐵");
    }
    
    private static void printMenu() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("Choose a chaos test scenario:");
        System.out.println("1. Cluster Health Check");
        System.out.println("2. Light Performance Test (250 ops, 5 threads)");
        System.out.println("3. Heavy Performance Test (2000 ops, 20 threads)");
        System.out.println("4. Stress Test (1000 ops, 15 concurrent users)");
        System.out.println("5. Failover Scenario (300 keys, 5 retry attempts)");
        System.out.println("6. Data Integrity Test (500 records)");
        System.out.println("7. Memory Handling Test (50 large objects)");
        System.out.println("8. Concurrent Access Test (20 threads, 50 ops each)");
        System.out.println("9. Full Chaos Scenario (runs multiple tests)");
        System.out.println("10. Cleanup Test Data");
        System.out.println("0. Exit");
        System.out.print("Enter your choice: ");
    }
    
    private static void testClusterHealth() {
        System.out.println("\n🏥 Running Cluster Health Check...");
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(BASE_URL + "/health", Map.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Cluster Health: " + body.get("clusterState"));
                System.out.println("📊 Nodes: " + body.get("nodes"));
                System.out.println("✅ Health check completed successfully!");
            } else {
                System.out.println("❌ Health check failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during health check: " + e.getMessage());
        }
    }
    
    private static void testLightPerformance() {
        System.out.println("\n🚀 Running Light Performance Test...");
        runPerformanceTest(250, 5, false);
    }
    
    private static void testHeavyPerformance() {
        System.out.println("\n🚀 Running Heavy Performance Test...");
        runPerformanceTest(2000, 20, true);
    }
    
    private static void runPerformanceTest(int operations, int threads, boolean async) {
        try {
            String url = BASE_URL + "/performance-test?operations=" + operations + 
                        "&threads=" + threads + "&async=" + async;
            
            System.out.println("🔄 Testing " + operations + " operations with " + threads + " threads...");
            long startTime = System.currentTimeMillis();
            
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            
            long endTime = System.currentTimeMillis();
            
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Test completed in " + (endTime - startTime) + "ms");
                System.out.println("📊 Results: " + body);
            } else {
                System.out.println("❌ Test failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during performance test: " + e.getMessage());
        }
    }
    
    private static void testStressTest() {
        System.out.println("\n⚡ Running Stress Test...");
        try {
            String url = BASE_URL + "/stress-test?operations=1000&concurrentUsers=15";
            
            System.out.println("🔄 Stress testing with 15 concurrent users...");
            long startTime = System.currentTimeMillis();
            
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            
            long endTime = System.currentTimeMillis();
            
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Stress test completed in " + (endTime - startTime) + "ms");
                System.out.println("📊 Results: " + body);
            } else {
                System.out.println("❌ Stress test failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during stress test: " + e.getMessage());
        }
    }
    
    private static void testFailoverScenario() {
        System.out.println("\n🔄 Running Failover Scenario Test...");
        try {
            String url = BASE_URL + "/failover-test?keyCount=300&retryAttempts=5";
            
            System.out.println("🔄 Testing failover with 300 keys and 5 retry attempts...");
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Failover test completed successfully!");
                System.out.println("📊 Results: " + body);
            } else {
                System.out.println("❌ Failover test failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during failover test: " + e.getMessage());
        }
    }
    
    private static void testDataIntegrity() {
        System.out.println("\n🔍 Running Data Integrity Test...");
        try {
            String url = BASE_URL + "/data-integrity-test?recordCount=500";
            
            System.out.println("🔄 Testing data integrity with 500 records...");
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Data integrity test completed!");
                System.out.println("📊 Results: " + body);
            } else {
                System.out.println("❌ Data integrity test failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during data integrity test: " + e.getMessage());
        }
    }
    
    private static void testMemoryHandling() {
        System.out.println("\n💾 Running Memory Handling Test...");
        try {
            String url = BASE_URL + "/memory-test?largeObjectCount=50&objectSizeBytes=20480";
            
            System.out.println("🔄 Testing memory handling with 50 large objects (20KB each)...");
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Memory handling test completed!");
                System.out.println("📊 Results: " + body);
            } else {
                System.out.println("❌ Memory handling test failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during memory test: " + e.getMessage());
        }
    }
    
    private static void testConcurrentAccess() {
        System.out.println("\n🔀 Running Concurrent Access Test...");
        try {
            String url = BASE_URL + "/concurrent-access-test?threadCount=20&operationsPerThread=50";
            
            System.out.println("🔄 Testing concurrent access with 20 threads, 50 operations each...");
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                Map<String, Object> body = response.getBody();
                System.out.println("✅ Concurrent access test completed!");
                System.out.println("📊 Results: " + body);
            } else {
                System.out.println("❌ Concurrent access test failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during concurrent access test: " + e.getMessage());
        }
    }
    
    private static void runFullChaosScenario() {
        System.out.println("\n🔥 Running Full Chaos Scenario...");
        System.out.println("This will run multiple tests in sequence. It may take several minutes...");
        
        // Run a comprehensive chaos scenario
        testClusterHealth();
        System.out.println("\n⏳ Waiting 2 seconds...");
        sleep(2000);
        
        runPerformanceTest(500, 10, true);
        System.out.println("\n⏳ Waiting 2 seconds...");
        sleep(2000);
        
        testStressTest();
        System.out.println("\n⏳ Waiting 2 seconds...");
        sleep(2000);
        
        testFailoverScenario();
        System.out.println("\n⏳ Waiting 2 seconds...");
        sleep(2000);
        
        testDataIntegrity();
        System.out.println("\n⏳ Waiting 2 seconds...");
        sleep(2000);
        
        testMemoryHandling();
        System.out.println("\n⏳ Waiting 2 seconds...");
        sleep(2000);
        
        testConcurrentAccess();
        
        System.out.println("\n🎉 Full Chaos Scenario completed!");
        testClusterHealth(); // Final health check
    }
    
    private static void cleanup() {
        System.out.println("\n🧹 Cleaning up test data...");
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                BASE_URL + "/cleanup", 
                org.springframework.http.HttpMethod.DELETE, 
                null, 
                String.class
            );
            
            if (response.getStatusCode().is2xxSuccessful()) {
                System.out.println("✅ Cleanup completed successfully!");
                System.out.println("📊 Response: " + response.getBody());
            } else {
                System.out.println("❌ Cleanup failed with status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            System.out.println("❌ Error during cleanup: " + e.getMessage());
        }
    }
    
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}