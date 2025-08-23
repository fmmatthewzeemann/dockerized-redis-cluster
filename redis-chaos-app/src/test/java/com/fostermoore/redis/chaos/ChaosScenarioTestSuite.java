package com.fostermoore.redis.chaos;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles("integration")
@DisplayName("Redis Chaos Engineering Test Scenarios")
public class ChaosScenarioTestSuite {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private String getBaseUrl() {
        return "http://localhost:" + port + "/chaos";
    }

    @Test
    @Order(1)
    @DisplayName("Scenario 1: Basic Cluster Health Validation")
    void testBasicClusterHealth() {
        ResponseEntity<Map> response = restTemplate.getForEntity(getBaseUrl() + "/health", Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).containsKey("clusterState");
        assertThat(response.getBody()).containsKey("nodes");
        
        System.out.println("✓ Cluster Health Test Passed");
    }

    @Test
    @Order(2)
    @DisplayName("Scenario 2: Light Load Performance Test")
    void testLightLoadPerformance() {
        String url = getBaseUrl() + "/performance-test?operations=250&threads=5&async=false";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("totalOperations")).isEqualTo(250);
        assertThat(response.getBody().get("threadCount")).isEqualTo(5);
        
        System.out.println("✓ Light Load Performance Test Passed");
    }

    @Test
    @Order(3)
    @DisplayName("Scenario 3: Heavy Load Performance Test")
    void testHeavyLoadPerformance() {
        String url = getBaseUrl() + "/performance-test?operations=2000&threads=20&async=true";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("totalOperations")).isEqualTo(2000);
        assertThat(response.getBody().get("threadCount")).isEqualTo(20);
        
        System.out.println("✓ Heavy Load Performance Test Passed");
    }

    @Test
    @Order(4)
    @DisplayName("Scenario 4: Stress Test with Concurrent Users")
    void testConcurrentUserStress() {
        String url = getBaseUrl() + "/stress-test?operations=1500&concurrentUsers=25";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("totalOperations")).isEqualTo(1500);
        assertThat(response.getBody().get("concurrentUsers")).isEqualTo(25);
        
        System.out.println("✓ Concurrent User Stress Test Passed");
    }

    @Test
    @Order(5)
    @DisplayName("Scenario 5: Failover and Retry Mechanism Test")
    void testFailoverMechanism() {
        String url = getBaseUrl() + "/failover-test?keyCount=300&retryAttempts=5";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("keyCount")).isEqualTo(300);
        assertThat(response.getBody().get("retryAttempts")).isEqualTo(5);
        
        System.out.println("✓ Failover Mechanism Test Passed");
    }

    @Test
    @Order(6)
    @DisplayName("Scenario 6: Data Integrity Under Load")
    void testDataIntegrityUnderLoad() {
        String url = getBaseUrl() + "/data-integrity-test?recordCount=500";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("recordCount")).isEqualTo(500);
        assertThat(response.getBody()).containsKey("dataIntegrityChecks");
        
        System.out.println("✓ Data Integrity Test Passed");
    }

    @Test
    @Order(7)
    @DisplayName("Scenario 7: Large Object Memory Test")
    void testLargeObjectMemoryHandling() {
        String url = getBaseUrl() + "/memory-test?largeObjectCount=50&objectSizeBytes=20480";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("largeObjectCount")).isEqualTo(50);
        assertThat(response.getBody().get("objectSizeBytes")).isEqualTo(20480);
        
        System.out.println("✓ Large Object Memory Test Passed");
    }

    @Test
    @Order(8)
    @DisplayName("Scenario 8: High Concurrency Access Pattern")
    void testHighConcurrencyAccess() {
        String url = getBaseUrl() + "/concurrent-access-test?threadCount=30&operationsPerThread=100";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("threadCount")).isEqualTo(30);
        assertThat(response.getBody().get("operationsPerThread")).isEqualTo(100);
        
        System.out.println("✓ High Concurrency Access Test Passed");
    }

    @Test
    @Order(9)
    @DisplayName("Scenario 9: Mixed Workload Chaos Test")
    void testMixedWorkloadChaos() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        
        // Run multiple test types concurrently
        CompletableFuture<ResponseEntity<Map>> performance = CompletableFuture.supplyAsync(() ->
            restTemplate.postForEntity(getBaseUrl() + "/performance-test?operations=500&threads=10&async=true", null, Map.class), executor);
        
        CompletableFuture<ResponseEntity<Map>> stress = CompletableFuture.supplyAsync(() ->
            restTemplate.postForEntity(getBaseUrl() + "/stress-test?operations=300&concurrentUsers=8", null, Map.class), executor);
        
        CompletableFuture<ResponseEntity<Map>> failover = CompletableFuture.supplyAsync(() ->
            restTemplate.postForEntity(getBaseUrl() + "/failover-test?keyCount=150&retryAttempts=3", null, Map.class), executor);
        
        CompletableFuture<ResponseEntity<Map>> integrity = CompletableFuture.supplyAsync(() ->
            restTemplate.postForEntity(getBaseUrl() + "/data-integrity-test?recordCount=200", null, Map.class), executor);
        
        // Wait for all tests to complete
        CompletableFuture.allOf(performance, stress, failover, integrity).get(120, TimeUnit.SECONDS);
        
        // Verify all tests passed
        assertThat(performance.get().getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(stress.get().getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(failover.get().getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(integrity.get().getStatusCode().is2xxSuccessful()).isTrue();
        
        executor.shutdown();
        System.out.println("✓ Mixed Workload Chaos Test Passed");
    }

    @Test
    @Order(10)
    @DisplayName("Scenario 10: Extreme Load Test")
    void testExtremeLoad() {
        // This test pushes the system to its limits
        String url = getBaseUrl() + "/performance-test?operations=5000&threads=50&async=true";
        ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody().get("totalOperations")).isEqualTo(5000);
        assertThat(response.getBody().get("threadCount")).isEqualTo(50);
        
        System.out.println("✓ Extreme Load Test Passed");
    }

    @Test
    @Order(11)
    @DisplayName("Scenario 11: Recovery and Cleanup Test")
    void testRecoveryAndCleanup() {
        // Verify cluster is still healthy after all tests
        ResponseEntity<Map> healthResponse = restTemplate.getForEntity(getBaseUrl() + "/health", Map.class);
        assertThat(healthResponse.getStatusCode().is2xxSuccessful()).isTrue();
        
        // Clean up all test data
        ResponseEntity<String> cleanupResponse = restTemplate.exchange(
            getBaseUrl() + "/cleanup", 
            org.springframework.http.HttpMethod.DELETE, 
            null, 
            String.class
        );
        assertThat(cleanupResponse.getStatusCode().is2xxSuccessful()).isTrue();
        
        System.out.println("✓ Recovery and Cleanup Test Passed");
    }

    @Test
    @Order(12)
    @DisplayName("Scenario 12: Post-Chaos Health Verification")
    void testPostChaosHealth() {
        // Final health check to ensure system is stable after all chaos tests
        ResponseEntity<Map> response = restTemplate.getForEntity(getBaseUrl() + "/health", Map.class);
        
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).containsKey("clusterState");
        
        System.out.println("✓ Post-Chaos Health Verification Passed");
        System.out.println("🎉 All Chaos Engineering Test Scenarios Completed Successfully!");
    }
}