package com.fostermoore.redis.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.time.Instant;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureWebMvc
@ActiveProfiles("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ChaosTestControllerIT {

    private static final Logger logger = LoggerFactory.getLogger(ChaosTestControllerIT.class);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        logger.info("📋 SETUP: Preparing for test: {}", testInfo.getDisplayName());
        
        Instant startTime = Instant.now();
        
        try {
            // Verify cluster health before each test
            logger.info("SETUP: Checking cluster health...");
            MvcResult healthResult = mockMvc.perform(get("/chaos/health"))
                    .andExpect(status().isOk())
                    .andReturn();
            
            String healthResponse = healthResult.getResponse().getContentAsString();
            logger.debug("SETUP: Health check response: {}", healthResponse);
            
            // Clean up before each test
            logger.info("SETUP: Performing pre-test cleanup...");
            MvcResult cleanupResult = mockMvc.perform(delete("/chaos/cleanup"))
                    .andExpect(status().isOk())
                    .andReturn();
            
            Duration setupTime = Duration.between(startTime, Instant.now());
            logger.info("SETUP: ✅ Setup completed in {}ms for test: {}", 
                setupTime.toMillis(), testInfo.getDisplayName());
                
        } catch (Exception e) {
            Duration setupTime = Duration.between(startTime, Instant.now());
            logger.error("SETUP: ❌ Setup FAILED after {}ms for test: {} - Error: {}", 
                setupTime.toMillis(), testInfo.getDisplayName(), e.getMessage());
            throw e;
        }
    }

    @Test
    @Order(1)
    @DisplayName("Cluster Health Check API Test")
    void testClusterHealth() throws Exception {
        logger.info("🏥 API-TEST: Cluster Health Check - STARTING");
        
        Instant startTime = Instant.now();
        
        try {
            MvcResult result = mockMvc.perform(get("/chaos/health"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.healthy", notNullValue()))
                    .andExpect(jsonPath("$.clusterInfo", notNullValue()))
                    .andExpect(jsonPath("$.nodes", notNullValue()))
                    .andExpect(jsonPath("$.nodeCount", notNullValue()))
                    .andReturn();
            
            Duration testTime = Duration.between(startTime, Instant.now());
            String responseContent = result.getResponse().getContentAsString();
            
            logger.info("API-TEST: Health check completed in {}ms", testTime.toMillis());
            logger.info("API-TEST: Response status: {}", result.getResponse().getStatus());
            logger.debug("API-TEST: Response body: {}", responseContent);
            
            // Parse and log key metrics
            if (responseContent.contains("\"healthy\":true")) {
                logger.info("API-TEST: ✅ Cluster is HEALTHY");
            } else {
                logger.warn("API-TEST: ⚠️  Cluster health status unclear");
            }
            
            logger.info("✅ API-TEST: Cluster Health Check - COMPLETED");
            
        } catch (Exception e) {
            Duration testTime = Duration.between(startTime, Instant.now());
            logger.error("❌ API-TEST: Cluster Health Check FAILED after {}ms - Error: {}", 
                testTime.toMillis(), e.getMessage());
            throw e;
        }
    }

    @Test
    void testBasicPerformanceTest() throws Exception {
        mockMvc.perform(post("/chaos/performance-test")
                .param("operations", "100")
                .param("threads", "2")
                .param("async", "false"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalOperations", is(100)))
                .andExpect(jsonPath("$.threadCount", is(2)))
                .andExpect(jsonPath("$.successfulOperations", greaterThanOrEqualTo(0)));
    }

    @Test
    void testLargePerformanceTest() throws Exception {
        mockMvc.perform(post("/chaos/performance-test")
                .param("operations", "1000")
                .param("threads", "10")
                .param("async", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalOperations", is(1000)))
                .andExpect(jsonPath("$.threadCount", is(10)));
    }

    @Test
    void testStressTest() throws Exception {
        mockMvc.perform(post("/chaos/stress-test")
                .param("operations", "500")
                .param("concurrentUsers", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalOperations", is(500)))
                .andExpect(jsonPath("$.concurrentUsers", is(5)))
                .andExpect(jsonPath("$.results", notNullValue()));
    }

    @Test
    void testHighConcurrencyStressTest() throws Exception {
        mockMvc.perform(post("/chaos/stress-test")
                .param("operations", "2000")
                .param("concurrentUsers", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalOperations", is(2000)))
                .andExpect(jsonPath("$.concurrentUsers", is(20)));
    }

    @Test
    void testFailoverTest() throws Exception {
        mockMvc.perform(post("/chaos/failover-test")
                .param("keyCount", "50")
                .param("retryAttempts", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.keyCount", is(50)))
                .andExpect(jsonPath("$.retryAttempts", is(3)))
                .andExpect(jsonPath("$.successfulOperations", greaterThanOrEqualTo(0)));
    }

    @Test
    void testLargeFailoverTest() throws Exception {
        mockMvc.perform(post("/chaos/failover-test")
                .param("keyCount", "200")
                .param("retryAttempts", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.keyCount", is(200)))
                .andExpect(jsonPath("$.retryAttempts", is(5)));
    }

    @Test
    void testDataIntegrityTest() throws Exception {
        mockMvc.perform(post("/chaos/data-integrity-test")
                .param("recordCount", "100"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recordCount", is(100)))
                .andExpect(jsonPath("$.dataIntegrityChecks", notNullValue()));
    }

    @Test
    void testLargeDataIntegrityTest() throws Exception {
        mockMvc.perform(post("/chaos/data-integrity-test")
                .param("recordCount", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recordCount", is(1000)));
    }

    @Test
    void testMemoryTest() throws Exception {
        mockMvc.perform(post("/chaos/memory-test")
                .param("largeObjectCount", "10")
                .param("objectSizeBytes", "1024"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.largeObjectCount", is(10)))
                .andExpect(jsonPath("$.objectSizeBytes", is(1024)))
                .andExpect(jsonPath("$.results", notNullValue()));
    }

    @Test
    void testLargeMemoryTest() throws Exception {
        mockMvc.perform(post("/chaos/memory-test")
                .param("largeObjectCount", "50")
                .param("objectSizeBytes", "10240"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.largeObjectCount", is(50)))
                .andExpect(jsonPath("$.objectSizeBytes", is(10240)));
    }

    @Test
    void testConcurrentAccessTest() throws Exception {
        mockMvc.perform(post("/chaos/concurrent-access-test")
                .param("threadCount", "5")
                .param("operationsPerThread", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.threadCount", is(5)))
                .andExpect(jsonPath("$.operationsPerThread", is(20)))
                .andExpect(jsonPath("$.results", notNullValue()));
    }

    @Test
    void testHighConcurrentAccessTest() throws Exception {
        mockMvc.perform(post("/chaos/concurrent-access-test")
                .param("threadCount", "20")
                .param("operationsPerThread", "100"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.threadCount", is(20)))
                .andExpect(jsonPath("$.operationsPerThread", is(100)));
    }

    @Test
    void testNetworkPartitionSimulation() throws Exception {
        mockMvc.perform(post("/chaos/network-partition-test")
                .param("duration", "5")
                .param("affectedNodeCount", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duration", is(5)))
                .andExpect(jsonPath("$.affectedNodeCount", is(2)))
                .andExpect(jsonPath("$.partitionResults", notNullValue()));
    }

    @Test
    void testConnectionPoolExhaustion() throws Exception {
        mockMvc.perform(post("/chaos/connection-pool-test")
                .param("connectionCount", "30")
                .param("holdTimeSeconds", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.connectionCount", is(30)))
                .andExpect(jsonPath("$.holdTimeSeconds", is(2)))
                .andExpect(jsonPath("$.poolExhaustionTest", notNullValue()));
    }

    @Test
    void testCleanup() throws Exception {
        // First create some test data
        mockMvc.perform(post("/chaos/performance-test")
                .param("operations", "50")
                .param("threads", "1")
                .param("async", "false"))
                .andExpect(status().isOk());

        // Then clean it up
        mockMvc.perform(delete("/chaos/cleanup"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("Cleanup completed")));
    }

    @Test
    void testSequentialTestScenarios() throws Exception {
        // Run a sequence of tests to verify system behavior under various loads
        
        // 1. Basic health check
        mockMvc.perform(get("/chaos/health"))
                .andExpect(status().isOk());

        // 2. Small performance test
        mockMvc.perform(post("/chaos/performance-test")
                .param("operations", "100")
                .param("threads", "2")
                .param("async", "false"))
                .andExpect(status().isOk());

        // 3. Failover test
        mockMvc.perform(post("/chaos/failover-test")
                .param("keyCount", "50")
                .param("retryAttempts", "3"))
                .andExpect(status().isOk());

        // 4. Data integrity check
        mockMvc.perform(post("/chaos/data-integrity-test")
                .param("recordCount", "100"))
                .andExpect(status().isOk());

        // 5. Cleanup
        mockMvc.perform(delete("/chaos/cleanup"))
                .andExpect(status().isOk());
    }

    @Test
    void testFullChaosScenario() throws Exception {
        // This test runs a comprehensive chaos scenario
        
        // Step 1: Verify cluster health
        mockMvc.perform(get("/chaos/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterState", is("ok")));

        // Step 2: Load test with moderate concurrency
        mockMvc.perform(post("/chaos/performance-test")
                .param("operations", "500")
                .param("threads", "10")
                .param("async", "true"))
                .andExpect(status().isOk());

        // Step 3: Stress test with high concurrency
        mockMvc.perform(post("/chaos/stress-test")
                .param("operations", "1000")
                .param("concurrentUsers", "15"))
                .andExpect(status().isOk());

        // Step 4: Test failover scenarios
        mockMvc.perform(post("/chaos/failover-test")
                .param("keyCount", "200")
                .param("retryAttempts", "5"))
                .andExpect(status().isOk());

        // Step 5: Test data integrity under load
        mockMvc.perform(post("/chaos/data-integrity-test")
                .param("recordCount", "300"))
                .andExpect(status().isOk());

        // Step 6: Test memory handling
        mockMvc.perform(post("/chaos/memory-test")
                .param("largeObjectCount", "25")
                .param("objectSizeBytes", "5120"))
                .andExpect(status().isOk());

        // Step 7: Final cleanup
        mockMvc.perform(delete("/chaos/cleanup"))
                .andExpect(status().isOk());
    }
}