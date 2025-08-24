package com.fostermoore.redis.chaos.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.net.InetAddress;
import java.util.Arrays;

@Component
public class RedisConnectionTester {

    private static final Logger logger = LoggerFactory.getLogger(RedisConnectionTester.class);

    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private JedisCluster jedisCluster;

    @EventListener(ApplicationReadyEvent.class)
    public void testRedisConnectionOnStartup() {
        logger.info("============================================");
        logger.info("=== Redis Connection Diagnostic Test ===");
        logger.info("============================================");

        // 1. Show environment properties
        showEnvironmentProperties();

        // 2. Test DNS resolution
        testDnsResolution();

        // 3. Test socket connectivity
        testSocketConnectivity();

        // 4. Test JedisCluster connectivity
        testJedisClusterConnectivity();

        logger.info("============================================");
        logger.info("=== Redis Diagnostic Test Complete ===");
        logger.info("============================================");
    }

    private void showEnvironmentProperties() {
        logger.info("--- Environment Properties ---");
        
        // Check various property sources
        String[] propertiesToCheck = {
                "redis.cluster.nodes",
                "spring.data.redis.cluster.nodes",
                "spring.profiles.active"
        };

        for (String property : propertiesToCheck) {
            String value = environment.getProperty(property);
            logger.info("Property {}: {}", property, value != null ? value : "NOT SET");
        }

        // Show all redis-related properties
        logger.info("--- All Redis Properties ---");
        if (environment.getProperty("redis.cluster.nodes") != null) {
            String nodes = environment.getProperty("redis.cluster.nodes");
            logger.info("redis.cluster.nodes: {}", nodes);
            String[] nodeArray = nodes.split(",");
            logger.info("Parsed {} cluster nodes:", nodeArray.length);
            for (int i = 0; i < nodeArray.length; i++) {
                logger.info("  Node {}: {}", i + 1, nodeArray[i].trim());
            }
        }
    }

    private void testDnsResolution() {
        logger.info("--- DNS Resolution Test ---");
        String nodesProperty = environment.getProperty("redis.cluster.nodes", 
                "localhost:7001,localhost:7002,localhost:7003");
        
        String[] nodes = nodesProperty.split(",");
        for (String node : nodes) {
            String[] parts = node.trim().split(":");
            if (parts.length == 2) {
                String host = parts[0];
                try {
                    InetAddress address = InetAddress.getByName(host);
                    logger.info("DNS SUCCESS: {} -> {}", host, address.getHostAddress());
                    
                    // Test if it's loopback
                    if (address.isLoopbackAddress()) {
                        logger.info("  {} is loopback address", host);
                    }
                    
                    // Test if it's reachable
                    boolean reachable = address.isReachable(5000);
                    logger.info("  {} is reachable: {}", host, reachable);
                    
                } catch (Exception e) {
                    logger.error("DNS FAILED: {} - {}", host, e.getMessage());
                }
            }
        }
    }

    private void testSocketConnectivity() {
        logger.info("--- Socket Connectivity Test ---");
        String nodesProperty = environment.getProperty("redis.cluster.nodes", 
                "localhost:7001,localhost:7002,localhost:7003");
        
        String[] nodes = nodesProperty.split(",");
        for (String node : nodes) {
            String[] parts = node.trim().split(":");
            if (parts.length == 2) {
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                try (java.net.Socket socket = new java.net.Socket()) {
                    socket.connect(new java.net.InetSocketAddress(host, port), 5000);
                    logger.info("SOCKET SUCCESS: {}:{}", host, port);
                    
                    // Test Redis PING
                    try {
                        java.io.OutputStream out = socket.getOutputStream();
                        java.io.InputStream in = socket.getInputStream();
                        
                        // Send PING command
                        out.write("PING\r\n".getBytes());
                        out.flush();
                        
                        // Read response
                        byte[] buffer = new byte[1024];
                        int bytesRead = in.read(buffer);
                        if (bytesRead > 0) {
                            String response = new String(buffer, 0, bytesRead);
                            logger.info("  PING Response: {}", response.trim());
                        }
                        
                    } catch (Exception pingEx) {
                        logger.warn("  PING test failed: {}", pingEx.getMessage());
                    }
                    
                } catch (Exception e) {
                    logger.error("SOCKET FAILED: {}:{} - {}", host, port, e.getMessage());
                }
            }
        }
    }

    private void testJedisClusterConnectivity() {
        logger.info("--- JedisCluster Connectivity Test ---");
        
        if (jedisCluster == null) {
            logger.error("JedisCluster bean is NULL - dependency injection failed");
            return;
        }
        
        try {
            logger.info("JedisCluster bean available: {}", jedisCluster.getClass().getName());
            
            // Test basic connectivity
            logger.info("Testing JedisCluster PING...");
            String pingResult = jedisCluster.ping();
            logger.info("JedisCluster PING SUCCESS: {}", pingResult);
            
            // Test cluster info
            logger.info("Testing cluster info retrieval...");
            var clusterNodes = jedisCluster.getClusterNodes();
            logger.info("JedisCluster reports {} cluster nodes", clusterNodes.size());
            
            for (var entry : clusterNodes.entrySet()) {
                logger.info("  Cluster node: {}", entry.getKey());
            }
            
        } catch (Exception e) {
            logger.error("JedisCluster test FAILED: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            logger.error("Full exception:", e);
        }
    }
}