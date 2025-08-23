package com.fostermoore.redis.chaos.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.HostAndPort;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class RedisConfig {

    private static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);

    @Value("${redis.cluster.nodes:localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005,localhost:7006}")
    private List<String> clusterNodes;

    @Value("${redis.cluster.max-redirects:3}")
    private int maxRedirects;

    @Value("${redis.cluster.timeout:5000}")
    private int timeout;

    @Value("${redis.cluster.pool.max-total:50}")
    private int maxTotal;

    @Value("${redis.cluster.pool.max-idle:20}")
    private int maxIdle;

    @Value("${redis.cluster.pool.min-idle:5}")
    private int minIdle;

    @Value("${redis.cluster.pool.max-wait-millis:3000}")
    private long maxWaitMillis;

    @Bean
    @Primary
    public RedisConnectionFactory redisConnectionFactory() {
        logger.info("=== Redis Connection Factory Configuration ===");
        logger.info("Cluster nodes from config: {}", clusterNodes);
        logger.info("Max redirects: {}", maxRedirects);
        logger.info("Connection timeout: {}ms", timeout);
        
        // Test DNS resolution for each node
        for (String node : clusterNodes) {
            String[] parts = node.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            try {
                InetAddress address = InetAddress.getByName(host);
                logger.info("DNS Resolution SUCCESS: {} -> {}", host, address.getHostAddress());
                
                // Test socket connectivity
                try (java.net.Socket socket = new java.net.Socket()) {
                    socket.connect(new java.net.InetSocketAddress(host, port), 5000);
                    logger.info("Socket Connection SUCCESS: {}:{}", host, port);
                } catch (Exception socketEx) {
                    logger.error("Socket Connection FAILED: {}:{} - {}", host, port, socketEx.getMessage());
                }
                
            } catch (UnknownHostException ex) {
                logger.error("DNS Resolution FAILED: {} - {}", host, ex.getMessage());
            }
        }
        
        RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration(clusterNodes);
        clusterConfig.setMaxRedirects(maxRedirects);
        
        JedisConnectionFactory factory = new JedisConnectionFactory(clusterConfig);
        factory.afterPropertiesSet();
        
        logger.info("Redis connection factory created successfully");
        return factory;
    }

    @Bean
    public GenericObjectPoolConfig<redis.clients.jedis.Connection> jedisPoolConfig() {
        GenericObjectPoolConfig<redis.clients.jedis.Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setMaxWait(Duration.ofMillis(maxWaitMillis));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setNumTestsPerEvictionRun(10);
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMinutes(1));
        poolConfig.setMinEvictableIdleTime(Duration.ofMinutes(10));
        return poolConfig;
    }

    @Bean
    @Primary
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public JedisCluster jedisCluster() {
        logger.info("=== JedisCluster Configuration ===");
        logger.info("Creating JedisCluster with nodes: {}", clusterNodes);
        logger.info("Timeout: {}ms, MaxRedirects: {}", timeout, maxRedirects);
        
        Set<HostAndPort> hostAndPorts = clusterNodes.stream()
                .map(node -> {
                    String[] hostPort = node.split(":");
                    HostAndPort hostAndPortObj = new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1]));
                    logger.info("Added cluster node: {}", hostAndPortObj);
                    return hostAndPortObj;
                })
                .collect(Collectors.toSet());
        
        logger.info("Total cluster nodes configured: {}", hostAndPorts.size());
        
        try {
            JedisCluster cluster = new JedisCluster(hostAndPorts, timeout, timeout, maxRedirects, jedisPoolConfig());
            logger.info("JedisCluster created successfully");
            
            // Test the cluster connection immediately
            try {
                String pingResult = cluster.ping();
                logger.info("JedisCluster PING test SUCCESS: {}", pingResult);
            } catch (Exception pingEx) {
                logger.error("JedisCluster PING test FAILED: {}", pingEx.getMessage(), pingEx);
            }
            
            return cluster;
        } catch (Exception ex) {
            logger.error("JedisCluster creation FAILED: {}", ex.getMessage(), ex);
            throw ex;
        }
    }
}