package com.fostermoore.redis.chaos.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class RedisClusterService {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterService.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private JedisCluster jedisCluster;

    @Retryable(
        retryFor = {JedisConnectionException.class, DataAccessException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void setWithRetry(String key, Object value) {
        try {
            redisTemplate.opsForValue().set(key, value);
            logger.debug("Successfully set key: {}", key);
        } catch (Exception e) {
            logger.error("Failed to set key: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    @Retryable(
        retryFor = {JedisConnectionException.class, DataAccessException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public Object getWithRetry(String key) {
        try {
            Object value = redisTemplate.opsForValue().get(key);
            logger.debug("Successfully retrieved key: {}", key);
            return value;
        } catch (Exception e) {
            logger.error("Failed to get key: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public void setWithExpiration(String key, Object value, long ttlSeconds) {
        try {
            redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(ttlSeconds));
            logger.debug("Successfully set key with expiration: {} (TTL: {}s)", key, ttlSeconds);
        } catch (Exception e) {
            logger.error("Failed to set key with expiration: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public boolean exists(String key) {
        try {
            Boolean exists = redisTemplate.hasKey(key);
            return exists != null && exists;
        } catch (Exception e) {
            logger.error("Failed to check existence of key: {} - {}", key, e.getMessage());
            return false;
        }
    }

    public void delete(String key) {
        try {
            redisTemplate.delete(key);
            logger.debug("Successfully deleted key: {}", key);
        } catch (Exception e) {
            logger.error("Failed to delete key: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public void setHash(String key, String field, Object value) {
        try {
            redisTemplate.opsForHash().put(key, field, value);
            logger.debug("Successfully set hash field: {}:{}", key, field);
        } catch (Exception e) {
            logger.error("Failed to set hash field: {}:{} - {}", key, field, e.getMessage());
            throw e;
        }
    }

    public Object getHashField(String key, String field) {
        try {
            Object value = redisTemplate.opsForHash().get(key, field);
            logger.debug("Successfully retrieved hash field: {}:{}", key, field);
            return value;
        } catch (Exception e) {
            logger.error("Failed to get hash field: {}:{} - {}", key, field, e.getMessage());
            throw e;
        }
    }

    public void addToList(String key, Object value) {
        try {
            redisTemplate.opsForList().rightPush(key, value);
            logger.debug("Successfully added to list: {}", key);
        } catch (Exception e) {
            logger.error("Failed to add to list: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public List<Object> getList(String key, long start, long end) {
        try {
            List<Object> list = redisTemplate.opsForList().range(key, start, end);
            logger.debug("Successfully retrieved list: {} ({}:{})", key, start, end);
            return list;
        } catch (Exception e) {
            logger.error("Failed to get list: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public void addToSet(String key, Object... values) {
        try {
            redisTemplate.opsForSet().add(key, values);
            logger.debug("Successfully added to set: {}", key);
        } catch (Exception e) {
            logger.error("Failed to add to set: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public Set<Object> getSet(String key) {
        try {
            Set<Object> set = redisTemplate.opsForSet().members(key);
            logger.debug("Successfully retrieved set: {}", key);
            return set;
        } catch (Exception e) {
            logger.error("Failed to get set: {} - {}", key, e.getMessage());
            throw e;
        }
    }

    public Map<String, String> getClusterInfo() {
        try {
            // Get any connection from the cluster and execute CLUSTER INFO
            Map<String, redis.clients.jedis.ConnectionPool> clusterNodes = jedisCluster.getClusterNodes();
            if (clusterNodes.isEmpty()) {
                throw new RuntimeException("No cluster nodes available");
            }
            
            redis.clients.jedis.ConnectionPool connectionPool = clusterNodes.values().iterator().next();
            try (redis.clients.jedis.Connection connection = connectionPool.getResource()) {
                // Execute CLUSTER INFO command directly on the connection
                connection.sendCommand(redis.clients.jedis.Protocol.Command.CLUSTER, "INFO");
                String clusterInfoResponse = connection.getStatusCodeReply();
                Map<String, String> infoMap = new HashMap<>();
                
                String[] lines = clusterInfoResponse.split("\r?\n");
                for (String line : lines) {
                    if (line.contains(":")) {
                        String[] parts = line.split(":", 2);
                        infoMap.put(parts[0].trim(), parts[1].trim());
                    }
                }
                
                logger.debug("Successfully retrieved cluster info");
                return infoMap;
            }
        } catch (Exception e) {
            logger.error("Failed to get cluster info - {}", e.getMessage());
            // Return a fallback map with basic info only on error
            Map<String, String> fallbackMap = new HashMap<>();
            fallbackMap.put("cluster_state", "fail");
            fallbackMap.put("cluster_slots_assigned", "0");
            fallbackMap.put("cluster_known_nodes", "0");
            fallbackMap.put("error", e.getMessage());
            return fallbackMap;
        }
    }

    public List<Map<String, Object>> getClusterNodes() {
        try {
            // Get any connection from the cluster and execute CLUSTER NODES
            Map<String, redis.clients.jedis.ConnectionPool> clusterNodes = jedisCluster.getClusterNodes();
            if (clusterNodes.isEmpty()) {
                throw new RuntimeException("No cluster nodes available");
            }
            
            redis.clients.jedis.ConnectionPool connectionPool = clusterNodes.values().iterator().next();
            try (redis.clients.jedis.Connection connection = connectionPool.getResource()) {
                // Execute CLUSTER NODES command directly on the connection
                connection.sendCommand(redis.clients.jedis.Protocol.Command.CLUSTER, "NODES");
                String clusterNodesResponse = connection.getStatusCodeReply();
                List<Map<String, Object>> nodesList = new ArrayList<>();
                
                String[] lines = clusterNodesResponse.split("\r?\n");
                for (String line : lines) {
                    if (!line.trim().isEmpty()) {
                        String[] parts = line.split("\\s+");
                        if (parts.length >= 8) {
                            Map<String, Object> nodeInfo = new HashMap<>();
                            nodeInfo.put("id", parts[0]);
                            nodeInfo.put("address", parts[1]);
                            nodeInfo.put("flags", parts[2]);
                            nodeInfo.put("master", parts[3]);
                            nodeInfo.put("ping_sent", parts[4]);
                            nodeInfo.put("pong_recv", parts[5]);
                            nodeInfo.put("config_epoch", parts[6]);
                            nodeInfo.put("link_state", parts[7]);
                            
                            if (parts.length > 8) {
                                StringBuilder slots = new StringBuilder();
                                for (int i = 8; i < parts.length; i++) {
                                    slots.append(parts[i]).append(" ");
                                }
                                nodeInfo.put("slots", slots.toString().trim());
                            }
                            
                            nodesList.add(nodeInfo);
                        }
                    }
                }
                
                logger.debug("Successfully retrieved cluster nodes info");
                return nodesList;
            }
        } catch (Exception e) {
            logger.error("Failed to get cluster nodes - {}", e.getMessage());
            // Return empty list as fallback only on error
            return new ArrayList<>();
        }
    }

    public boolean isClusterHealthy() {
        try {
            Map<String, String> clusterInfo = getClusterInfo();
            String state = clusterInfo.get("cluster_state");
            return "ok".equals(state);
        } catch (Exception e) {
            logger.error("Failed to check cluster health - {}", e.getMessage());
            return false;
        }
    }

    public Object getWithFallback(String key, Object fallbackValue) {
        try {
            Object value = getWithRetry(key);
            return value != null ? value : fallbackValue;
        } catch (Exception e) {
            logger.warn("Using fallback value for key: {} - {}", key, e.getMessage());
            return fallbackValue;
        }
    }

    public boolean setWithFallback(String key, Object value) {
        try {
            setWithRetry(key, value);
            return true;
        } catch (Exception e) {
            logger.warn("Failed to set key, continuing with degraded service: {} - {}", key, e.getMessage());
            return false;
        }
    }
}