# Redis Chaos Monkey Tester

A comprehensive Spring Boot application for chaos monkey testing of Redis clusters using Jedis. This application provides extensive testing capabilities for performance, failover, data integrity, and graceful degradation scenarios.

## Features

### Core Capabilities
- **Redis Cluster Integration** - Full support for Redis cluster mode with automatic failover
- **Jedis Client** - Using Jedis 6.0.0 for robust Redis cluster connectivity
- **Comprehensive Testing Suite** - Multiple test scenarios covering all aspects of Redis operations
- **Performance Benchmarking** - Detailed performance and throughput testing
- **Chaos Engineering** - Built-in chaos monkey patterns for resilience testing
- **Graceful Degradation** - Fallback mechanisms for service continuity

### Test Scenarios

#### 1. **Cluster Health & Basic Operations**
- Cluster state validation
- Node discovery and health checks
- Basic CRUD operations verification
- Key distribution analysis

#### 2. **Performance & Throughput Testing**
- Configurable load testing with multiple threads
- Detailed metrics collection (latency, throughput, error rates)
- Memory usage analysis
- Large object handling

#### 3. **Failover & Retry Mechanisms**
- Automatic retry with exponential backoff
- Connection pool resilience testing
- Node failure simulation handling
- Cluster reconfiguration testing

#### 4. **Data Integrity Testing**
- Data corruption detection
- Consistency verification across shards
- Hash tag functionality validation
- Cross-shard operation testing

#### 5. **Concurrent Access Stress Testing**
- Multi-threaded concurrent operations
- Race condition detection
- Deadlock prevention validation
- Resource contention analysis

#### 6. **Memory & Large Value Testing**
- Large object storage and retrieval
- Memory pressure simulation
- Data serialization/deserialization stress
- Hash, List, and Set operations with large datasets

## Architecture

### Configuration
- **Connection Pooling**: Optimized connection pool settings with health checks
- **Retry Logic**: Configurable retry mechanisms with circuit breaker patterns
- **Monitoring**: Integrated metrics and health endpoints
- **Profiles**: Environment-specific configurations (test, integration, production)

### Service Layer
- **RedisClusterService**: Core service with retry logic and fallback mechanisms
- **Graceful Degradation**: Automatic fallback to cached or default values
- **Connection Management**: Intelligent connection pooling and lifecycle management

### REST API Endpoints

#### Health & Monitoring
```bash
GET /chaos/health                    # Cluster health and node information
GET /actuator/health                 # Spring Boot health checks
GET /actuator/metrics               # Performance metrics
GET /actuator/prometheus            # Prometheus metrics
```

#### Performance Testing
```bash
POST /chaos/performance-test?operations=1000&threads=10&async=false
POST /chaos/stress-test?operations=5000&concurrentUsers=20
POST /chaos/memory-test?largeObjectCount=100&objectSizeBytes=10240
```

#### Resilience Testing
```bash
POST /chaos/failover-test?keyCount=100&retryAttempts=5
POST /chaos/data-integrity-test?recordCount=1000
```

#### Cleanup
```bash
DELETE /chaos/cleanup                # Clean up all test data
```

## Usage

### Starting the Application

1. **Ensure Redis cluster is running:**
```bash
cd /path/to/dockerized-redis-cluster
COMPOSE_PROFILES=full,init,app docker-compose up -d
```

2. **Build and run the application:**
```bash
cd redis-chaos-app
mvn clean install
mvn spring-boot:run
```

3. **Or run with specific profile:**
```bash
mvn spring-boot:run -Dspring.profiles.active=integration
```

### Running Tests

#### Unit and Integration Tests
```bash
mvn test                    # Unit tests
mvn failsafe:integration-test  # Integration tests
mvn verify                  # Full test suite
```

#### Chaos Monkey Integration Tests
```bash
mvn test -Dtest=ChaosMonkeyIT
```

### API Usage Examples

#### Check Cluster Health
```bash
curl http://localhost:8080/chaos/health
```

#### Run Performance Test
```bash
curl -X POST "http://localhost:8080/chaos/performance-test?operations=5000&threads=20"
```

#### Run Stress Test
```bash
curl -X POST "http://localhost:8080/chaos/stress-test?operations=10000&concurrentUsers=50"
```

#### Test Failover Scenarios
```bash
curl -X POST "http://localhost:8080/chaos/failover-test?keyCount=500&retryAttempts=3"
```

## Configuration

### Redis Connection Settings
```yaml
redis:
  cluster:
    nodes: localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005,localhost:7006
    max-redirects: 3
    timeout: 5000
    pool:
      max-total: 50
      max-idle: 20
      min-idle: 5
      max-wait-millis: 3000
```

### Performance Tuning
- **Connection Pool Size**: Adjust based on expected concurrent load
- **Timeout Settings**: Configure based on network latency and cluster size
- **Retry Configuration**: Tune retry attempts and backoff strategy
- **Thread Pool Size**: Optimize for your hardware and load patterns

## Metrics and Monitoring

The application provides comprehensive metrics through:

- **Spring Boot Actuator** - Health checks, metrics, and application info
- **Micrometer** - Performance metrics collection
- **Prometheus** - Metrics export for monitoring systems
- **Custom Metrics** - Redis-specific performance indicators

### Key Metrics
- Response time percentiles (P50, P95, P99)
- Throughput (operations per second)
- Error rates and types
- Connection pool utilization
- Redis cluster health status

## Chaos Engineering Patterns

### 1. **Dependency Injection of Failures**
- Automatic retry mechanisms
- Circuit breaker patterns
- Fallback value provision

### 2. **Resource Exhaustion Testing**
- Connection pool exhaustion
- Memory pressure scenarios
- Thread starvation conditions

### 3. **Network Partition Simulation**
- Node unavailability handling
- Cluster split-brain scenarios
- Network latency injection

### 4. **Data Consistency Validation**
- Cross-shard consistency checks
- Eventual consistency verification
- Data corruption detection

## Best Practices

### Testing Strategy
1. **Start Simple** - Begin with basic health checks
2. **Gradual Load Increase** - Progressively increase test intensity
3. **Monitor Resources** - Watch system resources during tests
4. **Cleanup After Tests** - Always clean up test data
5. **Document Results** - Record performance baselines and thresholds

### Production Readiness
- Use connection pooling for production deployments
- Implement comprehensive monitoring and alerting
- Set up proper retry and circuit breaker configurations
- Plan for graceful degradation scenarios
- Regular chaos testing in staging environments

## Troubleshooting

### Common Issues
- **Connection Timeouts**: Increase timeout values or check network connectivity
- **Pool Exhaustion**: Increase pool size or reduce connection hold times
- **Memory Issues**: Monitor JVM heap usage and Redis memory consumption
- **Cluster Reconfiguration**: Wait for cluster to stabilize after node changes

### Debugging
- Enable DEBUG logging for detailed operation traces
- Use Spring Boot Actuator endpoints for runtime diagnostics
- Monitor Redis cluster logs during testing
- Check connection pool statistics via metrics endpoints

## Contributing

When adding new tests or features:
1. Follow existing patterns for retry and error handling
2. Add comprehensive logging for debugging
3. Include cleanup logic for test data
4. Update documentation and examples
5. Ensure thread-safety for concurrent operations