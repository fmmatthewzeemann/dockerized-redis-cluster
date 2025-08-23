# Redis OSS Cluster via Docker Compose 🚀

**Fully automated Redis cluster setup with modern Docker Compose practices**

This repository provides a complete Redis cluster testing environment that eliminates manual setup steps and provides comprehensive testing tools for learning and experimenting with Redis OSS clustering.

## ✨ Features

- 🔄 **Fully automated cluster initialization** - No manual `redis-cli` commands needed
- 🏥 **Health checks & dependencies** - Services wait for dependencies to be ready  
- 🎯 **Profile-based deployment** - Easy switching between different configurations
- 🧪 **Comprehensive testing suite** - Extensive tests for cluster functionality
- 🐍 **Python test environment** - Ready-to-use testing container
- 🖥️ **Optional Web GUI** - Redis Insight for visual cluster management
- ⚡ **Modern practices** - Latest Redis and Docker Compose features

## 🚀 Quick Start

```bash
# Start full 6-node cluster with automatic initialization and testing environment
COMPOSE_PROFILES=full,init,app docker-compose up -d

# Test the cluster (runs automatically after initialization)
docker-compose exec app python simple_test.py

# Run comprehensive test suite
docker-compose exec app python connection.py
```

That's it! The cluster is automatically created and ready for testing.

## 🎯 Configuration Options

### Deployment Profiles

Choose the setup that fits your needs:

**🔧 Development (3 masters only)**
```bash
COMPOSE_PROFILES=minimal,init,app docker-compose up -d
```

**🏭 Production-like (6 nodes: 3 masters + 3 replicas)**
```bash
COMPOSE_PROFILES=full,init,app docker-compose up -d
```

**🖥️ With Redis Insight Web GUI**
```bash
COMPOSE_PROFILES=full,init,app,insight docker-compose up -d
# Access Redis Insight at http://localhost:8001
```

### Environment Configuration

Copy the starter environment to `.env`:
```bash
cp env-starter .env
```

The environment file contains configuration options:

```bash
# Redis version
REDIS_VERSION=7-alpine

# Python version for test container  
PYTHON_VERSION=3.11-alpine

# Cluster configuration
REPLICAS_PER_MASTER=1
CLUSTER_NODES=redis.localhost:7001 redis.localhost:7002 redis.localhost:7003 redis.localhost:7004 redis.localhost:7005 redis.localhost:7006

# Active profiles
COMPOSE_PROFILES=full,init,app
```

## 🧪 Testing

### Available Test Scripts

The testing environment includes comprehensive Redis cluster tests:

**Quick Connectivity Check**
```bash
docker-compose exec app python simple_test.py
```
- Verifies cluster connection
- Basic health check
- Returns success/failure status

**Comprehensive Test Suite**
```bash
docker-compose exec app python connection.py
```
- Key distribution across shards
- Cluster information and node status  
- Hash tag functionality (keys with same tag → same shard)
- Performance testing (1000 operations)
- Different Redis data types (strings, lists, sets, hashes)
- Key expiration testing
- Pattern matching operations
- Automatic cleanup

### Interactive Testing

Access the test container for manual testing:
```bash
docker-compose exec app sh
cd /usr/local/cluster-tester
python  # Start Python interpreter with redis available
```

## 📋 Management Commands

### Monitoring

**Check cluster status**
```bash
docker-compose exec redis-1 redis-cli -p 7001 cluster info
docker-compose exec redis-1 redis-cli -p 7001 cluster nodes
```

**View logs**
```bash
# All services
docker-compose logs -f

# Specific services
docker-compose logs -f cluster-init  # Initialization logs
docker-compose logs -f app           # Test container logs
docker-compose logs -f redis-1       # Individual Redis node
```

**Service status**
```bash
docker-compose ps
```

### Lifecycle Management

**Stop cluster**
```bash
docker-compose down
```

**Complete cleanup** (removes data and logs)
```bash
docker-compose down -v
rm -rf logs/
```

**Restart services**
```bash
docker-compose restart app           # Restart test container
docker-compose restart redis-1       # Restart specific Redis node
```


## 🔍 Troubleshooting

### Common Issues

**"Connection refused" errors**
```bash
# Check if cluster initialization completed
docker-compose logs cluster-init

# Verify all services are healthy
docker-compose ps
```

**"CLUSTERDOWN Hash slot not served"**
```bash
# Cluster creation didn't complete properly, restart initialization
docker-compose restart cluster-init
docker-compose logs -f cluster-init
```

**Python import errors**
```bash
# Check if dependencies installed correctly
docker-compose logs app
docker-compose exec app pip list | grep redis
```

### Manual Cluster Creation (if needed)

If automatic initialization fails, you can create the cluster manually:
```bash
docker-compose exec redis-1 redis-cli -p 7001 --cluster create redis.localhost:7001 redis.localhost:7002 redis.localhost:7003 redis.localhost:7004 redis.localhost:7005 redis.localhost:7006 --cluster-replicas 1 --cluster-yes
```

### Reset Everything

For a complete fresh start:
```bash
docker-compose down -v
rm -rf logs/
COMPOSE_PROFILES=full,init,app docker-compose up -d
```
## 📁 Repository Structure

```
├── docker-compose.yml          # Main automated cluster setup
├── .env                       # Environment configuration  
├── app/                       # Python testing environment
│   ├── simple_test.py         # Quick connectivity check
│   ├── connection.py          # Comprehensive test suite  
│   ├── requirements.txt       # Python dependencies
│   └── docker-entrypoint.sh   # Container initialization
├── scripts/                   # Cluster management scripts
│   ├── init-cluster.sh        # Automated cluster initialization
│   └── cluster-status.sh      # Health monitoring
├── logs/                      # Redis logs (created at runtime)
├── legacy/                    # Archived legacy setup files
│   ├── docker-compose.legacy.yml
│   ├── create_cluster.sh
│   ├── delete_cluster.sh
│   └── configs/               # Static config files
└── README.md                  # This documentation
```

## 🔧 Advanced Usage

### Custom Redis Configuration

To modify Redis settings, edit the command parameters in `docker-compose.yml`:

```yaml
command: >
  redis-server
  --port 7001
  --cluster-enabled yes
  --cluster-config-file nodes.conf
  --cluster-node-timeout 5000
  --appendonly yes
  --bind 0.0.0.0
  --protected-mode no
  # Add your custom settings here
```

### Adding More Nodes

To create larger clusters, add more Redis services to the docker-compose.yml following the existing pattern and update the `CLUSTER_NODES` environment variable.


## 📄 License

This project is provided as-is for educational and testing purposes. Please refer to Redis OSS licensing for production use.

---

**Happy Redis clustering! 🎉**

*This setup provides a complete learning environment for Redis OSS clustering. Feel free to experiment, break things, and learn how Redis clusters work in a safe, reproducible environment.*
