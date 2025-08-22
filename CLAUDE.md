# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a **fully automated Redis OSS cluster setup** using Docker Compose with profile-based deployment. The system provides:

- **Profile-based cluster sizing**: `minimal` (3 masters), `full` (6 nodes with replicas)
- **Automated cluster initialization**: No manual redis-cli commands needed
- **Comprehensive testing environment**: Python-based test suite with extensive cluster validation
- **Health-checked dependencies**: Services wait for dependencies to be ready before starting
- **Network isolation**: Custom bridge network with static IP assignments (10.0.0.x/16)

### Key Components

- **Redis Nodes**: 6 total nodes (redis-1 through redis-6) with cluster-enabled configuration
- **Cluster Initializer**: `cluster-init` service that automatically creates the cluster using `scripts/init-cluster.sh`
- **Test Application**: Python container with redis-py-cluster for testing and validation
- **Optional Redis Insight**: Web GUI for cluster visualization (profile: `insight`)

### Network Architecture

- Static IP assignments: 10.0.0.11-16 for Redis nodes, 10.0.0.20 for app container
- Port mapping: 7001-7006 for external access to Redis nodes
- Internal cluster communication via node IDs and internal network

## Common Commands

### Starting the Cluster

```bash
# Full 6-node cluster with initialization and testing
COMPOSE_PROFILES=full,init,app docker-compose up -d

# Minimal 3-master cluster
COMPOSE_PROFILES=minimal,init,app docker-compose up -d

# With Redis Insight GUI (access at http://localhost:5544)
COMPOSE_PROFILES=full,init,app,insight docker-compose up -d
```

### Testing Commands

```bash
# Quick connectivity test
docker-compose exec app python simple_test.py

# Comprehensive cluster test suite (key distribution, performance, data types)
docker-compose exec app python connection.py

# Manual cluster status check
docker-compose exec redis-1 redis-cli -p 7001 cluster info
docker-compose exec redis-1 redis-cli -p 7001 cluster nodes
```

### Management Commands

```bash
# View cluster initialization logs
docker-compose logs -f cluster-init

# Check service status
docker-compose ps

# Access Python test environment interactively
docker-compose exec app sh

# Complete cleanup (removes volumes and logs)
docker-compose down -v && rm -rf logs/
```

### Monitoring and Health Checks

```bash
# Run cluster status script
./scripts/cluster-status.sh

# View individual node logs
docker-compose logs redis-1

# Check all service logs
docker-compose logs -f
```

## Configuration

### Environment Setup

Copy `starter-env` to `.env` and modify as needed:
- `REDIS_VERSION`: Redis Docker image version (default: 7-alpine)
- `PYTHON_VERSION`: Python container version (default: 3.11-alpine)
- `REPLICAS_PER_MASTER`: Number of replicas per master (default: 1)
- `CLUSTER_NODES`: Space-separated list of cluster node addresses
- `COMPOSE_PROFILES`: Active Docker Compose profiles

### Profile System

- `minimal`: 3 master nodes only (redis-1, redis-2, redis-3)
- `full`: All 6 nodes (3 masters + 3 replicas)  
- `replicas`: Additional nodes for replica functionality
- `init`: Automated cluster initialization service
- `app`: Python testing container
- `insight`: Redis Insight web GUI

## Development Notes

### Test Suite Structure

- `app/simple_test.py`: Basic connectivity and health check
- `app/connection.py`: Comprehensive test suite including:
  - Key distribution across shards
  - Hash tag functionality validation
  - Performance testing (1000 ops)
  - Multiple data type testing
  - Key expiration validation
  - Pattern matching operations

### Cluster Initialization Process

1. Health checks ensure all Redis nodes are ready
2. `scripts/init-cluster.sh` checks for existing cluster
3. If no cluster exists, creates new cluster with specified topology
4. Validates cluster creation with retries
5. Displays cluster information and node assignments

### Adding Nodes

To expand the cluster:
1. Add new Redis service to `docker-compose.yml` following existing pattern
2. Update `CLUSTER_NODES` in environment configuration
3. Assign static IP in 10.0.0.x range
4. Add appropriate profiles if needed

### Troubleshooting Commands

```bash
# Manual cluster creation if initialization fails
docker-compose exec redis-1 redis-cli -p 7001 --cluster create 10.0.0.11:7001 10.0.0.12:7002 10.0.0.13:7003 10.0.0.14:7004 10.0.0.15:7005 10.0.0.16:7006 --cluster-replicas 1 --cluster-yes

# Check cluster slots assignment
docker-compose exec redis-1 redis-cli -p 7001 cluster slots

# Verify Python dependencies
docker-compose exec app pip list | grep redis
```