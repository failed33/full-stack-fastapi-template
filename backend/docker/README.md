# Dockerfiles for Hardware-Specific Workers

This directory contains specialized Dockerfiles for different hardware acceleration scenarios in the TTS pipeline.

## Dockerfiles

### `Dockerfile.cpu`
- **Purpose**: CPU-only workers for tasks like conversion, splitting, and CPU-based transcription
- **Base Image**: `python:3.12-slim`
- **Key Features**:
  - FFmpeg for audio processing
  - Redis tools for debugging
  - Health check script at `/usr/local/bin/redis-health-check.sh`

### `Dockerfile.cuda`
- **Purpose**: NVIDIA CUDA GPU workers for accelerated transcription
- **Base Image**: `nvidia/cuda:12.1.1-devel-ubuntu22.04`
- **Key Features**:
  - CUDA 12.1.1 development environment
  - PyTorch with CUDA support (automatically installed)
  - Health check script at `/usr/local/bin/health-check.sh` (checks both Redis and CUDA)

### `Dockerfile.rocm`
- **Purpose**: AMD ROCm GPU workers for accelerated transcription
- **Base Image**: `rocm/dev-ubuntu-22.04:5.7`
- **Key Features**:
  - ROCm 5.7 development environment
  - PyTorch with ROCm support (automatically installed)
  - Health check script at `/usr/local/bin/health-check.sh` (checks Redis, PyTorch, and ROCm)

## Environment Variables

All workers expect these Redis configuration variables:
- `REDIS_HOST`: Redis server hostname (default: `redis`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `REDIS_DB`: Redis database number (default: `0`)

## Usage with Docker Compose

The workers are configured in `docker-compose.override.yml` with different profiles:

```bash
# Start with CPU workers only (default)
docker-compose up -d

# Start with CUDA workers
docker-compose --profile cuda up -d

# Start with ROCm workers
docker-compose --profile rocm up -d
```

## Worker Commands

Each worker runs Dramatiq with specific queue assignments:

- **convert/split/final workers**: Use `Dockerfile.cpu` and listen to CPU queues
- **transcribe-cuda worker**: Uses `Dockerfile.cuda` and listens to `transcribe_cuda` queue
- **transcribe-rocm worker**: Uses `Dockerfile.rocm` and listens to `transcribe_rocm` queue

## Health Checks and Debugging

### Test Redis Connection
```bash
# Inside any container
redis-health-check.sh

# Or manually
redis-cli -h $REDIS_HOST -p $REDIS_PORT ping
```

### Test GPU Availability
```bash
# CUDA workers
health-check.sh

# Or manually check CUDA
python3 -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"

# ROCm workers
rocm-smi --showproductname
```

### Monitor Worker Logs
```bash
# Monitor specific worker types
docker-compose logs -f worker-transcribe-cuda
docker-compose logs -f worker-transcribe-cpu
docker-compose logs -f worker-convert
```

## Hardware Routing

The system automatically routes transcription tasks based on the `target_hardware` field:

- `"cpu"` → `transcribe_cpu` queue → CPU worker
- `"cuda"` → `transcribe_cuda` queue → CUDA worker
- `"rocm"` → `transcribe_rocm` queue → ROCm worker

Conversion and splitting tasks always run on CPU workers regardless of target hardware.

## Performance Tuning

### Worker Process Counts
Adjust the `--processes` parameter in docker-compose commands:
- CPU workers: 2-4 processes per worker service
- GPU workers: 1-2 processes (GPUs are shared between processes)

### Memory Considerations
- CPU workers: ~1-2GB RAM per process
- CUDA workers: ~2-4GB RAM + GPU memory
- ROCm workers: ~2-4GB RAM + GPU memory

### Queue Scaling
Scale workers independently by running multiple instances:
```bash
# Scale CUDA workers
docker-compose --profile cuda up -d --scale worker-transcribe-cuda=3
```
