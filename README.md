# MCP Data Sorter

Generates CSV data (schema: id,name,address,Continent), produces to Kafka topic `source`, sorts by id/name/continent and publishes to topics `id`, `name`, `continent`.

Single-container image includes Redpanda (Kafka-compatible) and Go binaries.

## Build
Edit scripts/build.sh image tag or pass tag:
./scripts/build.sh yourdockerhubuser/mcp-data-sorter:latest

## Run
Quick local test (small dataset):
./scripts/run_local.sh yourdockerhubuser/mcp-data-sorter:latest

Full run (inside docker):
docker run --rm -it --cpus=4 --memory=2g --memory-swap=2g -v /tmp/mcp_data:/data yourdockerhubuser/mcp-data-sorter:latest

Override env:
-e GEN_COUNT=10000 -e MEMBUF=20000

## Important notes
- Disk: external sorting writes temp files to /data/runs. Ensure host volume has enough space (8+ GB for 50M).
- Memory: container should be run with --memory=2g and --cpus=4.
- Performance: run_generator uses byte-level parsing for speed. Tweak --membuf to tune memory vs runs.

## Verify
Verifier samples topics:
./bin/verifier --brokers=localhost:9092 --sample=1000
