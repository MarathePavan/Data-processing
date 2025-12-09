#!/usr/bin/env bash
set -euo pipefail

export GOMAXPROCS=${GOMAXPROCS:-4}

echo "Start script running. GOMAXPROCS=${GOMAXPROCS}"

mkdir -p /data
mkdir -p /data/runs

echo "Starting Redpanda..."
if ! command -v redpanda >/dev/null 2>&1; then
  echo "redpanda binary not found. Exiting."
  exit 1
fi

redpanda start --overprovisioned 1 --developer-mode 1 --node-id 0 --smp 1 --memory 256M || {
  echo "redpanda start returned non-zero (it might already be running)"
}

echo "Waiting for Kafka broker on 9092..."
for i in $(seq 1 60); do
  if nc -z localhost 9092; then
    echo "Broker available."
    break
  fi
  sleep 1
  echo "waiting..."
done

echo "Creating topics: source,id,name,continent"
if command -v rpk >/dev/null 2>&1; then
  rpk topic create source --replicas 1 || true
  rpk topic create id --replicas 1 || true
  rpk topic create name --replicas 1 || true
  rpk topic create continent --replicas 1 || true
else
  echo "rpk not found; topics may be auto-created by producers/clients."
fi

GEN_COUNT=${GEN_COUNT:-50000000}
GEN_BATCH=${GEN_BATCH:-2000}
MEMBUF=${MEMBUF:-150000}

echo "Starting generator (count=${GEN_COUNT})..."
/app/bin/generator --brokers=localhost:9092 --topic=source --count=${GEN_COUNT} --batch=${GEN_BATCH} &
GEN_PID=$!

sleep 2

echo "Starting sorter..."
/app/bin/sorter --brokers=localhost:9092 --source=source --count=${GEN_COUNT} --membuf=${MEMBUF} --rundir=/data/runs &
SORT_PID=$!

wait ${GEN_PID}
wait ${SORT_PID}

echo "Generator and sorter finished."

echo "Running verifier..."
/app/bin/verifier --brokers=localhost:9092 --sample=1000 || true

echo "Stopping redpanda..."
redpanda stop || true

echo "All done."