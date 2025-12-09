#!/usr/bin/env bash
# run verifier inside running container (assuming kafka reachable at localhost:9092)
./bin/verifier --brokers=localhost:9092 --sample=1000