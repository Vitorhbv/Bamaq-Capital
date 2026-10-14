#!/usr/bin/env bash
set -euo pipefail
for topic in transactions.commands.v1 transactions.status.v1 transactions.dlq.v1; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
    --create --if-not-exists --topic "$topic" --partitions 6 --replication-factor 1 \
    --config retention.ms=604800000
done
