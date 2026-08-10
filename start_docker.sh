#!/bin/bash

# Start Spark cluster in Docker
echo "Starting Spark cluster..."
docker compose up -d spark-master spark-worker-1 spark-worker-2 spark-worker-3

echo "Waiting for Spark Master to initialize..."
sleep 10

echo "Spark cluster started!"
echo "Spark Master UI: http://localhost:8081"
echo ""
echo "To run the Data Quality Framework with Docker driver:"
echo "  docker compose --profile dq run --rm dq-runner"
echo ""
echo "To run from host Python against Docker Spark cluster:"
echo "  python main.py --remote"
echo ""
echo "To stop the cluster:"
echo "  docker compose down"