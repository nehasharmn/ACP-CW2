#!/bin/bash

echo "Setting up environment for ACP Assignment 2"

# Create docker network if not exists
if ! docker network ls | grep -q acp_network; then
  echo "Creating docker network: acp_network"
  docker network create acp_network
else
  echo "Docker network acp_network already exists"
fi

# Start Redis if not running
if ! docker ps | grep -q redis; then
  echo "Starting Redis"
  docker run -d --name redis -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
else
  echo "Redis is already running"
fi

# Start RabbitMQ if not running
if ! docker ps | grep -q rabbitmq; then
  echo "Starting RabbitMQ"
  docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management
else
  echo "RabbitMQ is already running"
fi

# Start Kafka if not running
if ! docker ps | grep -q kafka; then
  echo "Starting Kafka"
  docker-compose -p acp up -d
else
  echo "Kafka is already running"
fi

echo "Environment setup complete!"
echo "Redis: localhost:6379"
echo "RabbitMQ: localhost:5672 (Management: http://localhost:15672)"
echo "Kafka: localhost:9092"
echo "ACP Storage Service: https://acp-storage.azurewebsites.net"