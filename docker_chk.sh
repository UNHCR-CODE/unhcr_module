#!/bin/bash

COMPOSE_DIR=~/prospect

# Check if Docker is running
if ! systemctl is-active --quiet docker; then
    echo "Docker is not running. Attempting to start Docker..."
    sudo systemctl start docker && echo "Docker started successfully." || { echo "Failed to start Docker."; exit 1; }
else
    echo "Docker is already running."
fi

# Check again to confirm docker is active
if systemctl is-active --quiet docker; then
    echo "Starting Docker Compose services..."

    cd "$COMPOSE_DIR" && \
    docker-compose up -d && \
    echo "Docker Compose started successfully." || \
    echo "Failed to start Docker Compose."
else
    echo "Docker is still not running. Cannot proceed with Docker Compose."
    exit 1
fi
