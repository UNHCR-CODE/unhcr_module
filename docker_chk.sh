#!/bin/bash

# Define the working directory containing docker-compose.yml
COMPOSE_DIR=~/prospect

# Define a container name to check (you can change this)
EXPECTED_CONTAINER="prospect-core-1"

# Function to check if a port is accessible
check_port() {
    nc -z -v -w5 $1 $2 > /dev/null 2>&1
    return $?
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "$(date): Docker not running. Attempting to start Docker..."
    sudo systemctl start docker

    # Wait until Docker is up
    while ! docker info > /dev/null 2>&1; do
        echo "Waiting for Docker to start..."
        sleep 3
    done
    echo "Docker started successfully."
else
    echo "$(date): Docker is already running."
fi

# Check if the expected container is running
if ! docker ps --format '{{.Names}}' | grep -q "^$EXPECTED_CONTAINER$"; then
    echo "$(date): Container $EXPECTED_CONTAINER not running. Starting Docker Compose..."

    # Navigate to compose directory and start services
    cd "$COMPOSE_DIR" || { echo "Failed to change directory to $COMPOSE_DIR"; exit 1; }

    # Start services
    docker compose up -d

    echo "$(date): Docker Compose started."
else
    echo "$(date): Container $EXPECTED_CONTAINER is already running."
fi

# Check if key services are accessible via ports
RESTART_NEEDED=false

if ! check_port localhost 3000; then
    echo "⚠️  localhost:3000 not accessible (Grafana?)"
    RESTART_NEEDED=true
fi

if ! check_port localhost 5431; then
    echo "⚠️  localhost:5431 not accessible (Postgres?)"
    RESTART_NEEDED=true
fi

# If any port check failed, restart Docker Compose
if [ "$RESTART_NEEDED" = true ]; then
    echo "$(date): Restarting services due to inaccessible ports."

    # Take down the current containers and bring them up again
    cd "$COMPOSE_DIR" || { echo "Failed to change directory to $COMPOSE_DIR"; exit 1; }
    docker compose down
    docker compose up -d

    echo "$(date): Docker Compose services restarted."
else
    echo "$(date): All key services are accessible."
fi
