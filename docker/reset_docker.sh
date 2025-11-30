#!/bin/bash

echo "Stopping all containers..."
docker compose down -v

echo "Removing PostgreSQL volume..."
docker volume rm $(docker volume ls -q | grep pgdata) || true

echo "Pruning unused volumes..."
docker volume prune -f

echo "Removing dangling images..."
docker image prune -f

echo "Rebuilding containers..."
docker compose up --build -d

sleep 5

docker ps
