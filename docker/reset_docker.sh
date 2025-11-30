#!/bin/bash

echo "Stopping all containers..."
sudo docker compose down -v

echo "Removing PostgreSQL volume..."
sudo docker volume rm $(sudo docker volume ls -q | grep pgdata) || true

echo "Pruning unused volumes..."
sudo docker volume prune -f

echo "Removing containers..."
sudo docker rm -f $(sudo docker ps -a -q) || true

echo "Removing dangling images..."
sudo docker image prune -f

echo "Rebuilding containers..."
sudo docker compose up --build -d

sleep 5

sudo docker ps