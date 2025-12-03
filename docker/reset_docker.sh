#!/bin/bash

sudo docker compose down -v

sudo docker volume rm $(sudo docker volume ls -q | grep pgdata) || true

sudo docker volume prune -f

sudo docker rm -f $(sudo docker ps -a -q) || true

sudo docker image prune -f

sudo docker compose up --build -d

sleep 5

sudo docker ps