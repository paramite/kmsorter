#!/bin/sh

podman run --name nats -d -p 4222:4222 -p 8222:8222 nats --http_port 8222
podman run --name redis -d -p 6379:6379 redis/redis-stack:latest
