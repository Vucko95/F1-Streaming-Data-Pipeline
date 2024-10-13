#!/bin/bash


until curl -s http://localhost:8088/health | grep "OK"; do
  echo "Waiting for Superset to start..."
  sleep 5
done



access_token=$(curl -X POST 'http://localhost:8088/api/v1/security/login' \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "db",
    "refresh": true,
    "password": "admin",
    "username": "admin"
  }' | grep -o '"access_token":"[^"]*' | sed 's/"access_token":"//')

# echo "Access Token: $access_token"

curl -X POST \
  http://localhost:8088/api/v1/dataset/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $access_token" \
  -d '{
    "database": 1,
    "schema": "public",
    "table_name": "car_setup_data"
  }'