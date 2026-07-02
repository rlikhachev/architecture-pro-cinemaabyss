#!/bin/bash

# Set up test environment
export PORT=8000
export MONOLITH_URL="http://localhost:8080"
export MOVIES_SERVICE_URL="http://localhost:8081"
export EVENTS_SERVICE_URL="http://localhost:8082"
export MOVIES_MIGRATION_PERCENT="50"
export RANDOM_SEED="test"
export ROUTING_SEED="test"

# Build the proxy service
GOOS=linux CGO_ENABLED=0 go build -a -o proxy-service ./src/microservices/proxy

# Start the proxy service in the background
echo "Starting proxy service on port 8000..."
./proxy-service &
PROXY_PID=$!

# Wait for proxy service to start
sleep 2

# Test proxy service health
api_response=$(curl -s http://localhost:8000/health || echo "FAILED")
echo "Proxy service health: $api_response"

# Test /api/movies routing - make requests to see routing behavior
echo "Testing /api/movies endpoint routing (50% to movies-service, 50% to monolith)..."
for i in {1..10}; do
    response=$(curl -s -w "%{http_code}" "http://localhost:8000/api/movies" || echo "FAILED 000")
    echo "Request $i: HTTP $response"
    sleep 0.1
done

# Test /api/users endpoint (should always go to monolith)
echo "Testing /api/users endpoint routing (should always go to monolith)..."
for i in {1..5}; do
    echo "Request $i: $(curl -s -w "\n%{http_code}" "http://localhost:8000/api/users" || echo "FAILED")"
done

# Test /api/movies/health endpoint (should go to movies-service)
echo "Testing /api/movies/health endpoint routing (should go to movies-service)..."
response=$(curl -s -w "\n%{http_code}" "http://localhost:8000/api/movies/health" || echo "FAILED 000")

echo "Testing /api/events/health endpoint routing (should go to events-service)..."
response=$(curl -s -w "\n%{http_code}" "http://localhost:8000/api/events/health" || echo "FAILED 000")

# Stop the proxy service
echo "Stopping proxy service (PID: $PROXY_PID)..."
kill $PROXY_PID 2>/dev/null || true
wait $PROXY_PID 2>/dev/null || true

echo "Test completed."
