#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until nc -z postgres 5432; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done
echo "PostgreSQL is up - starting API server"


# Start the API server
echo "Starting API server..."
exec uvicorn src.api:app --host 0.0.0.0 --port 8000
