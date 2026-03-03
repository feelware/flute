#!/bin/bash

# Start the API server
echo "Starting API server..."
exec uvicorn src.api:app --host 0.0.0.0 --port 8000
