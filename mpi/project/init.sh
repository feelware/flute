#!/bin/bash

HOSTNAME=$(hostname)
if [ "$HOSTNAME" = "master" ] || [ "$HOSTNAME" = "mpi-master" ]; then
  echo "Waiting for RabbitMQ to be ready..."
  for i in {1..30}; do
    if nc -z rabbitmq 5672 2>/dev/null; then
      echo "RabbitMQ is ready!"
      break
    fi
    echo "Attempt $i/30: RabbitMQ not ready yet, waiting..."
    sleep 2
  done

  # Iniciar el consumer de RabbitMQ en background (SOLO EN MASTER)
  echo "Starting RabbitMQ consumer in background..."
  /usr/local/bin/rabbitmq_consumer > /var/log/rabbitmq_consumer.log 2>&1 &
  CONSUMER_PID=$!
  echo "RabbitMQ consumer started with PID: $CONSUMER_PID"
  echo "Logs: tail -f /var/log/rabbitmq_consumer.log"
else
  echo "This is a worker node ($HOSTNAME), skipping RabbitMQ consumer..."
fi
