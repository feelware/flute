#!/bin/sh
set -e

# chmod +x /.../dvp/minio-init.sh

mkdir -p /data/artifacts
minio server /data --console-address ":9001" &
MINIO_PID=$!

echo "Esperando a que MinIO esté listo..."
until curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    sleep 1
done
echo "MinIO está listo"

mc alias set local http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

if ! mc ls local/artifacts > /dev/null 2>&1; then
    echo "Creando bucket 'artifacts'..."
    mc mb local/artifacts
fi

# Política de acceso público para que permita cualquier descarga anónima
echo "Configurando política de acceso público..."
mc anonymous set download local/artifacts

echo "Bucket 'artifacts' configurado con acceso público para descarga"

wait $MINIO_PID # Espera a que MinIO termine (se mantiene el contenedor corriendo)