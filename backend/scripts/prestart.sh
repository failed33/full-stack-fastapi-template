#! /usr/bin/env bash

set -e
set -x

# Let the DB start
python app/backend_pre_start.py

# Run migrations
alembic upgrade head

# Create initial data in DB
python app/initial_data.py

# Initialize MinIO buckets and policies
python app/minio_init.py

echo "Prestart script completed successfully."
