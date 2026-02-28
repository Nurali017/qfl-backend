#!/bin/sh
set -e

# Run Alembic migrations only for the main backend (gunicorn), not celery workers
if [ "$1" = "gunicorn" ]; then
    echo "Running database migrations..."
    python -c "
from alembic.config import Config
from alembic import command
alembic_cfg = Config('alembic.ini')
command.upgrade(alembic_cfg, 'heads')
print('Migrations completed successfully.')
"
fi

exec "$@"
