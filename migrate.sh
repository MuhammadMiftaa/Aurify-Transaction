#!/bin/sh

set -e

# Ambil env variables yang di-inject (3 variabel teratas)
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
GOOSE_DBSTRING=${GOOSE_DBSTRING}

# Ambil command dari argument pertama
GOOSE_CMD=${1:-up}

# Repeated separator strings — defined once to avoid duplication
SEPARATOR_THICK="========================================="
SEPARATOR_THIN="-----------------------------------------"

# Validasi: GOOSE_DBSTRING harus ada
if [ -z "$GOOSE_DBSTRING" ]; then
  echo "ERROR: GOOSE_DBSTRING environment variable is not set" >&2
  echo "Example: GOOSE_DBSTRING='postgres://user:pass@host:5432/dbname?sslmode=disable'" >&2
  exit 1
fi

echo "$SEPARATOR_THICK"
echo "Starting database migration"
echo "$SEPARATOR_THICK"
echo "Command: $GOOSE_CMD"
echo "Database Host: $DB_HOST"
echo "Database Port: $DB_PORT"
echo "$SEPARATOR_THIN"

# Set goose configuration
export GOOSE_DRIVER=postgres
export GOOSE_MIGRATION_DIR=/app/migrations

# Check database connection
echo "Checking database connection..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U postgres > /dev/null 2>&1; do
    echo "Waiting for database to be ready..."
    sleep 2
done

echo "✓ Database is ready"
echo "$SEPARATOR_THIN"
echo "Running goose $GOOSE_CMD..."
echo "GOOSE_DRIVER: $GOOSE_DRIVER"
echo "GOOSE_DBSTRING: ${GOOSE_DBSTRING%%\?*}" # Hide query params for security
echo "GOOSE_MIGRATION_DIR: $GOOSE_MIGRATION_DIR"
echo "$SEPARATOR_THIN"

# Run goose command
cd /app
goose "$GOOSE_CMD"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "$SEPARATOR_THIN"
    echo "✓ Migration completed successfully"
    echo "$SEPARATOR_THICK"
else
    echo "$SEPARATOR_THIN"
    echo "✗ Migration failed with exit code: $EXIT_CODE" >&2
    echo "$SEPARATOR_THICK"
    exit $EXIT_CODE
fi