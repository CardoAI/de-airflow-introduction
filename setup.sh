#! /bin/bash
# Install Apache Airflow
AIRFLOW_VERSION=2.6.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-postgres==5.4.0 --constraint "${CONSTRAINT_URL}"

# Install other required Python packages
pip install sanpy==0.11.6

# Install PostgreSQL DB
sudo apt update && sudo apt install postgresql-15

# Setup PostgreSQL DB
export PATH="$PATH:/usr/lib/postgresql/15/bin"
sudo chmod a+w /var/run/postgresql
initdb -D ./postgresql

# Start the PostgreSQL DB
postgres -D ./postgresql >postgresql.log 2>&1 &

# Wait until PostgreSQL DB is up and running
until psql postgres -c "select 1" > /dev/null 2>&1; do
  echo "Waiting for postgres server to be ready..."
  sleep 5
done

# Create the 'crypto_scraper' DATABASE
export DB_NAME="crypto_scraper"
echo "SELECT 'CREATE DATABASE ${DB_NAME}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_NAME}')\gexec" | psql postgres
# Set password for the current user (we need this in order to connect to the DB with Airflow)
PASSWD="postgres"
echo "ALTER USER ${USER} PASSWORD '${PASSWD}'\gexec" | psql ${DB_NAME}

# Setup Airflow
export PATH="$PATH:${HOME}/.local/bin"
export AIRFLOW_HOME="${PWD}/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////${AIRFLOW_HOME}/airflow.db"

# Configure the connection to the PostgreSQL DB
export AIRFLOW_CONN_POSTGRESQL_DATABASE="postgresql+psycopg2://${USER}:${PASSWD}@localhost:5432/${DB_NAME}"

# Start Airflow
airflow standalone >airflow.log 2>&1 &
