#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER student WITH PASSWORD 'student' CREATEDB;
	
    CREATE DATABASE sparkifydb OWNER student;
	GRANT ALL PRIVILEGES ON DATABASE sparkifydb TO student;
    
	CREATE DATABASE studentdb OWNER student;
	GRANT ALL PRIVILEGES ON DATABASE studentdb TO student;
EOSQL