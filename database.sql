
# Chande user
sudo su - postgres

# Log on postgres
psql

# Create user
CREATE USER abdoul WITH PASSWORD '1234';

# Create database
CREATE DATABASE test_db;

# Grant acces to user abdoul
GRANT ALL PRIVILEGES ON DATABASE test_db to abdoul;

# Quit postgres
\q

# Connect to postgres database
psql -h 127.0.0.1 -d test_db -U abdoul

# Change database
\c test_db

# Create a Table 
CREATE TABLE trx
               (ID INT PRIMARY KEY NOT NULL,
                quantity REAL,
                spend_amount REAL,
                period CHAR(50) NOT NULL,
                hhk_code CHAR(50) NOT NULL,
                trx_key_code CHAR(50) NOT NULL,
                sub_code CHAR(50) NOT NULL);
