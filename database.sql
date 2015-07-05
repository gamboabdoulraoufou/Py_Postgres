# Create database
CREATE DATABASE rpcm;

# Create a Table 
CREATE TABLE rpcm.trx
               (ID INT PRIMARY KEY NOT NULL,
                categorie CHAR(50),
                transaction_key CHAR(50) NOT NULL,
                household_key CHAR(20) NOT NULL,
                spend_amount REAL,
                transaction_date date);