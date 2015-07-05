#!/usr/bin/python

import psycopg2
inport json
import csv

# Get configuration file data
with open('conf.json') as conf_file:    
    conf = json.load(conf_file)

# Connecting To Database     
try:
  conn = psycopg2.connect(database="testdb", user="postgres", password="123", host="127.0.0.1", port="5432")
  print "Opened database successfully"
except:
  print "Connexion wrong"

# Create a Table  
cur = conn.cursor()
cur.execute('''CREATE TABLE trx
               (ID INT PRIMARY KEY NOT NULL,
                transaction_key CHAR(50) NOT NULL,
                household_key CHAR(50) NOT NULL,
                spend_amount REAL);
            ''')
print "Table created successfully"

conn.commit()
conn.close()

# Load data
def importFromCsv(conn, fname, table):
    with open(fname) as inf:
        conn.cursor().copy_from(inf, table)

def main():

    importFromCsv(conn, "c:/myfile.csv", "MyTable")
    print("Data copied")
    

# Query data
cur = conn.cursor()
cur.execute("SELECT id, name, address, salary  from COMPANY")
records = cur.fetchall()

# Save query result in CSV file
with open('result.csv', 'w') as f:
    writer = csv.writer(f, delimiter=';')
    for row in records:
        writer.writerow(row)

print "Done Writing"
