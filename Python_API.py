#!/usr/bin/python

import psycopg2
import json
import csv
import os
import sys

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
                categorie CHAR(50),
                transaction_key CHAR(50) NOT NULL,
                household_key CHAR(20) NOT NULL,
                spend_amount REAL,
                transaction_date date);
            ''')
print "Table created successfully"
conn.commit()


# Load data
def importFromCsv(conn, fname, table):
    with open(fname) as inf:
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.copy_from(inf, table)
            cursor.commit()
        except psycopg2.DatabaseError, e:
            if cursor:
                cursor.rollback()
            print 'Error %s' % e    
                sys.exit(1)
        finally:
            if cursor:
                cursor.close()
        inf.close()

list_file = [i for i in os.listdir(conf_file['path']) if os.path.isfile(i)]
for f in list_file:
    importFromCsv(conn, os.path.join(conf_file['path'], f), conf_file['table'])
    print("%s data copied" % (f))
    

# Query data
cur = conn.cursor()
cur.execute("SELECT categorie,
                    COUNT(DISTINCT household_key) AS Nb_client,
                    COUNT(DISTINCT transaction_key) AS Nb_trx ,
                    SUM(spend_amount) AS CA
                    from COMPANY
                    where transaction_date BETWEEN %s AND %s 
                    GROUP BY categorie" % (conf_file[date_debut], conf_file[date_fin]))
records = cur.fetchall()

# Save query result in CSV file
with open('result.csv', 'w') as f:
    writer = csv.writer(f, delimiter=';')
    for row in records:
        writer.writerow(row)

print "Done Writing"

# Close connexion
conn.close()
