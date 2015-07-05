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
        cur = None
        try:
            cur = conn.cursor()
            cur.copy_from(inf, table)
            cur.commit()
        except psycopg2.DatabaseError, e:
            if cur:
                cur.rollback()
            print 'Error %s' % e    
                sys.exit(1)
        finally:
            if cur:
                cur.close()
        inf.close()

list_file = [i for i in os.listdir(conf_file['inpath']) if os.path.isfile(i)]
for f in list_file:
    importFromCsv(conn, os.path.join(conf_file['inpath'], f), conf_file['table'])
    print("%s data copied" % (f))
    

# Query data
try:
    cur = conn.cursor()
    cur.execute("SELECT categorie,
                        COUNT(DISTINCT household_key) AS Nb_client,
                        COUNT(DISTINCT transaction_key) AS Nb_trx ,
                        SUM(spend_amount) AS CA
                        from COMPANY
                        where transaction_date BETWEEN %s AND %s 
                        GROUP BY categorie" % (conf_file[date_debut], conf_file[date_fin]))
    records = cur.fetchall()
except psycopg2.DatabaseError, e:
    print 'Error %s' % e
finally:
    cur.close()
    
# Save query result in CSV file
with open(conf_file['outpath']+'result.csv', 'w') as f:
    writer = csv.writer(f, delimiter=';')
    for row in records:
        writer.writerow(row)

print "Done Writing"

# Close connexion
conn.close()
