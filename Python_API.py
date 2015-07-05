#!/usr/bin/python

import psycopg2
import json
import csv
import os
import sys
import datetime

# Load data
def importFromCsv(conn, inpath, table):
    cur = None
    try:
        cur = conn.cursor()
        list_file = [i for i in os.listdir(inpath) if os.path.isfile(i)]
        for i in list_file:
            with open(os.path.join(inpath, i)) as inf:
                cur.copy_from(inf, table)
                cur.commit()
                print("%s data copied" % (i))
                inf.close()
    except psycopg2.DatabaseError, e:
        if cur:
            cur.rollback()
        print 'Error %s' % e    
            sys.exit(1)
    finally:
        if cur:
            cur.close()


# Update table
def prepare_data(conn, table, date_debut):
    cur = conn.cursor()
    cur.execute('DELETE FROM %s WHERE transaction_date < %s', (table, date_debut)
    
def process_data(conn, conf):
    # Query data
    try:
        cur = conn.cursor()
        cur.execute("""SELECT categorie,
                            COUNT(DISTINCT household_key) AS Nb_client,
                            COUNT(DISTINCT transaction_key) AS Nb_trx ,
                            SUM(spend_amount) AS CA
                            from COMPANY
                            where transaction_date BETWEEN %s AND %s 
                            GROUP BY categorie""" % (conf[date_debut], conf[date_fin]))
        records = cur.fetchall()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
    finally:
        cur.close()
        
    # Save query result in CSV file
    with open(conf['outpath']+'result.csv', 'w') as f:
        writer = csv.writer(f, delimiter=';')
        for row in records:
            writer.writerow(row)
    print "Done Writing"


def main():
    # Get configuration file data
    with open('conf.json') as conf_file:    
        conf = json.load(conf_file)
        conf_file.close()
    
    # Connecting To Database     
    try:
      conn = psycopg2.connect(database="testdb", user="postgres", password="123", host="127.0.0.1", port="5432")
      print "Opened database successfully"
    except:
      print "Connexion wrong"
    
    # Prepare data
    try:    
        importFromCsv(conn, conf['inpath'], conf['table'])
        prepare_data(conn, conf['table'], conf['date_debut'])
        # Query results
        process_data(conn, conf)
    except:
        print "somthing wrong"

    # Close connexion
    conn.close()
    
if __name__=="__main__":
    main()
# http://stackoverflow.com/questions/14087853/python-psycopg2-logging-events
