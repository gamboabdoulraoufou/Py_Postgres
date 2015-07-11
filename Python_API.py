#!/usr/bin/python

import psycopg2
import json
import csv
import os
import sys
import datetime
import csv

# Load data
def importFromCsv(conn, inpath, table):
    list_file = [i for i in os.listdir(inpath) if os.path.isfile(os.path.join(inpath,i))]
    if len(list_file)==0:
        print 'There is any file in %s' % (inpath)    
        sys.exit(1)
    else:
        for my_file in list_file:
            csv_data = csv.reader(open(os.path.join(inpath, my_file), 'r'), dialect = 'excel',  delimiter = ',') 
            passData = "INSERT INTO trx3 (quantity, spend_amount, period, hhk_code, trx_key_code, sub_code) VALUES (%s, %s,%s,%s,%s,%s,%s);" 
            cur = conn.cursor()
            for row in csv_data:  
                csvLine = row       
                cur.execute(passData, csvLine) 
            conn.commit()
            print ("%s data copied" % (my_file))


# Update table
def prepare_data(conn, table, date_debut):
    cur = conn.cursor()
    cur.execute('DELETE FROM %s WHERE transaction_date < %s', (table, date_debut))

    
def process_data(conn, conf, query, file_name):
    # Query data
    try:
        cur = conn.cursor()
        cur.execute(query)
        records = cur.fetchall()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
    finally:
        cur.close()
    # Save query result in CSV file
    with open(conf['outpath']+file_name+'.csv', 'w') as f:
        writer = csv.writer(f, delimiter=';')
        for row in records:
            writer.writerow(row)
    print "Done Writing"


def main():
    # Connecting To Database     
    try:
      conn = psycopg2.dbapi.connect(database="test_db", user="abdoul", password="1234", host="127.0.0.1", port="5432")
      print "Opened database successfully"
    except:
      print "Connexion wrong"
    
    # Get configuration file data
    with open('conf.json') as conf_file:    
        conf = json.load(conf_file)
        conf_file.close()

    # Prepare data
    try:    
        importFromCsv(conn, conf['inpath'], 'trx3')
        #prepare_data(conn, conf['table'], conf['date_debut'])
        # Query results
        process_data(conn, conf, equco, "result1")
        process_data(conn, conf, count, "result2")
    except:
        print "somthing wrong"

    # Close connexion
    conn.close()


equco="""SELECT period, sub_code,
                  COUNT (DISTINCT hhk_code) AS Nb_client,
                  COUNT (*) AS Nb_UVC, 
                  SUM(quantity) AS Nb_uvc,
                  SUM(spend_amount) AS CA 
          FROM trx3
          GROUP BY sub_code, period"""
                
count="SELECT COUNT(*) AS Nb FROM trx3"


if __name__=="__main__":
    main()
# http://stackoverflow.com/questions/14087853/python-psycopg2-logging-events
