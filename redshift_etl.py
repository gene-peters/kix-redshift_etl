import csv
import pyhs2
from pyhs2.cursor import SSCursor
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import gzip
import psycopg2 
import re
import datetime
import time
import json
from multiprocessing import Pool

def get_max_id(field, table):
    '''Gets maximum value of field from table'''
    with open('pass.json', 'rb') as passfile:
        passdict = json.load(passfile)
    with psycopg2.connect(host=passdict['REDSHIFT_HOST'] , user = passdict['REDSHIFT_USER'], password = passdict['REDSHIFT_PASS'], database = 'dev', port = 5439) as conn:
        cur = conn.cursor()
        cur.execute("SELECT max(%s) from %s" % (field, table))
        result = cur.fetchone()
        return result if result is not None else 0

def hs2_pull_data(query, buffername, max_id, field):
    with open('pass.json', 'rb') as passfile:
        passdict = json.load(passfile)
    with pyhs2.connect(host=passdict['HIVE_HOST'],port=10000, authMechanism='PLAIN', user=passdict['HIVE_USER'], database='default', cursorclass=SSCursor) as conn:
        with gzip.open('/home/epeters/public/dumps/%s_redshift.gz' % (buffername), 'wb+') as gzip_csv_file:
            cur = conn.cursor()
            datacsv = csv.writer(gzip_csv_file, delimiter = '|', quotechar = '`', quoting = csv.QUOTE_MINIMAL)
            yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
            query = re.sub('@YEAR@', str(yesterday.year), query)
            query = re.sub('@MONTH@', str(yesterday.month), query)
            query = re.sub('@DAY@', str(yesterday.day), query)
            query = re.sub('@MAX_ID@', '%s > %s' % (max_id, field), query)
            #print "query is now:" 
            #print query   
            cur.execute(query)
            #print "Query execute successful"
            counter = 0
            for i in cur.fetch():
                counter += 1
                if counter % 10000 == 0:
                    print "counter now at %s" % (counter)
                datacsv.writerow(i)
        return cur.getSchema()

def rs_upsert_data(table, buffername, pkey=None, schema=None):
    with open('pass.json', 'rb') as passfile:
        passdict = json.load(passfile)
    with psycopg2.connect(host=passdict['REDSHIFT_HOST'] , user = passdict['REDSHIFT_USER'], password = passdict['REDSHIFT_PASS'], database = 'dev', port = 5439) as conn:
        cur = conn.cursor()
        s3_conn = S3Connection(passdict['AWS_ACCESS_KEY_ID'], passdict['AWS_SECRET_ACCESS_KEY'])
        bucket = s3_conn.get_bucket('kixanalytics-or')
        key = bucket.new_key('%s_redshift.gz' % (buffername))
        key.set_contents_from_filename('/home/epeters/public/dumps/%s_redshift.gz' % (buffername))
        key.close()
        #print "key uploaded"
        #split the name of the name of the table to remove schema -- temp tables can't have schemas attached
        try:
            table_noschema = table.split('.')[1]
        except IndexError:
            table_noschema = table
        update_statement = ["""
        BEGIN TRANSACTION;
        CREATE TEMP TABLE %s_stage (LIKE %s);
        COPY %s_stage (""" % (table_noschema, table, table_noschema)]
        
        for element in schema:
            update_statement.extend(["%s" % (element['columnName']), ","])
        update_statement = update_statement[:-1]
        update_statement.append(""") 
            FROM 's3://kixanalytics-or/%s_redshift.gz' 
            CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
            maxerror 2
            GZIP
            CSV
            DELIMITER as '|'
            QUOTE AS '`'
            BLANKSASNULL
            ACCEPTINVCHARS as '.'
            ROUNDEC;
            """ % (buffername, passdict['AWS_ACCESS_KEY_ID'], passdict['AWS_SECRET_ACCESS_KEY']))
        if pkey is not None:
            update_statement.append("DELETE FROM %s USING %s_stage WHERE " % (table, table_noschema))
            for pk in pkey:
                update_statement.append("%s.%s = %s_stage.%s" % (table, pk, table_noschema, pk))
                update_statement.append(" and ")
            update_statement = update_statement[:-1]
            update_statement.append(";")
        update_statement.append("""
        INSERT INTO %s SELECT * FROM %s_stage;
        DROP TABLE %s_stage;
        END TRANSACTION;
        """ % (table, table_noschema, table_noschema))
        print ''.join(update_statement)
        cur.execute(''.join(update_statement))
        conn.commit()

def process_worker(blob):
    qname, qbody = blob
    print "running query for %s" % (qname)
    max_id = get_max_id(qbody['id_field'], qbody['dest_table'])
    schema = hs2_pull_data(qbody['query'], qname, max_id, qbody['id_field'])
    print "data for %s pulled" % (qname)
    rs_upsert_data(qbody['dest_table'], qname, qbody.get('pkey'), schema)
    return True


def main():
    with open('queries.json') as queriesfile:
        queriesdict = json.load(queriesfile)
    workerpool = Pool(processes=8)
    workerpool.map(process_worker, queriesdict.items())
    workerpool.close()
    workerpool.join()


if __name__=="__main__":
    main()
