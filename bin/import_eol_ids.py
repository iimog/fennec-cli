#!/usr/bin/python

import psycopg2
import sys

with open(sys.argv[1]) as f:
    content = f.readlines();

try:
    conn = psycopg2.connect(dbname='chado', user='fennec', host='127.0.0.1', password='fennec', port=54321)
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
cur.execute("""SELECT db_id FROM db WHERE name='DB:NCBI_taxonomy'""")
rows = cur.fetchall()
if (len(rows) < 1):
    print "Error no db DB:NCBI_taxonomy"
ncbi_taxonomy_dbid = rows[0][0]
cur.execute("""SELECT db_id FROM db WHERE name='EOL'""")
rows = cur.fetchall()
if (len(rows) < 1):
    print "No EOL db - inserting entry"
    cur.execute("""INSERT INTO db (name, description, urlprefix, url) VALUES ('EOL', 'Encyclopedia of Life', 'http://eol.org/pages/', 'http://eol.org') RETURNING db_id""")
    rows = cur.fetchall()
eol_dbid = rows[0][0]

for line in content:
    line = line.rstrip()
    [ncbi_id, eol_id] = line.split("\t")
    # get organism_id by ncbi taxid
    statement = """SELECT organism_id FROM organism_dbxref od, dbxref WHERE od.dbxref_id = dbxref.dbxref_id AND dbxref.db_id = """+str(ncbi_taxonomy_dbid)+""" AND dbxref.accession = '"""+str(ncbi_id)+"""'"""
    #print statement
    cur.execute(statement)
    rows = cur.fetchall()
    if(len(rows) < 1):
        print "No entry with NCBI taxid "+str(ncbi_id)+" in db. Skipping..."
    else:
        organism_id = rows[0][0]
        # insert eol id as dbxref unless exists
        statement = """INSERT INTO dbxref (db_id, accession)
SELECT """+str(eol_dbid)+""", '"""+str(eol_id)+"""'
WHERE
    NOT EXISTS (
        SELECT db_id FROM dbxref WHERE db_id = """+str(eol_dbid)+""" AND accession = '"""+str(eol_id)+"""'
    );"""
        cur.execute(statement)
        cur.execute("""SELECT dbxref_id FROM dbxref WHERE db_id = """+str(eol_dbid)+""" AND accession = '"""+str(eol_id)+"""'""")
        rows = cur.fetchall()
        dbxref_id = rows[0][0]
        # insert organism_dbxref entry unless exists
        statement = """INSERT INTO organism_dbxref (organism_id, dbxref_id)
SELECT """+str(organism_id)+""", """+str(dbxref_id)+"""
WHERE
    NOT EXISTS (
        SELECT organism_id FROM organism_dbxref WHERE organism_id = """+str(organism_id)+""" AND dbxref_id = """+str(dbxref_id)+"""
    );"""
        cur.execute(statement)
  
conn.commit()
conn.close()
#print "INSERT INTO organism_dbxref (organism_id, dbxref_id)
#VALUES ('\''$F[2]'\'', (SELECT dbxref_id FROM dbxref WHERE db_id = 180 AND accession = '\''$F[1]'\''));\n\n"