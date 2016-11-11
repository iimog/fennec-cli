#!/usr/bin/python3

import sys
import psycopg2
import csv
from optparse import OptionParser

version = "0.1"
usage = "Usage: %prog [options --description=DESCRIPTION] --provider=PROVIDER FILE.tsv\n"
usage += "The tsv file has to have the following columns:\n"
usage += "scientific_name\tidentifier\tfennec_id\n"
usage += "only the first two are required\n"
usage += "a new fennec id is created if none is given using the scientifc name\n"
usage += "if there is a fennec_id the scientific name is ignored\n"
usage += "the identifier has to be unique for the given provider"
parser = OptionParser(version=version, usage=usage)
parser.add_option("-p", "--provider", dest="provider", help="name of the provider")
parser.add_option("-d", "--description", dest="description", help="description of the provider", default='')
parser.add_option("--db-user", dest="db_user", help="database user (default: %default)", default='fennec')
parser.add_option("--db-password", dest="db_pw", help="database password (default: %default)", default='fennec')
parser.add_option("--db-name", dest="db_name", help="database name (default: %default)", default='fennec')
parser.add_option("--db-host", dest="db_host", help="database host (default: %default)", default='localhost')
parser.add_option("--db-port", dest="db_port", help="database port (default: %default)", default='5432')

(options, args) = parser.parse_args()

if not options.provider:
    parser.error('No provider given. Use --provider')
if len(args) < 1:
    parser.error('No file for import given. Please provide at least one tsv file')

try:
    conn = psycopg2.connect(dbname=options.db_name, user=options.db_user, host=options.db_host, password=options.db_pw, port=options.db_port, connect_timeout=3)
except:
    print("I am unable to connect to the database")
    sys.exit(1)

def get_or_insert_provider(provider, description):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT db_id FROM db WHERE name=%s", (provider,))
            rows = cur.fetchall()
            if (len(rows) < 1):
                print("Creating provider " + provider)
                cur.execute("INSERT INTO db (name, description) VALUES (%s, %s) RETURNING db_id", (provider, description))
                rows = cur.fetchall()
    return rows[0][0]

def insert_organism(sciname):
    with conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO organism (scientific_name) VALUES (%s) RETURNING fennec_id", (sciname,))
            rows = cur.fetchall()
    return rows[0][0]

provider_id = get_or_insert_provider(options.provider, options.description)

def insert_fennec_dbxref(fennec_id, identifier):
    with conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO fennec_dbxref (db_id, identifier, fennec_id) VALUES (%s, %s, %s)", (provider_id, identifier, fennec_id))


with open(args[0]) as csvfile:
    reader = csv.reader(csvfile, delimiter="\t")
    for row in reader:
        (sciname, identifier, fennec_id) = row
        if fennec_id == '':
            fennec_id = insert_organism(sciname)
        insert_fennec_dbxref(fennec_id, identifier)
