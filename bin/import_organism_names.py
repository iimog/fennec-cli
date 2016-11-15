#!/usr/bin/python3

import sys
import psycopg2
import csv
from optparse import OptionParser

version = "0.1"
usage = "Usage: %prog [options] FILE.tsv\n"
usage += "The tsv file has to have the following columns:\n"
usage += "fennec_id\tname\ttype\n"
usage += "the fennec id must exist in the database and the name may not be empty\n"
usage += "the type is created if it does not exist (e.g. synonym, common name, etc.)"
parser = OptionParser(version=version, usage=usage)
parser.add_option("--db-user", dest="db_user", help="database user (default: %default)", default='fennec')
parser.add_option("--db-password", dest="db_pw", help="database password (default: %default)", default='fennec')
parser.add_option("--db-name", dest="db_name", help="database name (default: %default)", default='fennec')
parser.add_option("--db-host", dest="db_host", help="database host (default: %default)", default='localhost')
parser.add_option("--db-port", dest="db_port", help="database port (default: %default)", default='5432')

(options, args) = parser.parse_args()

if len(args) < 1:
    parser.error('No file for import given. Please provide at least one tsv file')

try:
    conn = psycopg2.connect(dbname=options.db_name, user=options.db_user, host=options.db_host, password=options.db_pw, port=options.db_port, connect_timeout=3)
except:
    print("I am unable to connect to the database")
    sys.exit(1)

name_types = dict()
def get_or_insert_name_type(name_type):
    if name_type in name_types:
        return name_types[name_type]
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name_type_id FROM name_type WHERE name_type=%s", (name_type,))
            rows = cur.fetchall()
            if (len(rows) < 1):
                print("Creating type " + name_type)
                cur.execute("INSERT INTO name_type (name_type) VALUES (%s) RETURNING name_type_id", (name_type,))
                rows = cur.fetchall()
            name_types[name_type] = rows[0][0]
    return name_types[name_type]

def insert_alternative_name(fennec_id, name, name_type):
    name_type_id = get_or_insert_name_type(name_type)
    with conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO alternative_name (fennec_id, name, name_type_id) VALUES (%s, %s, %s) RETURNING alternative_name_id", (fennec_id, name, name_type_id))
            rows = cur.fetchall()
    return rows[0][0]

with open(args[0]) as csvfile:
    reader = csv.reader(csvfile, delimiter="\t")
    for row in reader:
        (fennec_id, name, name_type) = row
        insert_alternative_name(fennec_id, name, name_type)
