#!/usr/bin/python3

import sys
import psycopg2
import csv
from optparse import OptionParser

version = "0.4.0"
usage = "Usage: %prog [options] --trait-type=TRAIT_TYPE --user-id=USER_ID FILE.tsv\n"
usage += "The tsv file has to have the following columns:\n"
usage += "fennec_id\tvalue\tvalue_ontology\tcitation\torigin_url\tprivate\tcreation_date\tdeletion_date\n"
usage += "only the first two are required"
parser = OptionParser(version=version, usage=usage)
parser.add_option("-t", "--trait-type", dest="trait_type", help="type of trait to import", type="string")
parser.add_option("-u", "--user-id", dest="user_id", help="ID of the user importing the data", type="int")
parser.add_option("-p", "--public", dest="private", help="import traits as public (default is private)",
 action="store_false", default=True)
parser.add_option("--db-user", dest="db_user", help="database user (default: %default)", default='fennec')
parser.add_option("--db-password", dest="db_pw", help="database password (default: %default)", default='fennec')
parser.add_option("--db-name", dest="db_name", help="database name (default: %default)", default='fennec')
parser.add_option("--db-host", dest="db_host", help="database host (default: %default)", default='localhost')
parser.add_option("--db-port", dest="db_port", help="database port (default: %default)", default='5432')

(options, args) = parser.parse_args()

if not options.trait_type:
    parser.error('No trait type given. Use --trait-type')
if not options.user_id:
    parser.error('No user ID given. Use --user-id')
if len(args) < 1:
    parser.error('No file for import given. Please provide at least one tsv file')

try:
    conn = psycopg2.connect(dbname=options.db_name, user=options.db_user, host=options.db_host, password=options.db_pw, port=options.db_port, connect_timeout=3)
except:
    print("I am unable to connect to the database")
    sys.exit(1)

def get_trait_type_id():
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM trait_type WHERE type=%s", (options.trait_type,))
            rows = cur.fetchall()
            if (len(rows) < 1):
                return -1
    return rows[0][0]

trait_type_id = get_trait_type_id()

if trait_type_id == -1:
    print("This trait type does not exist in the database. Please create it there")
    sys.exit(1)

def get_or_insert_trait_categorical_value(value, ontology_url):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM trait_categorical_value WHERE value=%s AND ontology_url=%s AND trait_type_id=%s", (value, ontology_url, trait_type_id))
            rows = cur.fetchall()
            if (len(rows) < 1):
                print("value '"+value+"' missing - inserting entry")
                cur.execute("INSERT INTO trait_categorical_value (value, ontology_url, trait_type_id) VALUES (%s, %s, %s) RETURNING id", (value, ontology_url, trait_type_id))
                rows = cur.fetchall()
    return rows[0][0]

def get_or_insert_trait_citation(citation):
    if citation is None:
        return None
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM trait_citation WHERE citation=%s", (citation,))
            rows = cur.fetchall()
            if (len(rows) < 1):
                print("citation '"+citation+"' missing - inserting entry")
                cur.execute("INSERT INTO trait_citation (citation) VALUES (%s) RETURNING id", (citation,))
                rows = cur.fetchall()
    return rows[0][0]

def insert_trait_categorical_entry(row):
    # convert all empty strings to None
    rowNull = list()
    for i in row:
        if i == '':
            rowNull.append(None)
        else:
            rowNull.append(i)
    (fennec_id, value, ontology_url, citation, origin_url, private, creation_date, deletion_date) = rowNull
    trait_categorical_value_id = get_or_insert_trait_categorical_value(value, ontology_url)
    trait_citation_id = get_or_insert_trait_citation(citation)
    if private is None:
        private = options.private
    with conn:
        with conn.cursor() as cur:
            if creation_date is None:
                cur.execute("INSERT INTO trait_categorical_entry (trait_type_id, fennec_id, trait_categorical_value_id, webuser_id, private, trait_citation_id, origin_url, deletion_date) "+
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                  (trait_type_id, fennec_id, trait_categorical_value_id, options.user_id, private, trait_citation_id, origin_url, deletion_date))
            else:
                cur.execute("INSERT INTO trait_categorical_entry (trait_type_id, fennec_id, trait_categorical_value_id, webuser_id, private, trait_citation_id, origin_url, creation_date, deletion_date) "+
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (trait_type_id, fennec_id, trait_categorical_value_id, options.user_id, private, trait_citation_id, origin_url, creation_date, deletion_date))

with open(args[0]) as csvfile:
    reader = csv.reader(csvfile, delimiter="\t")
    for row in reader:
        insert_trait_categorical_entry(row)
