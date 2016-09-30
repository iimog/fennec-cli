#!/usr/bin/python

import sys
from optparse import OptionParser

version = "0.1"
usage = "Usage: %prog [options] --trait-type=TRAIT_TYPE --user-id=USER_ID FILE.tsv\n"
usage += "The tsv file has to have the following columns:\n"
usage += "ncbi_taxid\tvalue\tvalue_ontology\tcitation\torigin_url\tprivate\tcreation_date\tdeletion_date\n"
usage += "only the first two are required"
parser = OptionParser(version=version, usage=usage)
parser.add_option("-t", "--trait-type", dest="trait_type", help="type of trait to import", type="string")
parser.add_option("-u", "--user-id", dest="user_id", help="ID of the user importing the data", type="int")
parser.add_option("-p", "--public", dest="public", help="import traits as public (default is private)",
 action="store_true", default=False)
parser.add_option("--db-user", dest="db_user", help="database user (default: %default)", default='fennec')
parser.add_option("--db-password", dest="db_pw", help="database password (default: %default)", default='fennec')
parser.add_option("--db-name", dest="db_name", help="database name (default: %default)", default='fennec')
parser.add_option("--db-host", dest="db_host", help="database host (default: %default)", default='localhost')

(options, args) = parser.parse_args()

print(options)
print(args)