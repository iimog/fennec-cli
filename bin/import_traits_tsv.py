#!/usr/bin/python

import sys
from optparse import OptionParser

parser = OptionParser(version="0.1")
parser.add_option("-t", "--trait-type", dest="trait_type", help="type of trait to import", type="string")
parser.add_option("-u", "--user-id", dest="user_id", help="ID of the user importing the data", type="int")
parser.add_option("-p", "--private", dest="private", help="import traits as private (default is public)",
 action="store_true", default=False)

(options, args) = parser.parse_args()

print(options)