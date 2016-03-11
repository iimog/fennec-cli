#!/usr/bin/python

import sys
import json
import psycopg2
from dns.rdatatype import NULL

try:
    conn = psycopg2.connect(dbname='test', user='test', host='127.0.0.1', password='test', port=54322)
except:
    print "I am unable to connect to the database"

def get_or_insert_cvterm(name, definition):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trait_cvterm_id FROM trait_cvterm WHERE definition=%s", (definition,))
            rows = cur.fetchall()
            if (len(rows) < 1):
                #print "cvterm '"+cvterm+"' missing - inserting entry"
                cur.execute("INSERT INTO trait_cvterm (name, definition) VALUES (%s, %s) RETURNING trait_cvterm_id", (name, definition))
                rows = cur.fetchall()
    return rows[0][0]

# Setup top level cv-terms
top_level_cvterms = {"dwc:measurementType": 0,
                     "dwc:measurementValue": 0,
                     "source": 0,
                     "measurement remarks": 0,
                     "citation": 0,
                     "measurement method": 0,
                     "contributor": 0,
                     "dwc:measurementUnit": 0,
                     "eolterms:statisticalMethod": 0
                     }

for cvterm in top_level_cvterms.keys():
    top_level_cvterms[cvterm] = get_or_insert_cvterm(cvterm, cvterm)

# Get EOL db id for mapping of eol id to organism_id
with conn:
    with conn.cursor() as cur:
        cur.execute("""SELECT db_id FROM db WHERE name='EOL'""")
        rows = cur.fetchall()
        if (len(rows) < 1):
            print "Error no db EOL - so mapping from EOL IDs to organism_ids not possible"
        eol_dbid = rows[0][0]

with open(sys.argv[1]) as f:
    file_content = f.read()

data = json.loads(file_content)
for element in data["@graph"]:
    if("@type" in element and element["@type"] == "dwc:MeasurementOrFact"):
        cur = conn.cursor()
        # get organism_id from db (skip if not exists)
        eolid = element["dwc:taxonID"].split("/")[-1]
        cur.execute("""SELECT organism_id FROM organism_dbxref od, dbxref WHERE od.dbxref_id = dbxref.dbxref_id AND dbxref.db_id="""
                    +str(eol_dbid)+""" AND dbxref.accession = '"""+str(eolid)+"""'""")
        rows = cur.fetchall()
        if (len(rows) < 1):
            print "No organism id found for eol id "+str(eolid)+" - skipping for now should be inserted as new organism in the future"
            continue
        organism_id = rows[0][0]

        type_cvterm_id = get_or_insert_cvterm(element["dwc:measurementType"]["rdfs:label"]["en"], element["dwc:measurementType"]["@id"])
        try:
            value = float(element["dwc:measurementValue"])
            cur.execute("INSERT INTO trait_entry (organism_id, type_cvterm_id, value, tb_id, data_point_uri_id) VALUES (%s, %s, %s, %s, %s) RETURNING trait_entry_id",
                        (organism_id, type_cvterm_id, value, element["@id"], element["data_point_uri_id"]))
            rows = cur.fetchall()
        except (TypeError):
            # The value is an object
            value_cvterm_id = get_or_insert_cvterm(element["dwc:measurementValue"]["rdfs:label"]["en"], element["dwc:measurementValue"]["@id"])
            cur.execute("INSERT INTO trait_entry (organism_id, type_cvterm_id, value_cvterm_id, tb_id, data_point_uri_id) VALUES (%s, %s, %s, %s, %s) RETURNING trait_entry_id",
                        (organism_id, type_cvterm_id, value_cvterm_id, element["@id"], element["data_point_uri_id"]))
            rows = cur.fetchall()
        except (ValueError):
            # The value is a string
            value_cvterm_id = get_or_insert_cvterm(None, element["dwc:measurementValue"])
            cur.execute("INSERT INTO trait_entry (organism_id, type_cvterm_id, value_cvterm_id, tb_id, data_point_uri_id) VALUES (%s, %s, %s, %s, %s) RETURNING trait_entry_id",
                        (organism_id, type_cvterm_id, value_cvterm_id, element["@id"], element["data_point_uri_id"]))
            rows = cur.fetchall()
        trait_entry_id = rows[0][0]
        for cvterm in top_level_cvterms.keys():
            if(cvterm == "dwc:measurementType" or cvterm == "dwc:measurementValue"):
                continue
            if(cvterm in element):
                if(type(element[cvterm]) is dict):
                    secondary_cvterm_id = get_or_insert_cvterm(element[cvterm]["rdfs:label"]["en"], element[cvterm]["@id"])
                else:
                    secondary_cvterm_id = get_or_insert_cvterm("", element[cvterm])
                cur.execute("""INSERT INTO trait_metadata (trait_entry_id, subject_cvterm_id, value_cvterm_id) VALUES (%s, %s, %s) RETURNING trait_metadata_id""",
                            (trait_entry_id, top_level_cvterms[cvterm], secondary_cvterm_id))
conn.commit()