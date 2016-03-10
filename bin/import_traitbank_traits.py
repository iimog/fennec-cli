#!/usr/bin/python

import sys
import json
import psycopg2

try:
    conn = psycopg2.connect(dbname='test', user='test', host='127.0.0.1', password='test', port=54322)
except:
    print "I am unable to connect to the database"

# Setup cv eol_traitbank
with conn:
    with conn.cursor() as cur:
        cur = conn.cursor()
        cur.execute("SELECT cv_id FROM cv WHERE name='EOL TraitBank'")
        rows = cur.fetchall()
        if (len(rows) < 1):
            print "No EOL TraitBank cv - inserting entry"
            cur.execute("INSERT INTO cv (name) VALUES ('EOL TraitBank') RETURNING cv_id")
            rows = cur.fetchall()
        eol_traitbank_cvid = rows[0][0]

# Setup db eol_traitbank
with conn:
    with conn.cursor() as cur:
        cur = conn.cursor()
        cur.execute("SELECT db_id FROM db WHERE name='EOL TraitBank'")
        rows = cur.fetchall()
        if (len(rows) < 1):
            print "No EOL TraitBank db - inserting entry"
            cur.execute("INSERT INTO db (name) VALUES ('EOL TraitBank') RETURNING db_id")
            rows = cur.fetchall()
        eol_traitbank_dbid = rows[0][0]

def get_or_insert_dbxref(cvterm, dbid):
    #cvterm name is varchar(255)
    cvterm = cvterm[0:255]
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT dbxref_id FROM dbxref WHERE accession=%s AND db_id=%s", (cvterm,dbid))
            rows = cur.fetchall()
            if (len(rows) < 1):
                print "dbxref '"+cvterm+"' missing - inserting entry"
                # I have to reset the cvterm_id serial generator - don't know why this is necessary
                #cur.execute("""SELECT setval('dbxref_dbxref_id_seq', (SELECT MAX(dbxref_id) FROM dbxref)+1)""")
                cur.execute("""INSERT INTO dbxref (db_id, accession) VALUES ("""+str(dbid)+""",'"""+cvterm+"""') RETURNING dbxref_id""")
                rows = cur.fetchall()
    return rows[0][0]

def get_or_insert_cvterm(cvterm, cvid, dbid):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT cvterm_id FROM cvterm WHERE name='"""+cvterm+"""' AND cv_id="""+str(cvid))
            rows = cur.fetchall()
            if (len(rows) < 1):
                # First insert dbxref
                dbxref_id = get_or_insert_dbxref(cvterm, dbid)
                # Now insert cvterm
                print "cvterm '"+cvterm+"' missing - inserting entry"
                # I have to reset the cvterm_id serial generator - don't know why this is necessary
                cur.execute("""SELECT setval('cvterm_cvterm_id_seq', (SELECT MAX(cvterm_id) FROM cvterm)+1)""")
                cur.execute("""INSERT INTO cvterm (cv_id, dbxref_id, name) VALUES ("""+str(cvid)+","+str(dbxref_id)+""",'"""+cvterm+"""') RETURNING cvterm_id""")
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
    top_level_cvterms[cvterm] = get_or_insert_cvterm(cvterm, eol_traitbank_cvid, eol_traitbank_dbid)

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

        type_cvterm_id = get_or_insert_cvterm(element["dwc:measurementType"]["@id"], eol_traitbank_cvid, eol_traitbank_dbid)
        try:
            value = float(element["dwc:measurementValue"])
            cur.execute("""INSERT INTO trait_entry (organism_id, type_cvterm_id, value) VALUES ("""+str(organism_id)+","+str(type_cvterm_id)+""","""+str(value)+""") RETURNING trait_entry_id""")
            rows = cur.fetchall()
        except (TypeError):
            # The value is an object
            value_cvterm_id = get_or_insert_cvterm(element["dwc:measurementValue"]["@id"], eol_traitbank_cvid, eol_traitbank_dbid)
            cur.execute("""INSERT INTO trait_entry (organism_id, type_cvterm_id, value_cvterm_id) VALUES ("""+str(organism_id)+","+str(type_cvterm_id)+""","""+str(value_cvterm_id)+""") RETURNING trait_entry_id""")
            rows = cur.fetchall()
        except (ValueError):
            # The value is a string
            value_cvterm_id = get_or_insert_cvterm(element["dwc:measurementValue"], eol_traitbank_cvid, eol_traitbank_dbid)
            cur.execute("""INSERT INTO trait_entry (organism_id, type_cvterm_id, value_cvterm_id) VALUES ("""+str(organism_id)+","+str(type_cvterm_id)+""","""+str(value_cvterm_id)+""") RETURNING trait_entry_id""")
            rows = cur.fetchall()
        trait_entry_id = rows[0][0]
        for cvterm in top_level_cvterms.keys():
            if(cvterm == "dwc:measurementType" or cvterm == "dwc:measurementValue"):
                continue
            if(cvterm in element):
                if(type(element[cvterm]) is dict):
                    secondary_cvterm_id = get_or_insert_cvterm(element[cvterm]["@id"], eol_traitbank_cvid, eol_traitbank_dbid)
                else:
                    secondary_cvterm_id = get_or_insert_cvterm(element[cvterm], eol_traitbank_cvid, eol_traitbank_dbid)
                cur.execute("""INSERT INTO trait_metadata (trait_entry_id, subject_cvterm_id, value_cvterm_id) VALUES ("""+str(trait_entry_id)+","+str(top_level_cvterms[cvterm])+""","""+str(secondary_cvterm_id)+""") RETURNING trait_metadata_id""")
conn.commit()