#!/bin/bash

set -o nounset
set -o errexit

DB_USER=fennectest
DB_PW=fennectest
DB_NAME=fennectest
DB_HOST=172.18.0.2
DB_PORT=5432

VERSION=$1
YEAR=${VERSION:0:4}

# create mapping file
PGPASSWORD=$DB_PW psql -At -F $'\t' -U $DB_USER -h $DB_HOST -d $DB_NAME -p $DB_PORT -c "SELECT fennec_id,identifier FROM fennec_dbxref WHERE db_id=(SELECT db_id FROM db WHERE name='iucn_redlist')" >fennec2iucn.tsv

# create input table
perl -F"\t" -ane 'BEGIN{open IN, "<fennec2iucn.tsv";while(<IN>){chomp;($f,$i)=split(/\t/);$i2f{$i}=$f}} chomp $F[11]; print "$i2f{$F[0]}\t$F[11]\thttp://www.iucnredlist.org/static/categories_criteria_2_3\tIUCN '$YEAR'. IUCN Red List of Threatened Species. Version '$VERSION' <www.iucnredlist.org>\thttp://apiv3.iucnredlist.org/api/v3/species/page/\t\t\t\n" if(exists $i2f{$F[0]})' IUCN-${VERSION}.tsv >iucn_traits.tsv

# get iucn webuser id
WEBUSER_ID=$(PGPASSWORD=$DB_PW psql -At -F $'\t' -U $DB_USER -h $DB_HOST -d $DB_NAME -p $DB_PORT -c "SELECT webuser_id FROM webuser WHERE oauth_id='iucn_redlist_api'")

# invalidate old iucn traits
PGPASSWORD=$DB_PW psql -U $DB_USER -h $DB_HOST -d $DB_NAME -p $DB_PORT -c "UPDATE trait_categorical_entry SET deletion_date=NOW() WHERE trait_type_id=(SELECT id FROM trait_type WHERE type = 'IUCN Threat Status') AND deletion_date IS NULL;"

# import trait table
python ~/projects/fennec-cli/bin/import_traits_tsv.py -t "IUCN Threat Status" -u $WEBUSER_ID -p --db-user $DB_USER --db-password $DB_PW --db-name $DB_NAME --db-host $DB_HOST --db-port $DB_PORT iucn_traits.tsv
