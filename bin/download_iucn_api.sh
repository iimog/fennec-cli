#!/bin/bash

TOKEN=$(cat .iucn_token)

VERSION=$(curl "apiv3.iucnredlist.org/api/v3/version" | jq --raw-output '.version')
OUT_FILE=IUCN-${VERSION}.json

if [ -e $OUT_FILE ]
then
    echo "IUCN file for version $VERSION exists: $OUT_FILE ... nothing to do"
    exit 0
fi

GET_NEXT=true
PAGE="0"
TMP_PREFIX="/tmp/IUCN-${VERSION}-page-"

while [ "$GET_NEXT" = true ]
do
    echo "Downloading version $VERSION page $PAGE"
    TMP_FILE="${TMP_PREFIX}${PAGE}.json"
    curl "apiv3.iucnredlist.org/api/v3/species/page/${PAGE}?token=${TOKEN}" >$TMP_FILE
    PAGE=$((PAGE + 1))
    COUNT=$(jq '.count' ${TMP_FILE})
    if (( COUNT < 1 ))
    then
        GET_NEXT=false
    fi
done

cat ${TMP_PREFIX}*.json | jq '.result[]' >$OUT_FILE
echo "IUCN file for version $VERSION created: $OUT_FILE ... done"
