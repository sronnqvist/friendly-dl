#LANG=Finnish
LANG=Swedish #$1
PACK_NR=1 #$2
OUTPUT_NAME=$LANG.$PACK_NR

zgrep "###C:warc-target-uri" store/conll_raw/$LANG/commoncrawl/filtered_text/text_$PACK_NR.gz > urls/$OUTPUT_NAME

python3 friendly_dl.py --dnscache store/cache/$OUTPUT_NAME --download store/html/$OUTPUT_NAME --r404 store/not_found/$OUTPUT_NAME urls/$OUTPUT_NAME

