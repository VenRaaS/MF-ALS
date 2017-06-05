#!/bin/bash
export BQ_DB="nono_tmp"
export TB_RATE_R="$BQ_DB.als_userItemRating4_R"

gsutil -m cp get gs://venraas_unima_us/$TB_RATE_R*.csv.gz .

for file in $TB_RATE_R*.gz; do gunzip -c "$file" >> $TB_RATE_R.csv; done
wc -l $TB_RATE_R.csv

