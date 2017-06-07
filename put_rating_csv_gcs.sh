#!/bin/bash
export BQ_DB="nono_tmp"
TB_RATE_R=$1

gsutil -m cp get gs://venraas_unima_us/$TB_RATE_R*.csv.gz .

# for file in $TB_RATE_R*.gz; do gunzip -c "$file" >> $TB_RATE_R.csv; done
# wc -l $TB_RATE_R.csv

for file in  nono_tmp.als_userItemRating4_R*.gz; do gunzip -c "$file" | hadoop fs -appendToFile - nono_tmp.als_userItemRating4_R.csv; done

hadoop fs -ls .

