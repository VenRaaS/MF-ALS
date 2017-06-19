#!/bin/bash
BQ_DB="gohappy_tmp"
TB_RATE_R=$1
GS_PATH="venraas_unima_us"

gsutil -m cp get gs://$GS_PATH/$TB_RATE_R*.csv.gz .

# for file in $TB_RATE_R*.gz; do gunzip -c "$file" >> $TB_RATE_R.csv; done
# wc -l $TB_RATE_R.csv

for file in $TB_RATE_R*.gz; do gunzip -c "$file" | hadoop fs -appendToFile - $TB_RATE_R.csv; done

hadoop fs -ls .

