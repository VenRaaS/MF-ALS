#!/bin/bash
export BQ_DB="nono_tmp"
export DATE_BEGIN="20170101"
export DATE_END="20170107"

## create training data
read -r -d '' BQ_SQL << EOM
SELECT uid, gid, LOG(1+count(qty),2) rating
FROM \`nono_Unima.orderlist_*\`
WHERE 
  ( _TABLE_SUFFIX BETWEEN '$DATE_BEGIN' AND '$DATE_END' )  
  and uid IS NOT NULL and uid <> ''
  and gid IS NOT NULL and gid <> ''
group by uid, gid
EOM
export TB_TRAIN="$BQ_DB.als_user_order_R"
bq query --destination_table=$TB_TRAIN --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_TRAIN"

## create als_mapping table - uid
read -r -d '' BQ_SQL << EOM
SELECT uid, ROW_NUMBER() OVER () row_num
FROM $TB_TRAIN
group by uid
EOM
export TB_MAP_UID="$BQ_DB.als_map_uid2Num"
bq query --destination_table=$TB_MAP_UID --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_MAP_UID"

## create als_mapping table - gid
read -r -d '' BQ_SQL << EOM
SELECT gid, ROW_NUMBER() OVER () row_num
FROM $TB_TRAIN
group by gid
EOM
export TB_MAP_GID="$BQ_DB.als_map_gid2Num"
bq query --destination_table=$TB_MAP_GID --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_MAP_GID"

## create userItemRating3_R
read -r -d '' BQ_SQL << EOM
select u.row_num userId, g.row_num itemId, rating
from $TB_TRAIN r
  join $TB_MAP_GID g
    on r.gid = g.gid
  join $TB_MAP_UID u
    on r.uid = u.uid
EOM
export TB_RATE_R="$BQ_DB.als_userItemRating5_R"
bq query --destination_table=$TB_RATE_R --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_RATE_R"

## export csv.gz
# export TB_RATE_R="nono_tmp.als_userItemRating3_R"
bq extract --compression=GZIP $TB_RATE_R gs://venraas_unima_us/$TB_RATE_R*.csv.gz

