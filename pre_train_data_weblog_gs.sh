#!/bin/bash
export BQ_DB="nono_tmp"
export DATE_BEGIN="20170101"
export DATE_END="20170228"

## create training data
read -r -d '' BQ_SQL << EOM
SELECT uid, gid, LOG(1+sum(rating),2) rating
FROM (
  SELECT _TABLE_SUFFIX day, uid, gid, LOG(1+count(*),2) rating
  FROM \`nono_Unima.weblog_*\`
  WHERE 
    ( _TABLE_SUFFIX BETWEEN '$DATE_BEGIN' AND '$DATE_END' )  
    and action = 'pageload'
    and page_type = 'gop'
    and uid IS NOT NULL and uid <> ''
    and gid IS NOT NULL and gid <> ''
  group by _TABLE_SUFFIX, uid, gid
) x
group by uid, gid
EOM
export TB_TRAIN="$BQ_DB.als_user_gop_R"
bq query --destination_table=$TB_TRAIN --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_TRAIN"

## create validation data
export DATE_BEGIN="20170301"
export DATE_END="20170330"
read -r -d '' BQ_SQL << EOM
SELECT uid, gid, LOG(1+sum(rating),2) rating
FROM (
  SELECT _TABLE_SUFFIX day, uid, gid, LOG(1+count(*),2) rating
  FROM \`nono_Unima.weblog_*\`
  WHERE 
    ( _TABLE_SUFFIX BETWEEN '$DATE_BEGIN' AND '$DATE_END' )  
    and action = 'pageload'
    and page_type = 'gop'
    and uid IS NOT NULL and uid <> ''
    and gid IS NOT NULL and gid <> ''
  group by _TABLE_SUFFIX, uid, gid
) x
group by uid, gid
EOM
export TB_VAL="$BQ_DB.als_user_gop_V"
bq query --destination_table=$TB_VAL --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_VAL"

## create als_mapping table - uid
read -r -d '' BQ_SQL << EOM
SELECT uid, ROW_NUMBER() OVER () row_num
FROM (
  select distinct uid FROM $TB_TRAIN
  UNION ALL
  select distinct uid FROM $TB_VAL
) x
group by uid
EOM
export TB_MAP_UID="$BQ_DB.als_map_uid2Num"
bq query --destination_table=$TB_MAP_UID --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_MAP_UID"

## create als_mapping table - gid
read -r -d '' BQ_SQL << EOM
SELECT gid, ROW_NUMBER() OVER () row_num
FROM (
  select distinct gid FROM $TB_TRAIN
  UNION ALL
  select distinct gid FROM $TB_VAL
) x
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
export TB_RATE_R="$BQ_DB.als_userItemRating4_R"
bq query --destination_table=$TB_RATE_R --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_RATE_R"

read -r -d '' BQ_SQL << EOM
select u.row_num userId, g.row_num itemId, rating
from $TB_VAL r
  join $TB_MAP_GID g
    on r.gid = g.gid
  join $TB_MAP_UID u
    on r.uid = u.uid
EOM
export TB_RATE_V="$BQ_DB.als_userItemRating4_V"
bq query --destination_table=$TB_RATE_V --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) from $TB_RATE_V"

## export csv.gz
# export TB_RATE_R="nono_tmp.als_userItemRating3_R"
# export TB_RATE_V="nono_tmp.als_userItemRating3_V"
bq extract --compression=GZIP $TB_RATE_R gs://venraas_unima_us/$TB_RATE_R*.csv.gz
bq extract --compression=GZIP $TB_RATE_V gs://venraas_unima_us/$TB_RATE_V*.csv.gz

