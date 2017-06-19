#!/bin/bash
export CODENAME="gohappy"
export BQ_DB=$CODENAME"_tmp"
export DATE_BEGIN="20170501"
export DATE_END="20170531"

## create training data
read -r -d '' BQ_SQL << EOM
WITH gid2validSess AS (
    SELECT
        gid, ven_session, COUNT(*) size_sess
    FROM         
        \`gohappy_unima.weblog_*\`
    WHERE
        ( _TABLE_SUFFIX BETWEEN '$DATE_BEGIN' AND '$DATE_END' )  
        AND page_type = 'gop'
        AND gid != ''
        AND gid IS NOT NULL
        AND ven_session != '' AND ven_session IS NOT NULL 
    GROUP BY gid,ven_session  
    HAVING 
        3 <= size_sess AND size_sess <= 50
)
SELECT
    lt_.gid AS gid,
    lt_.ven_session AS ven_session,
    rt_.gid AS gids
  FROM gid2validSess lt_
  INNER JOIN gid2validSess rt_
  ON
    lt_.ven_session = rt_.ven_session 
WHERE
  lt_.gid != rt_.gid
EOM
export TB_TRAIN="$BQ_DB.gid2sessionGids"
bq query --destination_table=$TB_TRAIN --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) num_lines from $TB_TRAIN"

## create userItemRating3_R
read -r -d '' BQ_SQL << EOM
SELECT
  gid,  gids,  rating
FROM (
  SELECT
    gid,
    gids,
    COUNT(DISTINCT ven_session) OVER (PARTITION BY gid, gids) AS rating
  FROM
    $TB_TRAIN )
GROUP BY
  gid,
  gids,
  rating
HAVING
  3 <= rating
EOM
export TB_RATE_R="$BQ_DB.als_gid2gid_rating_R_201705"
bq query --destination_table=$TB_RATE_R --nouse_legacy_sql --replace --allow_large_results=true $BQ_SQL
bq query "select count(*) num_ratings from $TB_RATE_R"

## export csv.gz
bq extract --compression=GZIP $TB_RATE_R gs://venraas-bq-temp/$TB_RATE_R*.csv.gz


