-- CREATING HIVE TABLE FOR u.data FILE

CREATE TABLE `udata`(
  `userid` int, 
  `movieid` int, 
  `rating` int, 
  `timestamp` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/udata'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}', 
  
  
-- LOAD DATA INTO TABLE 
LOAD DATA LOCAL INPATH '${env:HOME}/YOURFILEPATH' 
OVERWRITE INTO TABLE udata
 
 
-- CREATING HIVE TABLE FOR u.item FILE
  CREATE TABLE `u`(
  `movieid` int, 
  `title` string, 
  `releasedate` string, 
  `imdblink` string, 
  `column5` string, 
  `column6` int, 
  `column7` int, 
  `column8` int, 
  `column9` int, 
  `column10` int, 
  `column11` int, 
  `column12` int, 
  `column13` int, 
  `column14` int, 
  `column15` int, 
  `column16` int, 
  `column17` int, 
  `column18` int, 
  `column19` int, 
  `column20` int, 
  `column21` int, 
  `column22` int, 
  `column23` int, 
  `column24` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/u'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='{\"BASIC_STATS\":\"true\"}', 
  'numFiles'='1', 
  'numRows'='1682', 
  'rawDataSize'='854456', 
  'totalSize'='52061', 
  'transient_lastDdlTime'='1599989951')


-- LOAD DATA INTO TABLE 
LOAD DATA LOCAL INPATH '${env:HOME}/YOURFILEPATH' 
OVERWRITE INTO TABLE u
 
-- UUUUPS, NAMED SECOND TABLE u INSTEAD OF uitem, RENAMING
ALTER TABLE u RENAME TO uitem;


-- CREATE VIEW AND RETURN (of the Jedi, ups, sorry) THE MOST RATED MOVIE 
CREATE VIEW topMovieIDs AS 
SELECT movieID, count(movieID) as ratingCount 
FROM udata
GROUP BY movieID
ORDER BY ratingCount desc;


SELECT uitem.title, ratingCount
FROM topMovieIDs t 
JOIN uitem ON t.movieID=uitem.movieID;

DROP VIEW topMovieIDs;


-- MANAGED VS EXTERNAL TABLES 

-- create external table 

CREATE EXTERNAL TABLE IF NOT EXISTS yourtablename (
columnname INT,
columnname2 INT,
etc etc) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  -- for tabular files 
LOCATION '/yourfilepath/u.data' 
-- CAN ADD PARTITION 
PARTITIONED BY (yourcolumnname STRING);







