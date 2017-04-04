-- use on VM
--register /home/mpcs53013/pig-0.15.0-src/piggybank-0.16.0.jar;

--use on cloud
register /usr/local/pig/piggybank.jar;

--Loads csv file with player last name, first name, and retrosheet.org id
-- VM
--RAW_IDS = LOAD '/inputs/baseball/player_ids/player_names_and_ids.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER'); 
--cloud
RAW_IDS = LOAD '/inputs/jakecoll/baseball/player_ids/player_names_and_ids.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER');
ID_TO_NAME_FOR_BATTER = FOREACH RAW_IDS GENERATE CONCAT($1, ' ',$0) as batter_name, $2 as batter_id;
ID_TO_NAME_FOR_PITCHER = FOREACH RAW_IDS GENERATE CONCAT($1, ' ',$0) as pitcher_name, $2 as pitcher_id;
-- load play by play data by year passed as parameter Ex: pig -param year=2015 batter_v_pitcher.pigls
-- VM
--RAW_DATA = LOAD '/inputs/baseball/$year/*' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE','NOCHANGE');
-- cloud
RAW_DATA = LOAD '/inputs/jakecoll/baseball/$year/*' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE','NOCHANGE');
RAW_BATTER_DATA = FOREACH RAW_DATA GENERATE $10 as batter, $14 as pitcher, $34 as eventType, $35 as batterEvent, $36 as abFlag, $43 as RBIValue;

--filter out play by play that did not end batter's plate appearance (e.g. if data is for stolen base or pickoff it isn't needed)
FILTER_DATA_BY_BATTER_EVENT = FILTER RAW_BATTER_DATA BY (batterEvent matches 'T');
--join by batter_id
DATA_WITH_BATTER_NAME = JOIN FILTER_DATA_BY_BATTER_EVENT BY batter, ID_TO_NAME_FOR_BATTER BY batter_id;
DATA_WITH_NAMES = JOIN DATA_WITH_BATTER_NAME BY pitcher, ID_TO_NAME_FOR_PITCHER BY pitcher_id;
STATS_BY_PLATE_APPEARANCE = FOREACH DATA_WITH_NAMES GENERATE
	batter_name, pitcher_name, 
	(batterEvent matches 'T' ? 1 : 0) AS plateAppearance,
	(abFlag matches 'T' ? 1 : 0) AS atBat,
	(eventType == '3' ? 1 : 0) AS strikeout,
	(eventType == '14' ? 1 : 0) AS walked,
	(eventType == '15' ? 1 : 0) AS intentionalWalk,
	(eventType == '16' ? 1 : 0) AS hitByPitch,
	(eventType == '20' ? 1 : 0) AS single,
	(eventType == '21' ? 1 : 0) AS doubl,
	(eventType == '22' ? 1 : 0) AS triple,
	(eventType == '23' ? 1 : 0) AS homer;

-- create tuple of batter-pitcher matchup
BATTER_V_PITCHER = GROUP STATS_BY_PLATE_APPEARANCE BY (batter_name, pitcher_name);
SUMMED_BATTER_V_PITCHER = FOREACH BATTER_V_PITCHER GENERATE
	CONCAT(group.batter_name, group.pitcher_name) AS matchup,
	SUM($1.plateAppearance) AS PA, SUM($1.atBat) AS AB, SUM($1.strikeout) as K,
	SUM($1.walked) as walk, SUM($1.intentionalWalk) AS IW, SUM($1.hitByPitch) as HBP,
	SUM($1.single) as h1B, SUM($1.doubl) as h2B, SUM($1.triple) as h3B, SUM($1.homer) as HR;

--DUMP SUMMED_BATTER_V_PITCHER;	
STORE SUMMED_BATTER_V_PITCHER INTO 'hbase://jakecoll_batter_v_pitcher_$year'
	USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
	'stats:PA, stats:AB, stats:K, stats:walk, stats:IW, stats:HBP, stats:h1B, stats:h2B, stats:h3B, stats:HR');