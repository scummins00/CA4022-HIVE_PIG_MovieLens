/*
To re-run this script we must ensure the previous output is deleted
*/

fs -rm -r output/cleaned_tags

/*
Similar to ratings cleaning
Step 1: Read in file using CSVLoader
Step 2: Remove header
Step 3: Remove quotation marks from tags
Step 4: Group user and movies to create bag of tags and timestamps
Step 5: Add column that shows length of tag applied for future analysis
Step 6: Convert all tags to lowercase
Step 7: Store file using '\t'
*/

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
in_tags = LOAD 'data/tags.csv' USING CSVLoader() AS (userid: int, movieid: int, tag:chararray, timestamp:int);

no_header = FILTER in_tags BY NOT (tag == 'tag');

no_quote = FOREACH no_header GENERATE userid, movieid, REPLACE(tag, '"', '') AS tag, timestamp;

str_len = FOREACH no_quote GENERATE userid, movieid, tag, SIZE(tag) AS length, timestamp;

lower = FOREACH str_len GENERATE userid, movieid, LOWER(tag) as tag, length, timestamp;

grouped = GROUP lower BY (userid, movieid);

fin = FOREACH grouped GENERATE group.userid AS userid, 
	group.movieid AS movieid, SUM(lower.length) AS taglength,
	lower.tag AS tags, lower.timestamp AS timestamps;
finn = FOREACH fin {
	tmp = BagToTuple(tags);
	GENERATE userid, movieid, tmp AS tags, taglength;
}

STORE finn INTO 'output/cleaned_tags' USING PigStorage();

