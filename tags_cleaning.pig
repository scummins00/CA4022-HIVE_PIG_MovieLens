/*
Similar to ratings cleaning
Step 1: Read in file using CSVLoader
Step 2: Remove header
Step 3: Remove quotation marks from tags
Step 4: Add column that shows length of tag applied for future analysis
Step 5: Convert all tags to lowercase
Step 6: Store file using '\t'
*/

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
in_tags = LOAD 'movie/tags.csv' USING CSVLoader() AS (userid: int, movieid: int, tag:chararray, timestamp:int);

no_header = FILTER in_tags BY NOT (tag == 'tag');

no_quote = FOREACH no_header GENERATE userid, movieid, REPLACE(tag, '"', '') AS tag, timestamp;

str_len = FOREACH no_quote GENERATE userid, movieid, tag, SIZE(tag) AS length, timestamp;

lower = FOREACH str_len GENERATE userid, movieid, LOWER(tag) AS tag, length, timestamp;

STORE lower INTO 'output/cleaned_tags' USING PigStorage();

