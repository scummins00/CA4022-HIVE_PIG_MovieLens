/*
Step 1: Read in movie data using CSVLoader to take care of double quoted fields
Step 2: Replace comma in titles with '#com#'
Step 2: Remove the first tuple (column headers)
Step 3: Seperate the genres out into a tuple.
Step 4: Seperate title from date.
Step 6: Write out file with '\t' seperator
STEP 7: Read it back in with '\t' seperator.
Step 8: Fix the '#com#'
Step 10: Write out as tab seperated file.
*/
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

movies = LOAD 'movie/movies.csv' USING CSVLoader() as (movieid:int, title:chararray, genres:chararray);

fix_delim = FOREACH movies GENERATE movieid, REPLACE(title, ',', '#com#')AS title, genres;

no_header = FILTER fix_delim BY NOT (title == 'title' AND genres == 'genres'); 

genre_sep = FOREACH no_header GENERATE movieid, title, STRSPLIT(genres, '\\|', -1) as genres;

title_sep = FOREACH genre_sep GENERATE 
	movieid,
	REGEX_EXTRACT(title, '([\\S ]+) \\((\\d{4}|\\d{4}-?\\d{4})\\)', 1) AS tit,
	REGEX_EXTRACT(title, '\\((19\\d{2}|20\\d{2})\\)', 1) AS year,
	genres;

STORE title_sep INTO 'output/tmp_movies' USING PigStorage();

tab_mov = LOAD 'output/tmp_movies' USING PigStorage() as (movieid:int, title:chararray, year:int, genres:chararray);

fix_com = FOREACH tab_mov GENERATE movieid, REPLACE(title, '#com#', ',') AS title, year, genres;

STORE fix_com INTO 'output/cleaned_movies' USING PigStorage();
