/*
To re-run this file we must remove current output
*/
fs -rm -r output/all_joined

/*
Note: This JOIN will only consider movies which have been both rated and tagged.
Step 1: Read in all 3 tables
Step 2: Join ratings and tags
Step 3: Remove redundant data  
Step 4: Join ratings_and_tags with movies
Step 5: Remove redundant data
Step 6: Write out file
*/

ratings = LOAD 'output/cleaned_ratings' USING PigStorage() as (userid:int, movieid:int, rating:float, timestamp:int);

movies = LOAD 'output/cleaned_movies' USING PigStorage() as (movieid: int, title:chararray, year:int, genres:chararray);

tag = LOAD 'output/cleaned_tags' USING PigStorage() as (userid: int, movieid: int, tags:chararray, length:int);

ratings_and_tags = JOIN ratings BY (userid, movieid), tag BY (userid, movieid);

joined_r_and_t = FOREACH ratings_and_tags GENERATE ratings::userid AS userid, ratings::movieid AS movieid,
	ratings::rating AS rating, tag::tags AS tags, tag::length AS taglength;

j_m_r_t = JOIN movies BY movieid, joined_r_and_t BY movieid;

m_r_t = FOREACH j_m_r_t GENERATE movies::movieid AS movieid, movies::title AS title,
	  movies::year AS year, movies::genres AS genres, joined_r_and_t::userid AS userid, 
	  joined_r_and_t::rating AS rating, joined_r_and_t::tags AS tags, joined_r_and_t::taglength AS taglength;

STORE m_r_t into 'output/all_joined' USING PigStorage();
