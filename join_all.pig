/*
Note: This JOIN will only consider users who: Watch, Rate, and Tag a movie.
Step 1: Read in all 3 tables
Step 2: Join ratings and tags
Step 3: Remove redundant data  
Step 4: Join ratings_and_tags with movies
Step 5: Remove redundant data
Step 6: Write out file
*/

ratings = LOAD 'output/cleaned_ratings' USING PigStorage() as (userid:int, movieid:int, rating:float, timestamp:int);

movies = LOAD 'output/cleaned_movies' USING PigStorage() as (movieid: int, title:chararray, year:int, genres:chararray);

tags = LOAD 'output/cleaned_tags' USING PigStorage() as (userid: int, movieid: int, tag: chararray, length: int, timestamp:int);

ratings_and_tags = JOIN ratings BY (userid, movieid), tags BY (userid, movieid);

joined_r_and_t = FOREACH ratings_and_tags GENERATE ratings::userid AS userid, ratings::movieid AS movieid,
	ratings::rating AS rating, ratings::timestamp AS ratingtime, tags::tag AS tag, tags::length AS taglength,
	tags::timestamp AS tagtime;

/*
Trying to figure out if i can concat all tags together as one string, add up all tag lengths and leave 
tag timestamps as a tuple
grouped = GROUP joined_r_and_t BY (userid, movieid, rating, ratingtime);
test = FOREACH grouped GENERATE FLATTEN($0) as (userid, movieid, rating, ratingtime), $1;
dump test;
*/

j_m_r_t = JOIN movies BY movieid, joined_r_and_t BY movieid;

m_r_t = FOREACH j_m_r_t GENERATE movies::movieid AS movieid, movies::title AS title,
	  movies::year AS year, movies::genres AS genres, joined_r_and_t::userid AS userid, 
	  joined_r_and_t::rating AS rating, joined_r_and_t::ratingtime AS ratingtime,
	  joined_r_and_t::tag AS tag, joined_r_and_t::taglength AS taglength,
	  joined_r_and_t::tagtime AS tagtime;

STORE m_r_t into 'output/all_joined' USING PigStorage();
