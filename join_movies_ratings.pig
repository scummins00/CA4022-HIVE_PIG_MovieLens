/*
This file aims to join the movies and ratings tables. The inclusion of the tabs table found in 'join_all.pig' 
drops movies that have not received any tags. Therefore, we lose some ratings for movies.
*/

fs -rm -r output/m_r_joined

movies = LOAD 'output/cleaned_movies/part-m-00000' USING PigStorage() as (movieid:int, title:chararray, year:int,
	genres:chararray);

ratings = LOAD 'output/cleaned_ratings/part-m-00000' USING PigStorage() as (userid:int, movieid:int, 
	rating:float, timestamp:chararray);


j_m_r = JOIN movies BY movieid, ratings BY movieid;

jmr = FOREACH j_m_r GENERATE movies::movieid AS movieid, movies::title AS title, movies::year AS year,
	movies::genres AS genres, ratings::userid AS userid, ratings::rating AS rating,
	ratings::timestamp AS timestamp;

STORE jmr INTO 'output/m_r_joined' USING PigStorage();
