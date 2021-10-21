/* 
Let's load in our dataset
*/

m_r = LOAD 'output/m_r_joined/part-r-00000' USING PigStorage() AS (movieid: int, title:chararray, year:int,
	genres:chararray, userid:int, rating:float, ratingtime:chararray);

/*=============================================================
	Title of Movie with Highest Rating
==============================================================*/

grouped = GROUP m_r BY (movieid, title, year);
number_of_ratings = FOREACH grouped GENERATE group.title, group.year, COUNT(m_r.rating) AS rating_count;
ordered = ORDER number_of_ratings BY rating_count DESC;
lim = LIMIT ordered 1;
dump lim;

/*=============================================================
	Title of the most liked movie
==============================================================*/


--Filter movies less than 4 star. Group by rating and include count of that particular rating

less_than_4_stars = FILTER m_r BY(rating >3.5);
describe_rating = FOREACH less_than_4_stars GENERATE movieid, title, year, rating AS ph, rating AS rating;
grouped1 = GROUP describe_rating BY (movieid, title, year, ph);
count_each_rating = FOREACH grouped1 GENERATE group.movieid, group.title,
	group.year, group.ph AS rating, COUNT(describe_rating.rating) AS no_ratings;

--Let's group the movies so their ratings are together
grouped_movies = GROUP count_each_rating BY (movieid, title, year);

--Let's iterate through bag tuples and average the rating
average_rating = FOREACH grouped_movies {
	tot_rating = FOREACH count_each_rating GENERATE rating * no_ratings;
	GENERATE group.movieid, group.title, group.year, SUM(count_each_rating.no_ratings) AS total_number_of_ratings,
	SUM(tot_rating) / SUM(count_each_rating.no_ratings) AS avg_rating;
}

--The 5 star rated movie with the highest number of ratings
highest_5_star = FILTER average_rating BY(avg_rating==5.0);
ordered_5_star = ORDER highest_5_star BY total_number_of_ratings ASC;
high_5_star_movie = LIMIT ordered_5_star 1;

/*
A better example of a good rated movie is one with considerably more ratings.
Let's find the movie with a rating higher than 4.7 stars and many ratings.
*/
high_4_8_star = FILTER average_rating BY(avg_rating>4.75);
ordered_4_8_star = ORDER high_4_8_star BY total_number_of_ratings ASC;


/*============================================================
	User With Highest Average Rating
=============================================================*/

mov_rate = LOAD 'output/m_r_joined/part-r-00000' AS (movieid:int, title:chararray, year:int, genres:chararray,
	userid:int, rate:int, timestamp:chararray);

--Let's group by user
user_group = GROUP mov_rate BY userid;

--Get average rating for each user
user_rating_average = FOREACH user_group GENERATE group, SUM(mov_rate.rate) AS total_rating,
	COUNT(mov_rate.rate) AS no_of_ratings, SUM(mov_rate.rate) / COUNT(mov_rate.rate) AS avg_rating;

--Order by average rating, then order by number of ratings DESC and limt 1
top_rate = ORDER user_rating_average BY avg_rating DESC, no_of_ratings DESC;
top_rater = LIMIT top_rate 1;

--The top rater has 20 reviews all 5 stars.

--Let's also find the person with the most ratings with the highest average

highest_rate = ORDER user_rating_average BY no_of_ratings, avg_rating;

--This user has nearly 2,700 ratings with an average of 3 stars.
