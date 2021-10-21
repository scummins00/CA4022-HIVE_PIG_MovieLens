/*
To re-run this file we must delete the existing output
*/

fs -rm -r output/cleaned_ratings

/*
Step 1: Read in the data using CSVLoader
Step 2: For some reason first row is empty so we must remove it
Step 3: Write out file using '\t' sep.
*/

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

ratings = LOAD 'data/ratings.csv' USING CSVLoader() as (userid: int, movieid: int, rating:float, timestamp:int);

rm_null = FILTER ratings BY NOT (userid IS NULL);

STORE rm_null INTO 'output/cleaned_ratings' using PigStorage();
