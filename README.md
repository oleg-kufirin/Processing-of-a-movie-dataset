# Processing of a movie dataset using Hadoop MapReduce
## Part of the assesment for *Big Data Management* class at UNSW, Sydney 

In this project we will be working on the processing of a Movie dataset:  
https://raw.githubusercontent.com/sidooms/MovieTweetings/master/latest/ratings.dat

In this dataset, each row contains a movie rating done by a user (e.g., user1 has rated Titanic as 10). Here is the format of the dataset: 
-- user_id::movie_id::rating::timestamp

In this assignment, for each pair of movies A and B, you need to find all the users who rated both movie A and B. For example, given the following dataset (for the sake of illustration we have used U and M to represent users and movies respectively in the example):
```
U1::M1::2::11111111
U2::M2::3::11111111
U2::M3::1::11111111
U3::M1::4::11111111
U4::M2::5::11111111
U5::M2::3::11111111
U5::M1::1::11111111
U5::M3::3::11111111
```
