
# MovieLens Recommendation System Project

This project is part of a big data assignment using Spark and Cassandra to analyze the MovieLens dataset (`u.data`, `u.item`, `u.user`). The tasks involve data loading, cleaning, processing, and some basic analysis using PySpark in Zeppelin.

## Dataset Info

- `u.data`: User ratings with fields — `user_id`, `item_id`, `rating`, `timestamp`
- `u.item`: Movie details — `item_id`, `title`, etc.
- `u.user`: User information — `user_id`, `age`, `gender`, `occupation`, `zipcode`

## Tools & Setup

- Apache Zeppelin Notebook
- Spark 2 (on HDP Sandbox)
- Apache Cassandra
- Data loaded into both HDFS and Cassandra

## Assignment Tasks

### 1. Average rating for each movie
Grouped by `item_id`, calculated average rating, then joined with movie titles and displayed top 10 results.

### 2. Top-rated movies
Sorted movies by average rating, with optional filtering to exclude movies with very few ratings to avoid bias.

### 3. Average rating per user
Grouped by `user_id`, computed the mean rating each user gave.

### 4. Average rating per occupation
Joined `u.data` with `u.user`, then grouped by `occupation` and calculated the average ratings.

### 5. Scientists aged 30–40
Filtered users where `occupation = "scientist"` and age between 30 and 40. Used this group to explore their favorite or highest-rated movies.

## Data in Cassandra

Used PySpark to write processed data into Cassandra.  
Keyspace: `movielens`  
Tables like `users`, `items`, and `ratings` were created and populated.

## Notes on Issues Faced

- Configuring Zeppelin to connect with Cassandra required adding the spark-cassandra-connector package manually.
- Faced connection errors with Cassandra (port 9042) which were fixed by starting the Cassandra service manually via command line.
- Errors caused by file headers (e.g., trying to parse column names like "user_id" as integers).
- Carefully had to convert RDDs to DataFrames using `toDF()` with correct data types.

## Summary

All assignment questions have been completed in the Zeppelin notebook. Spark was used for reading and transforming the data, and Cassandra for storing the results. This project helped practice Spark transformations, joins, and simple analysis logic.
