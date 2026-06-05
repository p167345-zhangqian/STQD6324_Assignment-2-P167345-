# STQD6324_Assignment-2-P167345
# MovieLens 100K Data Pipeline using Apache Spark and Cassandra

## Course

STQD6324 Data Management

## Student ID

P167345

## Project Overview

This project builds a Python-based data pipeline using Apache Spark and Cassandra.

The MovieLens 100K dataset was used in this assignment. The dataset files were uploaded into HDFS and processed using Spark RDDs and Spark DataFrames.

The final analytical results were stored in Cassandra tables and validated using CQL queries.

## Dataset Files

The following MovieLens 100K files were used:

- `u.data`
- `u.user`
- `u.item`

## Technologies Used

- HDFS
- Apache Spark
- PySpark
- Cassandra
- CQL
- PuTTY
- Ambari
- Zeppelin

## Analytical Tasks

The following analytical tasks were completed:

1. Calculate the average rating for each movie.
2. Identify the top ten movies with the highest average ratings.
3. Identify users who rated at least 50 movies and determine their favourite movie genre.
4. Find all users who are less than 20 years old.
5. Find all users whose occupation is scientist and whose age is between 30 and 40 years old.

## Workflow

1. Create HDFS directory.
2. Upload MovieLens dataset files into HDFS.
3. Load raw files from HDFS into Spark RDDs.
4. Transform RDDs into Spark DataFrames.
5. Perform data cleaning and preprocessing.
6. Complete five analytical tasks.
7. Export analytical results as CSV files.
8. Create Cassandra keyspace and tables.
9. Import CSV files into Cassandra using CQL `COPY`.
10. Validate Cassandra tables using CQL queries.

## Cassandra Tables

The following Cassandra tables were created:

- `average_movie_ratings`
- `top10_movies`
- `favourite_genres`
- `users_under_20`
- `scientist_users_30_40`

## How to Run

Run the Spark script using:

```bash
spark-submit --master local[*] assignment2_generate_csv.py > generate_csv_output.log 2>&1
