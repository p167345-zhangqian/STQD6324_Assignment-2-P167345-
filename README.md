# STQD6324_Assignment-2-P167345
# MovieLens 100K Data Pipeline using Apache Spark and Cassandra

## Project Overview

In this assignment, i builds a data pipeline using the MovieLens 100K dataset. 
The pipeline covers loading raw data into HDFS, processing it with Apache Spark, and storing the results in Cassandra.
There are five task need to do:
1. Calculate the average rating for each movie
2. Identify the top 10 movies with the highest average ratings
3. Identify users who rated at least 50 movies and find their favourite genre
4. Find users who are less than 20 years old
5. Find users whose occupation is `scientist` and whose age is between 30 and 40

## Dataset Files
The following MovieLens 100K files were used:

- `u.data`
- `u.user`
- `u.item`
  
Description:
- Movie ratings
- User demographics
- Movie info and genres

## Technologies Used

- HDFS
- Apache Spark
- PySpark
- Cassandra
- CQL
- PuTTY
- Ambari
- Zeppelin

## Screenshots of results

## Run process

### 1. Upload dataset into HDFS
---
- hdfs dfs -mkdir -p /user/maria_dev/assignment2
- hdfs dfs -put u.data u.user u.item /user/maria_dev/assignment2
---

Verify the upload: hdfs dfs -ls /user/maria_dev/assignment2

### 2. Run the Spark script
---
- spark-submit --master local[*] assignment2_generate_csv.py > generate_csv_output.log 2>&1

- This generates the CSV result files in `/home/maria_dev/assignment2/cassandra_csv`.
---

### 3. Load results into Cassandra

Open cqlsh:
---
- cd /opt/apache-cassandra-3.11.13/bin
- python cqlsh.py 127.0.0.1 9042
---

Create the keyspace:
---
- CREATE KEYSPACE IF NOT EXISTS assignment2
- WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
- USE assignment2;
---

Import each CSV file using the `COPY` command, for example:
---
- COPY top10_movies (movie_id, movie_title, average_rating)
- FROM '/home/maria_dev/assignment2/cassandra_csv/top10_movies.csv'
- WITH HEADER = TRUE;
---
Repeat for all five tables.

### 4. Validate
---
- SELECT COUNT(*) FROM average_movie_ratings;
- SELECT COUNT(*) FROM top10_movies;
- SELECT COUNT(*) FROM favourite_genres;
- SELECT COUNT(*) FROM users_under_20;
- SELECT COUNT(*) FROM scientist_users_30_40;
---

## Notes

My original plan was to write Spark DataFrames directly into Cassandra with the Spark Cassandra Connector. But I encountered JAR dependency conflicts in the current environment, and I could not fix this issue. To get around this problem, I exported the Spark results as CSV files first, then used the CQL COPY command to load the data into Cassandra. The final data is fully stored in Cassandra and can be queried normally.


