from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, avg, desc, lit, row_number
from pyspark.sql.window import Window
import os
import shutil
import glob

# =========================================================
# Assignment 2: Generate Analytical Results as CSV Files
# =========================================================

# ---------------------------------------------------------
# Step 1: Create Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Assignment2_Generate_CSV_Results") \
    .getOrCreate()

sc = spark.sparkContext

print("Spark session successfully.")
print("Spark version:", spark.version)

# ---------------------------------------------------------
# Step 2: Load MovieLens files from HDFS and create RDDs
# ---------------------------------------------------------

ratings_path = "/user/maria_dev/assignment2/u.data"
users_path = "/user/maria_dev/assignment2/u.user"
items_path = "/user/maria_dev/assignment2/u.item"

ratings_rdd = sc.textFile(ratings_path)
users_rdd = sc.textFile(users_path)
items_rdd = sc.textFile(items_path)

print("\nRDDs created successfully.")

print("\nFirst 5 records from u.data:")
for row in ratings_rdd.take(5):
    print(row)

print("\nFirst 5 records from u.user:")
for row in users_rdd.take(5):
    print(row)

print("\nFirst 5 records from u.item:")
for row in items_rdd.take(5):
    print(row)

# ---------------------------------------------------------
# Step 3: Convert RDDs into DataFrames
# ---------------------------------------------------------

ratings_df = ratings_rdd.map(lambda line: line.split("\t")) \
    .map(lambda fields: Row(
        user_id=int(fields[0]),
        movie_id=int(fields[1]),
        rating=int(fields[2]),
        timestamp=int(fields[3])
    )).toDF()

ratings_df = ratings_df.select("user_id", "movie_id", "rating", "timestamp")

users_df = users_rdd.map(lambda line: line.split("|")) \
    .map(lambda fields: Row(
        user_id=int(fields[0]),
        age=int(fields[1]),
        gender=fields[2],
        occupation=fields[3],
        zip_code=fields[4]
    )).toDF()

users_df = users_df.select("user_id", "age", "gender", "occupation", "zip_code")

items_df = items_rdd.map(lambda line: line.split("|")) \
    .map(lambda fields: Row(
        movie_id=int(fields[0]),
        movie_title=fields[1],
        release_date=fields[2],
        unknown=int(fields[5]),
        Action=int(fields[6]),
        Adventure=int(fields[7]),
        Animation=int(fields[8]),
        Children=int(fields[9]),
        Comedy=int(fields[10]),
        Crime=int(fields[11]),
        Documentary=int(fields[12]),
        Drama=int(fields[13]),
        Fantasy=int(fields[14]),
        Film_Noir=int(fields[15]),
        Horror=int(fields[16]),
        Musical=int(fields[17]),
        Mystery=int(fields[18]),
        Romance=int(fields[19]),
        Sci_Fi=int(fields[20]),
        Thriller=int(fields[21]),
        War=int(fields[22]),
        Western=int(fields[23])
    )).toDF()

items_df = items_df.select(
    "movie_id", "movie_title", "release_date",
    "unknown", "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"
)

print("\nDataFrames created successfully.")

print("\nratings_df:")
ratings_df.show(5)
ratings_df.printSchema()

print("\nusers_df:")
users_df.show(5)
users_df.printSchema()

print("\nitems_df:")
items_df.select("movie_id", "movie_title", "release_date", "Action", "Comedy", "                                                                             Drama").show(5, truncate=False)
items_df.printSchema()

# ---------------------------------------------------------
# Step 4: Data Cleaning and Preprocessing
# ---------------------------------------------------------

print("\nRecord counts:")
print("ratings_df:", ratings_df.count())
print("users_df:", users_df.count())
print("items_df:", items_df.count())

ratings_df.createOrReplaceTempView("ratings")
users_df.createOrReplaceTempView("users")
items_df.createOrReplaceTempView("items")

print("\nTemporary SQL views created successfully.")

# ---------------------------------------------------------
# Task i: Calculate average rating for each movie
# ---------------------------------------------------------

avg_rating_df = ratings_df.groupBy("movie_id") \
    .agg(avg("rating").alias("average_rating"))

avg_rating_with_title_df = avg_rating_df.join(
    items_df.select("movie_id", "movie_title"),
    on="movie_id",
    how="inner"
).select(
    col("movie_id").cast("int").alias("movie_id"),
    col("movie_title").cast("string").alias("movie_title"),
    col("average_rating").cast("double").alias("average_rating")
)

print("\nTask i: Average rating for movies")
avg_rating_with_title_df.show(10, truncate=False)

# ---------------------------------------------------------
# Task ii: Top 10 movies with highest average ratings
# ---------------------------------------------------------

top10_movies_df = avg_rating_with_title_df.orderBy(desc("average_rating")).limit                                                                             (10)

print("\nTask ii: Top 10 movies with highest average ratings")
top10_movies_df.show(10, truncate=False)

# ---------------------------------------------------------
# Task iii: Users rated at least 50 movies and favourite genre
# ---------------------------------------------------------

active_users_df = ratings_df.groupBy("user_id") \
    .count() \
    .withColumnRenamed("count", "rating_count") \
    .filter(col("rating_count") >= 50)

active_user_ratings_df = ratings_df.join(
    active_users_df,
    on="user_id",
    how="inner"
)

active_user_movies_df = active_user_ratings_df.join(
    items_df,
    on="movie_id",
    how="inner"
)

genre_columns = [
    "unknown", "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"
]

genre_rating_df = None

for genre in genre_columns:
    temp_df = active_user_movies_df.filter(col(genre) == 1) \
        .select(
            "user_id",
            lit(genre).alias("genre")
        )

    if genre_rating_df is None:
        genre_rating_df = temp_df
    else:
        genre_rating_df = genre_rating_df.union(temp_df)

user_genre_count_df = genre_rating_df.groupBy("user_id", "genre") \
    .count() \
    .withColumnRenamed("count", "genre_count")

window_spec = Window.partitionBy("user_id").orderBy(desc("genre_count"))

favourite_genre_df = user_genre_count_df.withColumn(
    "rank",
    row_number().over(window_spec)
).filter(col("rank") == 1)

favourite_genre_df = favourite_genre_df.join(
    active_users_df,
    on="user_id",
    how="inner"
).select(
    col("user_id").cast("int").alias("user_id"),
    col("rating_count").cast("int").alias("rating_count"),
    col("genre").cast("string").alias("genre"),
    col("genre_count").cast("int").alias("genre_count")
)

print("\nTask iii: Favourite genre for users rated at least 50 movies")
favourite_genre_df.show(20, truncate=False)

# ---------------------------------------------------------
# Task iv: Users less than 20 years old
# ---------------------------------------------------------

users_under_20_df = users_df.filter(col("age") < 20).select(
    col("user_id").cast("int").alias("user_id"),
    col("age").cast("int").alias("age"),
    col("gender").cast("string").alias("gender"),
    col("occupation").cast("string").alias("occupation"),
    col("zip_code").cast("string").alias("zip_code")
)

print("\nTask iv: Users who are less than 20 years old")
users_under_20_df.show(20, truncate=False)

# ---------------------------------------------------------
# Task v: Scientist users aged between 30 and 40
# ---------------------------------------------------------

scientist_users_df = users_df.filter(
    (col("occupation") == "scientist") &
    (col("age") >= 30) &
    (col("age") <= 40)
).select(
    col("user_id").cast("int").alias("user_id"),
    col("age").cast("int").alias("age"),
    col("gender").cast("string").alias("gender"),
    col("occupation").cast("string").alias("occupation"),
    col("zip_code").cast("string").alias("zip_code")
)

print("\nTask v: Scientist users aged between 30 and 40")
scientist_users_df.show(20, truncate=False)

# ---------------------------------------------------------
# Step 5: Save analytical results as CSV files
# ---------------------------------------------------------

base_output_dir = "/home/maria_dev/assignment2/cassandra_csv"

if os.path.exists(base_output_dir):
    shutil.rmtree(base_output_dir)

os.makedirs(base_output_dir)

def save_single_csv(df, folder_name, final_file_name):
    temp_path = os.path.join(base_output_dir, folder_name)

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("file://" + temp_path)

    part_file = glob.glob(os.path.join(temp_path, "part-*.csv"))[0]
    final_path = os.path.join(base_output_dir, final_file_name)

    shutil.copy(part_file, final_path)

    print("CSV file created:", final_path)

save_single_csv(avg_rating_with_title_df, "average_movie_ratings_temp", "average                                                                             _movie_ratings.csv")
save_single_csv(top10_movies_df, "top10_movies_temp", "top10_movies.csv")
save_single_csv(favourite_genre_df, "favourite_genres_temp", "favourite_genres.c                                                                             sv")
save_single_csv(users_under_20_df, "users_under_20_temp", "users_under_20.csv")
save_single_csv(scientist_users_df, "scientist_users_30_40_temp", "scientist_use                                                                             rs_30_40.csv")

print("\nAll analytical result CSV files were created successfully.")
print("Output folder:", base_output_dir)

spark.stop()
