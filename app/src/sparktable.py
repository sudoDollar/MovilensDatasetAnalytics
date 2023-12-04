from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, explode, length, expr, from_unixtime
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("movielens").master('local') \
                    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movielens") \
                    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movielens") \
                    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                    .getOrCreate()

df = spark.read.csv("/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/movies.dat", sep='::', schema='MovieID int, oldTitle string, combinedGenres string')
df = df.withColumn("len", length(col("oldtitle")))
final_df = df.withColumn("Title", expr("substring(oldTitle, 1, len - 7)")).withColumn("Year", expr("substring(oldTitle, len - 4, 4)")).withColumn("Genres", split("combinedGenres", '\\|'))
movies_df = final_df.drop("oldTitle", "oldTitle_Modified", "Title_Year", "combinedGenres", "len")
movies_df.write.format("mongo").option("database", "movielens").option("collection", "movies").save()


df = spark.read.csv('/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/ratings.dat', sep='::', schema='UserID int, MovieID int, Rating int, Timestamp long')
df = df.withColumn("Time", from_unixtime(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))
ratings_df = df.drop("Timestamp")
ratings_df.write.format("mongo").option("database", "movielens").option("collection", "ratings").save()

df = spark.read.csv('/Users/ankitrajvanshi/MS CE/Sem 3/Big Data/Project/MovilensDatasetAnalytics/Dataset/users.dat', sep='::', schema='UserID int, Gender string, Age int, Occupation int, Zipcode int')
df.write.format("mongo").option("database", "movielens").option("collection", "users").save()