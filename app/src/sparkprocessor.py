from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, regexp_replace, split, explode, length, expr, from_unixtime
import pyspark.sql.functions as F
import shutil
from Utils import Utils

class SparkDataProcessor:

    def __init__(self, spark):
        self.spark = spark

    def load_data_into_db(self):
        df = self.spark.read.csv(Utils.MOVIE_FILE_PATH, sep='::', schema='MovieID int, oldTitle string, combinedGenres string')
        df = df.withColumn("len", length(col("oldtitle")))
        final_df = df.withColumn("Title", expr("substring(oldTitle, 1, len - 7)")).withColumn("Year", expr("substring(oldTitle, len - 4, 4)")).withColumn("Genres", split("combinedGenres", '\\|'))
        self.movies_df = final_df.drop("oldTitle", "oldTitle_Modified", "Title_Year", "combinedGenres", "len")
        self.movies_df.write.format("mongo").option("database", "movielens").option("collection", "movies").mode("overwrite").save()


        df = spark.read.csv(Utils.RATINGS_FILE_PATH, sep='::', schema='UserID int, MovieID int, Rating int, Timestamp long')
        df = df.withColumn("Time", from_unixtime(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))
        self.ratings_df = df.drop("Timestamp")
        self.ratings_df.write.format("mongo").option("database", "movielens").option("collection", "ratings").mode("overwrite").save()

        self.users_df = spark.read.csv(Utils.USERS_FILE_PATH, sep='::', schema='UserID int, Gender string, Age int, Occupation int, Zipcode int')
        self.users_df.write.format("mongo").option("database", "movielens").option("collection", "users").mode("overwrite").save()

    def save_all_genres(self):
        movies_rdd = self.spark.sparkContext.textFile(Utils.MOVIE_FILE_PATH)
        genres_rdd = movies_rdd.map(lambda x: x.split("::")[2])
        genres_rdd = genres_rdd.flatMap(lambda x: x.split("|"))
        genres_rdd = genres_rdd.map(lambda x: Row(genre = x))
        distinct_genres_rdd = genres_rdd.distinct().sortBy(lambda x: x)

        # Save RDD to MongoDB collection
        distinct_genres_rdd.toDF().write.format("mongo").option("database", "movielens").option("collection", "genres").mode("overwrite").save()


    
    
if __name__ == '__main__':

    spark = SparkSession.builder.appName("movielens").master('local') \
                        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movielens") \
                        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movielens") \
                        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                        .getOrCreate()

    sparkdp = SparkDataProcessor(spark)
    sparkdp.load_data_into_db()
    sparkdp.save_all_genres()
