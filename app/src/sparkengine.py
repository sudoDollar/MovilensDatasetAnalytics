from pyspark.sql.functions import col, regexp_replace, split, explode, length, expr, from_unixtime
import pyspark.sql.functions as F

class SparkEngine:
    def __init__(self, spark):
        self.spark = spark
        self.movies_df = spark.read \
                            .format("com.mongodb.spark.sql.DefaultSource") \
                            .option("database", "movielens") \
                            .option("collection", "movies") \
                            .load()
        self.ratings_df = spark.read \
                            .format("com.mongodb.spark.sql.DefaultSource") \
                            .option("database", "movielens") \
                            .option("collection", "ratings") \
                            .load()
        self.users_df = spark.read \
                            .format("com.mongodb.spark.sql.DefaultSource") \
                            .option("database", "movielens") \
                            .option("collection", "users") \
                            .load()

    def get_movies_by_year(self, year, num):
        movies = self.movies_df.select("Title").filter(self.movies_df["Year"] == int(year)).distinct()
        return movies.rdd.flatMap(lambda x: x).take(num)