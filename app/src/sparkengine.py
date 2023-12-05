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