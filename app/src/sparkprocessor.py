from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, regexp_replace, split, explode, length, expr, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from Utils import Utils

class SparkDataProcessor:

    genres = []
    ageGroup = Utils.ageGroup
    ageGroup2 = Utils.ageGroup2


    def __init__(self, spark):
        self.spark = spark
        self.ratings_rdd = spark.sparkContext.textFile(Utils.RATINGS_FILE_PATH)
        self.movies_rdd = spark.sparkContext.textFile(Utils.MOVIE_FILE_PATH)
        self.users_rdd = spark.sparkContext.textFile(Utils.USERS_FILE_PATH)
        self.data_clean = False
        self.ut = Utils()
        
    def load_clean_data_toDF(self):
        #Movies
        df = self.spark.read.csv(Utils.MOVIE_FILE_PATH, sep='::', schema='MovieID int, oldTitle string, combinedGenres string')
        df = df.withColumn("len", length(col("oldtitle")))
        final_df = df.withColumn("Title", expr("substring(oldTitle, 1, len - 7)")).withColumn("Year", expr("substring(oldTitle, len - 4, 4)")).withColumn("Genres", split("combinedGenres", '\\|'))
        self.movies_df = final_df.drop("oldTitle", "oldTitle_Modified", "Title_Year", "combinedGenres", "len")

        #Ratings
        df = self.spark.read.csv(Utils.RATINGS_FILE_PATH, sep='::', schema='UserID int, MovieID int, Rating int, Timestamp long')
        df = df.withColumn("Time", from_unixtime(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))
        self.ratings_df = df.drop("Timestamp")

        #Users
        self.users_df = self.spark.read.csv(Utils.USERS_FILE_PATH, sep='::', schema='UserID int, Gender string, Age int, Occupation int, Zipcode string')

        self.data_clean = True
    
    def load_data_into_db(self):
        if self.data_clean == False:
            print("Clean data and load into DF first")
            return False
        
        #Movies
        self.movies_df.write.format("mongo").option("database", "movielens").option("collection", "movies").mode("overwrite").save()

        #Ratings
        self.ratings_df.write.format("mongo").option("database", "movielens").option("collection", "ratings").mode("overwrite").save()

        #Users
        self.users_df.write.format("mongo").option("database", "movielens").option("collection", "users").mode("overwrite").save()

        return True

    def save_all_genres(self):
        genres_rdd = self.movies_rdd.map(lambda x: x.split("::")[2])
        genres_rdd = genres_rdd.flatMap(lambda x: x.split("|"))
        genres_rdd = genres_rdd.map(lambda x: Row(genre = x))
        distinct_genres_rdd = genres_rdd.distinct().sortBy(lambda x: x)
        self.genres = list(distinct_genres_rdd.flatMap(lambda x: x).collect())

        # Save RDD to MongoDB collection
        distinct_genres_rdd.toDF().write.format("mongo").option("database", "movielens").option("collection", "genres").mode("overwrite").save()
    
    def save_all_years(self):
        title_year_rdd = self.movies_rdd.map(lambda x: x.split("::")[1])
        year_rdd = title_year_rdd.map(lambda x: x[-5:-1]).map(lambda x: Row(year = x))
        distinct_years_rdd = year_rdd.distinct().sortBy(lambda x: x)

        # Save RDD to MongoDB collection
        distinct_years_rdd.toDF().write.format("mongo").option("database", "movielens").option("collection", "years").mode("overwrite").save()

    def save_top_n_most_viewed_movies(self, num):
        ratings_rdd = self.ratings_rdd
        movies = ratings_rdd.map(lambda x: (x.split("::")[1], 1))
        movies = movies.reduceByKey(lambda x, y: x + y)
        top_num_movies = movies.sortBy(lambda x: x[1], ascending=False).take(num)
        
        movies_rdd = self.movies_rdd
        title_rdd = movies_rdd.map(lambda x: (x.split("::")[0], x.split("::")[1][:-7]))
        top_num_movies_title = title_rdd.join(spark.sparkContext.parallelize(top_num_movies)).sortBy(lambda x: x[1][1], ascending=False).map(lambda x: (x[1][0], x[1][1]))
        top_num_movies_title_df = spark.createDataFrame(top_num_movies_title, ["Title", "Views"])
        top_num_movies_title_df.write.format("mongo").option("database", "movielens").option("collection", "top_viewed_movies").mode("overwrite").save()

    def save_top_n_most_liked_movies_by_gender(self, num: int, gender: str):
        join1 = self.ratings_df.join(self.users_df, on="UserID" , how="inner").filter(col("Gender") == gender)
        join2 = join1.join(self.movies_df, on="MovieID", how="inner")
        result = join2.groupBy("MovieID", "Title").agg(F.avg("Rating").alias("AvgRating")).orderBy(col("AvgRating").desc()).limit(num)
        result = result.drop("MovieID")
        result.write.format("mongo").option("database", "movielens").option("collection", "top_{}_most_liked_movies_by_{}".format(num,gender)).mode("overwrite").save()
    
    def save_top_n_most_viewed_movies_by_gender(self, num: int, gender: str):
        join1 = self.ratings_df.join(self.users_df, on="UserID" , how="inner").filter(col("Gender") == gender)
        join2 = join1.join(self.movies_df, on="MovieID", how="inner")
        result = join2.groupBy("MovieID", "Title").agg(F.count("UserID").alias("Viewers")).orderBy(col("Viewers").desc()).limit(num)
        result = result.drop("MovieID")
        result.write.format("mongo").option("database", "movielens").option("collection", "top_{}_most_viewed_movies_by_{}".format(num,gender)).mode("overwrite").save()

    def save_top_n_most_viewed_movies_by_genre(self, num: int, genre: str):
        movies = self.movies_df.select("MovieID", "Title", explode("Genres").alias("Genre")).filter(col("Genre") == genre)
        join1 = self.ratings_df.join(movies, on="MovieID", how="inner")
        result = join1.groupBy("MovieID", "Title").agg(F.count("UserID").alias("Viewers")).orderBy(col("Viewers").desc()).limit(num)
        result.drop("MovieID")
        result.write.format("mongo").option("database", "movielens").option("collection", "top_{}_most_viewed_movies_by_{}".format(num,genre)).mode("overwrite").save()

    def save_top_n_most_liked_movies_by_genre(self, num: int, genre: str):
        movies = self.movies_df.select("MovieID", "Title", explode("Genres").alias("Genre")).filter(col("Genre") == genre)
        join1 = self.ratings_df.join(movies, on="MovieID", how="inner")
        result = join1.groupBy("MovieID", "Title").agg(F.avg("Rating").alias("AvgRating")).orderBy(col("AvgRating").desc()).limit(num)
        result.drop("MovieID")
        result.write.format("mongo").option("database", "movielens").option("collection", "top_{}_most_liked_movies_by_{}".format(num,genre)).mode("overwrite").save()
    
    def save_movies_count_by_genre(self):
        movies = self.movies_df.select("MovieID", explode("Genres").alias("Genre"))
        genre_count = movies.groupBy("Genre").agg(F.count("MovieID").alias("Count")).orderBy(col("Count").desc())
        genre_count.write.format("mongo").option("database", "movielens").option("collection", "movies_count_by_genre").mode("overwrite").save()

    def save_movies_count_by_year(self):
        movies = self.movies_df.select("MovieID", "Year")
        genre_count = movies.groupBy("Year").agg(F.count("MovieID").alias("Count")).orderBy(col("Year"))
        genre_count.write.format("mongo").option("database", "movielens").option("collection", "movies_count_by_year").mode("overwrite").save()

    def save_state_genre_distribution(self):
        movies = self.movies_df.select("MovieID", explode("Genres").alias("Genre"))
        my_udf = F.udf(self.ut.get_state, StringType())
        users = self.users_df.withColumn("State", my_udf("Zipcode"))
        join1 = self.ratings_df.join(users, on="UserID", how="inner")
        join2 = join1.join(movies, on="MovieID", how="inner")
        result = join2.groupBy("State", "Genre").agg(F.count("UserID").alias("Count"))
        result.write.format("mongo").option("database", "movielens").option("collection", "state_genre_distribution").mode("overwrite").save()

        window_spec = Window.partitionBy("State")
        max_count_column = F.max("Count").over(window_spec)
        result = result.withColumn("max_count", max_count_column)
        result = result.filter(col("Count") == col("max_count")).drop("max_count")
        result.write.format("mongo").option("database", "movielens").option("collection", "state_genre_max").mode("overwrite").save()
        
    def save_all_movie_ratings(self):
        movies = self.movies_df.select("MovieID", "Title", "Year")
        join1 = self.ratings_df.join(movies, on="MovieID", how="inner")
        result = join1.groupBy("MovieID", "Title", "Year").agg(F.avg("Rating").alias("AvgRating")).orderBy(col("AvgRating").desc())
        result.write.format("mongo").option("database", "movielens").option("collection", "all_movie_ratings").mode("overwrite").save()

    def save_occupation_genre_distribution(self):
        movies = self.movies_df.select("MovieID", explode("Genres").alias("Genre"))
        my_udf = F.udf(Utils.get_occupation, StringType())
        users = self.users_df.withColumn("OccupationName", my_udf("Occupation"))
        join1 = self.ratings_df.join(users, on="UserID", how="inner")
        join2 = join1.join(movies, on="MovieID", how="inner")
        result = join2.groupBy("Occupation", "OccupationName","Genre").agg(F.count("UserID").alias("Count")).drop("Occupation")
        result.write.format("mongo").option("database", "movielens").option("collection", "occupation_genre_distribution").mode("overwrite").save()

        window_spec = Window.partitionBy("OccupationName")
        max_count_column = F.max("Count").over(window_spec)
        result = result.withColumn("max_count", max_count_column)
        result = result.filter(col("Count") == col("max_count")).drop("max_count")
        result.write.format("mongo").option("database", "movielens").option("collection", "occupation_genre_max").mode("overwrite").save()

    def save_age_genre_distribution(self):
        movies = self.movies_df.select("MovieID", explode("Genres").alias("Genre"))
        my_udf = F.udf(Utils.get_age_group, StringType())
        users = self.users_df.withColumn("AgeGroup", my_udf("Age"))
        join1 = self.ratings_df.join(users, on="UserID", how="inner")
        join2 = join1.join(movies, on="MovieID", how="inner")
        result = join2.groupBy("AgeGroup","Genre").agg(F.count("UserID").alias("Count"))
        result.write.format("mongo").option("database", "movielens").option("collection", "age_genre_distribution").mode("overwrite").save()

        window_spec = Window.partitionBy("AgeGroup")
        max_count_column = F.max("Count").over(window_spec)
        result = result.withColumn("max_count", max_count_column)
        result = result.filter(col("Count") == col("max_count")).drop("max_count")
        result.write.format("mongo").option("database", "movielens").option("collection", "age_genre_max").mode("overwrite").save()

    def save_top_n_most_liked_movies_by_age(self, num: int, age_group: str):
        my_udf = F.udf(Utils.get_age_group2, StringType())
        users = self.users_df.withColumn("AgeGroup", my_udf("Age"))
        join1 = self.ratings_df.join(users, on="UserID" , how="inner").filter(col("AgeGroup") == age_group)
        join2 = join1.join(self.movies_df, on="MovieID", how="inner")
        result = join2.groupBy("MovieID", "Title").agg(F.avg("Rating").alias("AvgRating")).orderBy(col("AvgRating").desc()).limit(num)
        result = result.drop("MovieID")
        result.write.format("mongo").option("database", "movielens").option("collection", "top_{}_most_liked_movies_by_{}".format(num,age_group)).mode("overwrite").save()

    def save_top_n_most_viewed_movies_by_age(self, num: int, age_group: str):
        my_udf = F.udf(Utils.get_age_group2, StringType())
        users = self.users_df.withColumn("AgeGroup", my_udf("Age"))
        join1 = self.ratings_df.join(users, on="UserID" , how="inner").filter(col("AgeGroup") == age_group)
        join2 = join1.join(self.movies_df, on="MovieID", how="inner")
        result = join2.groupBy("MovieID", "Title").agg(F.count("UserID").alias("Viewers")).orderBy(col("Viewers").desc()).limit(num)
        result = result.drop("MovieID")
        result.write.format("mongo").option("database", "movielens").option("collection", "top_{}_most_viewed_movies_by_{}".format(num,age_group)).mode("overwrite").save()



if __name__ == '__main__':

    spark = SparkSession.builder.appName("movielens").master('local') \
                        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movielens") \
                        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movielens") \
                        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                        .getOrCreate()

    sparkdp = SparkDataProcessor(spark)
    sparkdp.load_clean_data_toDF()
    sparkdp.load_data_into_db()
    sparkdp.save_all_genres()
    sparkdp.save_all_years()
    sparkdp.save_top_n_most_viewed_movies(10)
    sparkdp.save_top_n_most_liked_movies_by_gender(10, "M")
    sparkdp.save_top_n_most_liked_movies_by_gender(10, "F")
    sparkdp.save_top_n_most_viewed_movies_by_gender(10, "M")
    sparkdp.save_top_n_most_viewed_movies_by_gender(10, "F")
    for genre in sparkdp.genres:
        sparkdp.save_top_n_most_viewed_movies_by_genre(10, genre)
        sparkdp.save_top_n_most_liked_movies_by_genre(10, genre)
    sparkdp.save_movies_count_by_genre()
    sparkdp.save_movies_count_by_year()
    sparkdp.save_state_genre_distribution()
    sparkdp.save_all_movie_ratings()
    sparkdp.save_occupation_genre_distribution()
    sparkdp.save_age_genre_distribution()
    for age in sparkdp.ageGroup2:
        sparkdp.save_top_n_most_liked_movies_by_age(10, age)
        sparkdp.save_top_n_most_viewed_movies_by_age(10, age)
    

