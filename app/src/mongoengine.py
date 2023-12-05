from pymongo import MongoClient
import re

class MongoEngine:

    def __init__(self):
        # Create a MongoDB connection
        client = MongoClient("mongodb://127.0.0.1:27017/")
        # Access a specific database
        self.db = client["movielens"]
    
    def getGenreList(self):
        return [genre["genre"] for genre in self.db["genres"].find({}, {"genre": 1, "_id": 0})]
    
    def getYearList(self):
        return sorted([year["year"] for year in self.db["years"].find({}, {"year": 1, "_id": 0})], reverse=True)
    
    def get_top_viewed_by_filter(self, filter):
        if not filter or filter == "":
            return self.getTop10Movies()
        tableName = "top_10_most_viewed_movies_by_{}".format(filter)
        return [[movie["Title"],movie["Viewers"]] for movie in self.db[tableName].find({}, {"Title": 1, "Viewers": 1 , "_id": 0})]      
    
    
    def getTop10Movies(self):
        return [[movie["Title"],movie["Views"]] for movie in self.db["top_viewed_movies"].find({}, {"Title": 1, "Views": 1 , "_id": 0})]
    

    def getTopRatedMovies(self, movieFilter=None):
        if movieFilter and movieFilter != "":
            regex_pattern = re.compile(movieFilter, re.IGNORECASE)
            return [[movie["Title"], movie["AvgRating"], movie["Year"]] for movie in self.db["all_movie_ratings"].find({"Title": {"$regex": regex_pattern}}, {"Title": 1, "AvgRating": 1, "Year": 1, "_id": 0}).limit(20)]
        else:
            return [[movie["Title"], movie["AvgRating"], movie["Year"]] for movie in self.db["all_movie_ratings"].find({}, {"Title": 1, "AvgRating": 1, "Year": 1, "_id": 0}).limit(20)]
