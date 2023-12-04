from pymongo import MongoClient

class MongoEngine:

    def __init__(self):
        # Create a MongoDB connection
        client = MongoClient("mongodb://127.0.0.1:27017/")
        # Access a specific database
        self.db = client["movielens"]
    
    def getGenereList(self):
        return [genre["genre"] for genre in self.db["genres"].find({}, {"genre": 1, "_id": 0})]
    
    def getYearList(self):
        return [year["year"] for year in self.db["years"].find({}, {"year": 1, "_id": 0})]