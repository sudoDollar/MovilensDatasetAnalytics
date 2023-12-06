from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import datetime
import pandas as pd
import json
import plotly
import plotly.express as px
from dateutil.relativedelta import relativedelta
import shutil
from pyspark.sql import SparkSession
from sparkengine import SparkEngine
from mongoengine import MongoEngine
from Utils import Utils

spark = SparkSession.builder.appName("movielens").master('local') \
                    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movielens") \
                    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movielens") \
                    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                    .getOrCreate()

sc = spark.sparkContext
se = SparkEngine(spark)

me = MongoEngine()

app = Flask(__name__)


@app.route('/')
@app.route('/home')
def home(msg = ''):
    years = list(me.getYearList())
    return render_template('home.html',years = years, msg = msg)


@app.route('/api/movies', methods=['GET'])
def get_movies():
    # Get the 'year' query parameter
    movie = request.args.get('searchMovie')
    # Fetch the movies from the database
    movies = me.getTopRatedMovies(movie)
    # Convert each movie item into a dictionary with proper fields
    movies_dict = [{'title': movie[0], 'rating': movie[1], 'year': movie[2]} for movie in movies]
    
    # Convert the movies dictionary to a JSON response
    return jsonify(movies_dict)

@app.route('/api/movieFilter', methods=['GET'])
def get_movies_filter():
    filter = request.args.get('filter')
    
    if filter == '':
        return jsonify([])
    if filter == 'genre':
        return jsonify(me.getGenreList())
    if filter == 'gender':
        return jsonify(['M','F'])


@app.route('/topViewed')
def topViewed():
    return render_template('topViewed.html')

@app.route('/topRated')
def topRated():
    return render_template('topRated.html')

@app.route('/callback', methods = ['POST','GET'])
def cb():
    filterCondition = request.args.get('filter')
    graph = request.args.get('graph')
    if graph == 'mostViewed':
        return getTopViewedGraphJSON(filterCondition)
    if graph == "topRated":
        return getTopRatedGraphJSON(filterCondition)

#generate graph and it's JSON to pass to the html template 
#startDate and endDate to load data for user defined time period
#Change this function as per requirement. Currently it reads from sql table and returns JSON
def getTopViewedGraphJSON(filter: str = ''):
    graphData = me.get_top_viewed_by_filter(filter)
    return createGraphJSON(graphData, "Movies", "Views")

def getTopRatedGraphJSON(filter: str=''):
    graphData = me.get_top_rated_by_filter(filter)
    return createGraphJSON(graphData, "Movies", "Average Rating")

def createGraphJSON(graphData, xaxisTitle, yAxisTitle):
    movie, yData = zip(*graphData)
    fig = px.bar(x=movie, y=yData, text=movie)  # Set the text attribute to movie values
    fig.update_traces(textposition='inside')  # Set the text position inside the bar
    fig.update_layout(xaxis_title=xaxisTitle, xaxis={'visible': True, 'showticklabels': False}, yaxis_title=yAxisTitle)
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON

@app.route('/genreDistribution')
def genreDistribution():
    occupationList = list(Utils.occupation_dict.values())
    ageGroupList = sorted(me.get_unique_age_groups())
    return render_template('genreDistribution.html', occupations = occupationList, ageGroupList = ageGroupList)

@app.route('/getGenreOccupationChart')
def getGenreOccupationChart():
    graphData = me.get_genre_occupation_distribution(request.args.get('occupation'))
    return createPieChartJSON(graphData, 'Occupation')
    
@app.route('/getGenreAgeChart')
def getGenreAgeChart():
    graphData = me.get_genre_age_distribution(request.args.get('ageGroup'))
    return createPieChartJSON(graphData, 'Age Group')
    
def createPieChartJSON(graphData, title):
    genre, yData = zip(*graphData)
    fig = px.pie(values=yData, names=genre, title='Genre Distribution by '+title)
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON