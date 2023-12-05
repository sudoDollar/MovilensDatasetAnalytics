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
    year = request.args.get('year')
    # Fetch the movies from the database
    movies = list(se.get_movies_by_year(year))
    # Convert the movies to a JSON response
    return jsonify(movies)

@app.route('/api/movieFilter', methods=['GET'])
def get_movies_filter():
    filter = request.args.get('filter')
    
    if filter == '':
        return jsonify([])
    if filter == 'genre':
        return jsonify(me.getGenreList())
    if filter == 'gender':
        return jsonify(['M','F'])


@app.route('/graphs')
def graphs():
    return render_template('graphs.html')

@app.route('/callback', methods = ['POST','GET'])
def cb():
    filterCondition = request.args.get('filter')
    
    graphData = getGraphJSON(filter = filterCondition)
    print(filterCondition)
    return graphData


#generate graph and it's JSON to pass to the html template 
#startDate and endDate to load data for user defined time period
#Change this function as per requirement. Currently it reads from sql table and returns JSON
def getGraphJSON(filter: str = ''):
    graphData = me.get_top_viewed_by_filter(filter)
    movie, viewers = zip(*graphData)
    fig = px.bar(x=movie, y=viewers, text=movie)  # Set the text attribute to movie values
    fig.update_traces(textposition='inside')  # Set the text position inside the bar
    fig.update_layout(xaxis_title='Movies', xaxis={'visible': True, 'showticklabels': False}, yaxis_title='Number of Viewers')
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON