from flask import Flask, render_template, request, redirect, url_for, flash
import datetime
import pandas as pd
import json
import plotly
import plotly.express as px
from dateutil.relativedelta import relativedelta



app = Flask(__name__)

@app.route('/')
@app.route('/home')
def home(msg = ''):
    
    
    return render_template('home.html', msg = msg)


@app.route('/graphs')
def graphs():
    return render_template('graphs.html', graphJSON = getGraphJSON())

@app.route('/callback', methods = ['POST','GET'])
def cb():
    dateRange = request.args.get('dateRange')
    startDate, endDate = dateRange.split('-')
    format = "%m/%d/%Y %I:%M %p"
    startDate = datetime.datetime.strptime(startDate.rstrip().lstrip(), format)
    endDate = datetime.datetime.strptime(endDate.rstrip().lstrip(), format)
    tableName = request.args.get('tableName')
    return getGraphJSON(tableName, startDate, endDate)



#generate graph and it's JSON to pass to the html template 
#startDate and endDate to load data for user defined time period
#Change this function as per requirement. Currently it reads from sql table and returns JSON
def getGraphJSON(tableName: str = 'Temperature', startDate: datetime = (datetime.datetime.now() + relativedelta(months=-1)), endDate: datetime = datetime.datetime.now()):
    conn = get_db_connection()
    psqlTable = constants.dropdownTableMap[tableName]
    query = 'select time,value FROM public.\"{0}\" where time > timestamp \'{1}\' and time < timestamp \'{2}\' order by time;'.format(psqlTable, startDate, endDate)
    global dataFrame
    dataFrame = pd.read_sql(query,conn)
    conn.close()
    global dfMetadata
    dfMetadata = '{0}_{1}_{2}'.format(tableName, startDate, endDate).replace(" ", "_")
    fig = px.line(dataFrame,x='time',y='value')
    graphJSON = json.dumps(fig,cls = plotly.utils.PlotlyJSONEncoder)
    return graphJSON