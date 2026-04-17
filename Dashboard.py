import streamlit as st
import pandas as pd
import numpy as np
import platform
import sys
import os
from prophet import Prophet
from cassandra.cluster import Cluster

#code for what the workers should do is hiding in here:
import worker_task 

#and here is the chart gernerators:
import chartGen

#force spark to use the same Python as Streamlit / conda env
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

#force spark to run as a LOCAL instance
for var in ["SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED", "PYSPARK_REMOTE"]:
    os.environ.pop(var, None)

#set Hadoop utils for Spark.
if platform.system() == "Windows":
    # Set Hadoop home (winutils.exe must exist in bin/)
    hadoop_home = os.path.abspath("./Hadoop")
    winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")

    if not os.path.exists(winutils_path):
        raise RuntimeError(f"winutils.exe not found, download and place it in {os.path.join(hadoop_home,'bin')}, if you yoinked the zip file as is, it should already be there")

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

#create a spark session once on startup.
@st.cache_resource
def get_spark():
    from pyspark.sql import SparkSession
    import os
    #this ensures that Spark always has the Hadoop utils aveilable.
    hadoop_home = os.path.abspath("./Hadoop")

    return SparkSession.builder \
    .appName("StockFetcher") \
    .master("local[*]") \
    .config("spark.hadoop.home.dir", hadoop_home)\
    .config("spark.executorEnv.HADOOP_HOME", hadoop_home)\
    .config("spark.executorEnv.PATH", os.path.join(hadoop_home, "bin"))\
    .getOrCreate()

#create one Cassandra connection on startup.
@st.cache_resource
def get_session():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('stocks')

#start cassandra
session = get_session()

#start up spark
spark = get_spark()
sc = spark.sparkContext

#frontend text
st.title("Company data finder")
st.caption(f"not yet trademarked, working on it.")

st.write("fill this field with the shorthands for any companies you wish to have included in the database as a comma separated list")
st.write("example: AAPL,MSFT,K,DE. Press the button to insert data (run at least once to get initial data)")
identifier = st.text_input('the companies you want included in the data', key="identifier")

#when the button is clicked, run the code
if st.button("Insert Data"):
    st.info("Processing.")
    compDF = None
    try:
        #just a REALLY long list of companies so we have DATA to work with
        companies = [
        "AAPL","MSFT","GOOG","AMZN","TSLA","NVDA","META","NFLX","INTC","AMD",
        "ORCL","IBM","ADBE","CRM","CSCO","QCOM","TXN","AVGO","MU","AMAT",
        "LRCX","KLAC","ADI","NXPI","MCHP","SNPS","CDNS","ANSS","WDAY","NOW",
        "SHOP","SQ","PYPL","UBER","LYFT","ABNB","DASH","SNOW","PLTR","OKTA",
        "CRWD","ZS","NET","DDOG","MDB","TEAM","TWLO","DOCU","PANW","FTNT",
        "JPM","BAC","WFC","C","GS","MS","BLK","SCHW","AXP","COF",
        "USB","PNC","TFC","BK","STT","AIG","MET","PRU","ALL","TRV",
        "V","MA","FIS","FISV","GPN","PAYX","ADP","INTU","ICE","CME",
        "BRK-B","UNH","JNJ","PFE","MRK","ABBV","TMO","ABT","DHR","LLY",
        "BMY","AMGN","GILD","CVS","CI","HUM","ZTS","REGN","VRTX","BIIB",
        "MDT","SYK","ISRG","BSX","EW","IDXX","ILMN","DXCM","ALGN","HOLX",
        "WMT","COST","TGT","HD","LOW","NKE","SBUX","MCD","YUM","CMG",
        "BKNG","MAR","HLT","ROST","TJX","DG","DLTR","EBAY","ETSY","KMX",
        "KO","PEP","MNST","KDP","PM","MO","PG","CL","KMB","EL",
        "XOM","CVX","COP","SLB","EOG","PSX","VLO","MPC","OXY","HAL",
        "CAT","DE","BA","LMT","RTX","NOC","GD","HON","GE","MMM",
        "UPS","FDX","UNP","CSX","NSC","DAL","UAL","AAL","LUV","ALK",
        "GM","F","TSN","ADM","GIS","K","HSY","SJM","KR","WBA"
        ]
        
        #grab the user inputted codes and add them to the list.
        userInputs = identifier.split(",")
        companies.extend(userInputs)

        #then assign 5 Spark workers to get the data...
        rdd = sc.parallelize(companies, 5)
        rdd.foreachPartition(worker_task.process_partition)
        #sucess message
        st.success("sucessfully put data into the DB, now use the next row to show some of it")
    except Exception as e:
        st.exception(RuntimeError(f"Exception: {e}"))

st.markdown("---")
#this lets you skip the data insertion step after you have already done it once.
st.write("fill this field with a list of shorthands for any companies you wish to see analasys on after putting them into the database. Press button when ready")
grabber = st.text_input('the companies you want to get INFO about.', key="grabber")

#when the button is clicked, run the code
if st.button("Fetch Data"):

    #grab the companies the uses DEMANDED be shown.
    userInputs = grabber.split(",")

    for comp in userInputs:
        #decided to make the chart generation into a function for easier handling and more consistency across charts
        #also passing the Cassandra session variable into them to not create duplicate connections (saves on ram)
        chartGen.ChartGen(comp, session)