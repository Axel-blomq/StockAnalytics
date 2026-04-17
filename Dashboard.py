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

    #and here we start finding other semi random companies to show.
    ALLcomps = pd.DataFrame()
    frames = []
    bad_rows = []

    #take out all the company ids and put them in a long list
    rows = session.execute('SELECT company_id, company_info, company_year FROM companies')

    st.info("gathering other interesting companies")

    for row in rows:
        try:
            #just grab the id and put it in the list
            data = {row.company_id:'0'}
            ser = pd.Series(data=data, index=[f"{row.company_id}"])

            #trying to access the same variables we are using later, the except clause will filter out any broken items from the list.
            compInfoPD = pd.read_json(row.company_info)

            name = compInfoPD[['longName']].iloc[0][0]
            oppMargins = round((float(compInfoPD[['operatingMargins']].iloc[0][0]) *100),2)
            grossMargins = round((float(compInfoPD[['grossMargins']].iloc[0][0])*100),2)
            dte = round(float(compInfoPD[['debtToEquity']].iloc[0][0]),2)

            frames.append(ser)
        #any exceptions caused by this should just be ignored as part of the data fetching.
        except ValueError:
            continue   #bad JSON
        except KeyError:
            continue   #missing column in DB
        except Exception as e:
            #this clause is here to catch any odd exeptions and put them into a list before Keeping going on
            bad_rows.append((row.company_id, str(e)))

    st.info("this step can take a bit, please hold.")

    #put everything into one BIG pandas df for easier handling.
    ALLcomps = pd.concat(frames, axis=0)

    #pick 20 random companies, do predictions on all of them with Prophet.
    #pick the top 2 best highest valued and the 1 lowest valued acording to prophet
    #then show them alongside whatever the user put as their input, or just nothing if they didnt exist/didnt give an input.

    #assign some variables to keep track of the companies.
    bestComp = None
    bestCompValue = 0
    bestCompName = None

    secondComp = None
    secondCompValue = 0
    secondCompName = None

    worstComp = None
    worstCompValue = float("inf")
    worstCompName = None

    #select some companies at random from the data, set to a lower number for this to be faster.
    randomComps = ALLcomps.sample(20, axis=0)

    #and here we pick out the 2 best and 1 worst company.
    for rowName, rowData in randomComps.items():
        try:
            compID = rowName

            #for this step, we are Only interested in the comanys last year data
            query = "SELECT company_year FROM companies WHERE company_id = %s"
            row = session.execute(query, (compID,)).one()
            compYearDataPD = pd.read_json(row.company_year)

            #grab the singular column from the data that we are interested in.
            compOpenPrices = compYearDataPD[['Open']]
            #rename the column to "y" for use with Prophet.
            compOpenPrices = compOpenPrices.rename(columns={'Open': 'y'})
            #create a column with dates for Prophet to latch onto.
            compOpenPrices["ds"] = pd.date_range(end=pd.Timestamp.today().normalize(), periods=len(compOpenPrices), freq="D")
    
            #make Prophet model.
            model = Prophet()
            model.fit(compOpenPrices)

            future_pd = model.make_future_dataframe(periods=90,freq='d',include_history=False)
            forecast_pd = model.predict(future_pd)
            
            #grab the LAST result, we are interested in the yhat value
            last_day_data = forecast_pd.iloc[-1:]
            #and make it into a number you can do math with.
            predictedWorth = float(last_day_data[['yhat']].iloc[0][0])

            #then do comparisons
            #is it better than the Best one?
            if(predictedWorth > bestCompValue):
                #if its a new best company, good.
                #the previous best company is now second best.
                secondComp = bestComp
                secondCompValue = bestCompValue
                secondCompName = bestCompName
                #then the new best is now the best.
                bestComp = model
                bestCompValue = predictedWorth
                bestCompName = compID
            #is it better than the second company?
            elif(predictedWorth > secondCompValue):
                secondComp = model
                secondCompValue = predictedWorth
                secondCompName = compID
            #regardless of the two above:
            #is it worse than the Worst company?
            if(predictedWorth < worstCompValue):
                worstComp = model
                worstCompValue = predictedWorth
                worstCompName = compID
        except ValueError:
            continue   #bad JSON
        except KeyError:
            continue   #missing col
        except Exception as e:
            bad_rows.append((row.company_id, str(e)))

    st.success('Done!')

    st.markdown("---")
    #then display the companies in order of Best, second best, worst.
    st.write("Here are some other companies you might be interested in knowing exist")
    st.caption("for better AND worse.")

    #decided to make the chart generating into a function so they are easy to edit and consistent, 
    #yes i did have to remake it because i want to reuse the Models already created for them in the last step.
    #also passing the Cassandra session variable into them to not create duplicate connections
    chartGen.modelChartGen(bestCompName, bestComp, session)
    
    chartGen.modelChartGen(secondCompName, secondComp, session)

    chartGen.modelChartGen(worstCompName, worstComp, session)