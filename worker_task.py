import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta, timezone
from cassandra.cluster import Cluster

def comp_exists(compid: str) -> bool:
    try:
        stock = yf.Ticker(compid)
        info = stock.fast_info
        return info is not None
    except Exception:
        return False


def process_partition(company_ids):

    #Worker connects to Cassandra
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('stocks')

    #prepare statement for putting data into the DB
    insert_stmt = session.prepare("""
        INSERT INTO companies
        (company_ID, company_Info, company_Year, company_Month, company_Day, last_update_timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    #establish what time it is.
    now = datetime.now()
    yesterday = now - timedelta(days=1)

    #for each task given to the worker.
    for compID in company_ids:
        try:
            # check if the company is already in the DB
            query = """
            SELECT company_id, last_update_timestamp
            FROM companies
            WHERE company_id = %s
            """

            rows = session.execute(query, (compID,))
            rows_list = list(rows)

            #if the company already Exists.
            if len(rows_list) > 0:
                last_update = rows_list[0].last_update_timestamp
                #If the timestamp is OLDER than 1 day.
                if last_update and last_update > yesterday:
                    #then update it, otherwise skip.
                    continue

            #checks if Yfinance has this company.
            if not comp_exists(compID):
                continue
            
            #if those checks pass, get the data from Yfinance
            data = yf.Ticker(compID)

            info = pd.json_normalize(data.info)

            month = data.history(period='1mo')
            year = data.history(period='1y')
            day = data.history(period='3d')

            #then put all the gathered data that we are interested in, into the DB
            session.execute(insert_stmt, (
                compID,
                info.to_json(),
                year.to_json(orient="records"),
                month.to_json(orient="records"),
                day.to_json(orient="records"),
                now
            ))

        except Exception as e:
            #this exception will not be visible.
            #its here so any bad fetch statements simply get ignored.
            print(f"error while trying to get {compID}: {e}")

    #shut down the connections to not take up resources.
    session.shutdown()
    cluster.shutdown()

#this is here in case someone wants to try to run this file by CMD directly.
#takes in a py list of company shorthands
def main(string:str):
    process_partition(string)