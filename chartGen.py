import pandas as pd
import numpy as np
import streamlit as st
from prophet import Prophet
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split

def ChartGenProphetXGB(compID: str, session):
    try:
        # grab the companies DATA
        query = "SELECT * FROM companies WHERE company_id = %s"
        row = session.execute(query, (compID,)).one()

        if row:
            # Load data
            compYearDataPD = pd.read_json(row.company_year)
            compOpenPrices = compYearDataPD[['Open']].copy()

            # Prepare Prophet format
            compOpenPrices = compOpenPrices.rename(columns={'Open': 'y'})
            compOpenPrices["ds"] = pd.date_range(end=pd.Timestamp.today().normalize(),periods=len(compOpenPrices),freq="D")

            # Create lag features
            df_ml = compOpenPrices.copy()
            for lag in range(1, 6):
                df_ml[f"lag_{lag}"] = df_ml["y"].shift(lag)

            df_ml = df_ml.dropna()

            X = df_ml[[f"lag_{i}" for i in range(1, 6)]]
            y = df_ml["y"]

            # Train XGBoost
            xgb_model = XGBRegressor(n_estimators=100,max_depth=3,learning_rate=0.1)
            xgb_model.fit(X, y)

            # Generate predictions for training data
            df_ml["xgb_pred"] = xgb_model.predict(X)

            # Merge back into Prophet dataframe
            compOpenPrices = compOpenPrices.merge(
                df_ml[["ds", "xgb_pred"]],
                on="ds",
                how="left"
            )

            # Fill missing values (start of series)
            compOpenPrices["xgb_pred"] = compOpenPrices["xgb_pred"].fillna(method="bfill")

            #then do prophet
            model = Prophet()
            model.add_regressor("xgb_pred")

            model.fit(compOpenPrices)

            # Create future dataframe
            future_pd = model.make_future_dataframe(periods=90, freq='d', include_history=False)

            # Generate future XGB predictions
            last_values = compOpenPrices["y"].values[-5:].tolist()
            future_xgb_preds = []

            for _ in range(len(future_pd)):
                features = np.array(last_values[-5:]).reshape(1, -1)
                pred = xgb_model.predict(features)[0]
                future_xgb_preds.append(pred)
                last_values.append(pred)

            future_pd["xgb_pred"] = future_xgb_preds

            # Forecast
            forecast_pd = model.predict(future_pd)

            fig1 = model.plot(forecast_pd)

            # Company info
            compInfoPD = pd.read_json(row.company_info)
            name = compInfoPD[['longName']].iloc[0][0]

            oppMargins = round(float(compInfoPD[['operatingMargins']].iloc[0][0]) * 100, 2)
            grossMargins = round(float(compInfoPD[['grossMargins']].iloc[0][0]) * 100, 2)
            dte = round(float(compInfoPD[['debtToEquity']].iloc[0][0]), 2)

            # Output
            st.subheader(f"{name}")
            st.pyplot(fig1)
            st.write(f"operating margins: {oppMargins}% | gross margins: {grossMargins}% | Debt to Equity ratio: {dte}%")
            st.markdown("---")

        else:
            st.error(f"{compID} does not exist in the Database.")

    except ValueError:
        st.error(f"{compID} exists in database, but has corrupt JSON data, skipping")
    except KeyError:
        st.error(f"{compID} exists in database, but missing data, skipping")
    except Exception as e:
        st.error(f"{compID} unaccounted for exception: {e}")

def ChartGenProphet(compID:str, session):
    try:
        #grab the companies DATA
        query = "SELECT * FROM companies WHERE company_id = %s"
        row = session.execute(query, (compID,)).one()

        #if something existed with that id, generate it, or make an error message instead
        if row:
            #first, grab the year data, only the Open column 
            compYearDataPD = pd.read_json(row.company_year)
            compOpenPrices = compYearDataPD[['Open']]
            
            #grab the volume of traded stocks (unimplemented)
            compVolumes = compYearDataPD[['Volume']]

            #reformat and create a column with dates for Prophet to latch onto.
            compOpenPrices = compOpenPrices.rename(columns={'Open': 'y'})
            compOpenPrices["ds"] = pd.date_range(end=pd.Timestamp.today().normalize(), periods=len(compOpenPrices), freq="D")
    
            #Prophet.
            model = Prophet()
            model.fit(compOpenPrices)

            #predict and plot.
            future_pd = model.make_future_dataframe(periods=90,freq='d',include_history=False)
            forecast_pd = model.predict(future_pd)
            fig1 = model.plot(forecast_pd)

            #find and show info about the company.
            compInfoPD = pd.read_json(row.company_info)

            name = compInfoPD[['longName']].iloc[0][0]

            #convert some numbers into more readable percentages.
            oppMargins =  round((float(compInfoPD[['operatingMargins']].iloc[0][0]) *100),2)
            grossMargins = round((float(compInfoPD[['grossMargins']].iloc[0][0])*100),2)
            dte = round(float(compInfoPD[['debtToEquity']].iloc[0][0]),2)
            #and write it all out for the user to see
            st.subheader(f"{name}")
            st.pyplot(fig1) 
            st.write(f"operating margins: {oppMargins}% | gross margins: {grossMargins}% | Debt to Equity ratio: {dte}%")
            st.markdown("---")
        else:
            st.error(f"{compID} does not exist in the Database.")
    except ValueError:
       st.error(f"{compID} exists in database, but has corrupt JSON data, skipping")
    except KeyError:
        st.error(f"{compID} exists in database, but missing data, skipping")
    except Exception as e:
        st.error(f"{compID} unnacounted for exception: {e}") 


#TODO: ARIMA chart generator, P Q testing on ARIMA, 
#TODO: XGBoost chart generator, applied to prophet -!-!-!-!-
#TODO: "trend decompose" chart generator
#TODO: basis function for Arima, AIC score, SARIMA (seasonal).
#TODO: check if stock is seasonal.
#TODO: user input, and server integration CHECK!!!!