import pandas as pd
import numpy as np
import streamlit as st
from prophet import Prophet
from xgboost import XGBRegressor
from statsmodels.tsa.seasonal import seasonal_decompose
import sklearn
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from pmdarima import auto_arima
import matplotlib.pyplot as plt


#def ChartGenSARIMAX(row):
#    try:
#        # Load data
#        compYearDataPD = pd.read_json(row.company_year)
#        compOpenPrices = compYearDataPD[['Open']].copy()
#
#        model = SARIMAX(compOpenPrices['Open'], order=(0,1,1), seasonal_order=(0,1,1,252))
#        results = model.fit()
#        st.subheader(f"SARIMAX implementation")
#        st.write(results)
#        st.markdown("---")
#    except Exception as e:
#            st.error(f"SARIMAX unaccounted for exception: {e}")
def ChartGenARIMA(row):

    # Load data
    compYearDataPD = pd.read_json(row.company_year)
    series = compYearDataPD['Open']

    # Subheader for the anlysis looks more nice on UI
    st.subheader("ARIMA / SARIMA Analysis")

    # ADF tests
    result = adfuller(series)

    st.write("ADF Statistic:", result[0])
    st.write("p-value:", result[1])

    # Differencing check from the ADF

    # if fail to reject H₀
    if result[1] > 0.05:
        st.write("Series is non-stationary → applying differencing")
        series_diff = series.diff().dropna()

    #Else reject H₀
    else:
        st.write("Series is stationary")
        series_diff = series

    # Plot for more visual context
    fig1, ax1 = plt.subplots()
    ax1.plot(series_diff)
    ax1.set_title("Differenced Series")
    st.pyplot(fig1)

    # ACF / PACF
    st.write("ACF Plot")
    st.pyplot(plot_acf(series_diff))

    st.write("PACF Plot")
    st.pyplot(plot_pacf(series_diff))

    # AUTO ARIMA
    auto_model = auto_arima(
        series,
        seasonal=True,
        #stock are traded weekly mon-fri hence 5 selcted as seasonal period
        m=5,
        suppress_warnings=True
    )

    #Use for debug only, prints whole summary of SARIMAX ugly on UI
    #st.text(auto_model.summary())

    order = auto_model.order
    seasonal_order = auto_model.seasonal_order
    # SARIMAX
    model = SARIMAX(
        series,
        order=order,
        seasonal_order=seasonal_order
    )

    model_fit = model.fit()

    st.subheader("SARIMAX Key Results")

    #Create 3 columns for better visuals
    col1, col2, col3 = st.columns(3)

    # Showing key insights of SARIMAX
    with col1:
        order = model_fit.model.order
        seasonal_order = model_fit.model.seasonal_order
        st.metric("Order", f"{order}")
    with col2:
        st.metric("AIC", f"{model_fit.aic:.2f}")
    with col3:
        st.metric("BIC", f"{model_fit.bic:.2f}")

    #Coefficients table
    params = model_fit.params
    pvalues = model_fit.pvalues

    summary_df = pd.DataFrame({
    "Coefficient": params,
    "p-value": pvalues
    })

    st.write("### Coefficients")
    st.dataframe(summary_df)
    ssignificant = summary_df[summary_df["p-value"] < 0.05]
    st.write("### Significant Terms (p < 0.05)")
    st.dataframe(ssignificant)



    # Use for debug only, prints whole summary of SARIMAX ugly on UI
    #st.text(model_fit.summary())

    # Forecast of the stocks
    forecast = model_fit.forecast(steps=30)

    fig2, ax2 = plt.subplots()
    ax2.plot(series, label="Original")
    ax2.plot(forecast, label="Forecast")
    ax2.legend()
    st.pyplot(fig2)

    st.markdown("---")

def ChartGenProphetXGB(row):

    # Load data
    compYearDataPD = pd.read_json(row.company_year)
    compOpenPrices = compYearDataPD[['Open']].copy()

    # Subheader for the anlysis looks more nice on UI
    #st.subheader("Prophet Analysis") Wrong place but we can look if it looks nicer with these


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
    xgb_model = XGBRegressor(n_estimators=100,max_depth=5,learning_rate=0.1)
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
   
    # Output
    st.pyplot(fig1)
    st.markdown("---")

        

def ChartGenProphet(row):
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

    #and write it all out for the user to see
    st.pyplot(fig1)
    st.markdown("---")

def ChartGenSeasonal(row):
   
    #first, grab the year data, only the Open column 
    compYearDataPD = pd.read_json(row.company_year)
    compOpenPrices = compYearDataPD[['Open']]
    
    #grab the volume of traded stocks
    compVolumes = compYearDataPD[['Volume']]

    #check if there is a yearly seasonality
    result = seasonal_decompose(compOpenPrices['Open'], model='additive', period=252)
    priceplot = result.plot()
    result = seasonal_decompose(compVolumes['Volume'], model='additive', period=252)
    volplot = result.plot()
    st.subheader(f"Seasonality over a year in stock price (if any):")
    st.pyplot(priceplot)
    st.markdown("---")
    st.subheader(f"Seasonality over a year in stock trade volume (if any):")
    st.pyplot(volplot)
    st.markdown("---")
        





#TODO: ARIMA chart generator, P Q testing on ARIMA, 
#TODO: basis function for Arima, AIC score, SARIMA (seasonal).

