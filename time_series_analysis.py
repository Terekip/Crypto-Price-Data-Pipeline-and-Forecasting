from fastapi import FastAPI, Response
import plotly.express as px
import plotly.io as pio
from prophet import Prophet
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
conn= psycopg2.connect(
     user ='avnadmin',
     password='AVNS_5fLPUVkBBuUzmOuroVq',
     host='pg-3700d966-gilbert-c4d7.c.aivencloud.com',
     port='26765',
     database='defaultdb')
#engine = create_engine( f"postgresql://{user}:{password}@{host}:{port}/{database}")

app = FastAPI()

# Load data from database with proper connection
df = pd.read_sql("SELECT * FROM crypto_prices",conn)


# Define the analyze_and_forecast_crypto function
def analyze_and_forecast_crypto(df, coin=None, forecast_hours=24):
    #df = pd.read_sql("SELECT * FROM crypto_prices",conn)
    forecasts = {}
    df = df.rename(columns={'timestamp': 'ds'})  # ✅ Fix: Prophet expects 'ds'
    df = df.dropna(subset=['ds'])  # just in case    
    # Drop the original timestamp column if not needed (though 'ds' is used instead)
    df = df.drop(columns=['timestamp'], errors='ignore')
    # If coin is None, analyze all columns except 'ds'
    coins = [coin] if coin else [col for col in df.columns if col != 'ds']
    for coin in coins:
        print(f"Analyzing {coin}...")
        # Prepare data for Prophet
        df_coin = df[['ds', coin]]  # Select timestamp and price column
        df_coin.columns = ['ds', 'y']
        # Initialize and fit Prophet
        model = Prophet(
            seasonality_prior_scale=4.0,
            daily_seasonality=True,
            weekly_seasonality=False,
            yearly_seasonality=False,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.3
             )
        model.add_seasonality(name='hourly', period=1, fourier_order=3)
        model.fit(df_coin)
        # Forecast next 24 hours
        future = model.make_future_dataframe(periods=forecast_hours, freq='H')
        forecast = model.predict(future)
        forecasts[coin] = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        # Plot with Plotly and convert to PNG
        fig = px.line(title=f'{coin.capitalize()} Price Forecast (Next 24 Hours)')
        recent_data = df_coin.tail(72)  # Last 72 hours
        fig.add_scatter(x=recent_data['ds'], y=recent_data['y'], name='Actual',
                        mode='lines')
        fig.add_scatter(x=forecast['ds'].tail(24), y=forecast['yhat'].tail(24),
                        name='Forecast', mode='lines')
        fig.update_layout(xaxis_title='Date', yaxis_title='Price (USD)')
        # Convert Plotly figure to PNG bytes
        html_str = pio.to_html(fig, full_html=False)
        #img_bytes = pio.to_image(fig, format='png', width=800, height=600)
        # Serve the image
        return Response(content=html_str, media_type="text/html")
        #yield Response(content=img_bytes, media_type="image/png")

# API Endpoint to analyze and forecast
@app.get("/forecast/{coin}")
async def get_forecast(coin: str, forecast_hours: int = 24):
    # Validate coin exists in DataFrame
    if coin not in df.columns or coin == 'ds':
        return Response(content=b"Invalid coin name", media_type="text/plain",
                        status_code=400)
    
    # Generate forecasts and images
    return analyze_and_forecast_crypto(df, coin, forecast_hours)

# Run the application with uvicorn if this is the main module
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)