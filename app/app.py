import streamlit as st
import pandas as pd
import boto3
import io
import os
from dotenv import load_dotenv
import datetime

# Load environment variables
load_dotenv()

# Page settings
st.set_page_config(page_title="Weather Dashboard", layout="wide")

# Load data from S3
@st.cache_data
def load_data_from_s3(bucket_name, object_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    buffer = io.BytesIO()
    s3.download_fileobj(bucket_name, object_key, buffer)
    buffer.seek(0)
    df = pd.read_parquet(buffer)
    return df

# S3 bucket and file
bucket_name = 'vinicius-airflow'
object_key = 'weather_forecast/results_forecast_export_0_0_0.snappy.parquet'

# Load and prepare data
df = load_data_from_s3(bucket_name, object_key)

df = df.reset_index(drop=True)
df['FORECAST_DATE'] = pd.to_datetime(df['FORECAST_DATE']).dt.date

# Get today's date
today = datetime.date.today()

# Sidebar - Select data view (forecast or historical)

data_view = st.sidebar.selectbox("Selecione o tipo de informação desejada", ["Previsão do Tempo", "Dados Históricos"])

# Function to highlight precipitation
def highlight_precipitation(row):
    # Only highlight if PRECIPITACAO column exists
    if 'PRECIPITACAO' in row and row['PRECIPITACAO'] > 2:
        return ['background-color: lightblue'] * len(row)
    return [''] * len(row)

# Function to format float columns
def format_float_columns(df):
    float_cols = df.select_dtypes(include='float').columns
    return df.style.format({col: "{:.2f}" for col in float_cols}).apply(highlight_precipitation, axis=1)

# Display data based on selected view
if data_view == "Previsão do Tempo":
    # Filter data to only show future dates and hide the precipitation column
    future_data = df[df['FORECAST_DATE'] >= today].drop(columns=['PRECIPITACAO'])
    
    # Show the forecast data
    st.title("Previsão do tempo - Granja Santa Catarina (Próximos Dias)")
    st.dataframe(format_float_columns(future_data), use_container_width=True)

elif data_view == "Dados Históricos":
    # Filter data to show past dates and include all columns
    historical_data = df[df['FORECAST_DATE'] < today]
    historical_data['FORECAST_DATE'] = pd.to_datetime(historical_data['FORECAST_DATE'], errors='coerce')
    historical_data['DIA_PREVISTO'] = historical_data['FORECAST_DATE'].dt.strftime('%d-%m-%Y')
    historical_data.drop(columns=['FORECAST_DATE'], inplace=True)
    columns = ['DIA_PREVISTO'] + [col for col in historical_data.columns if col != 'DIA_PREVISTO']
    historical_data = historical_data[columns]
    
    # Show the historical data
    st.title("Dados Históricos - Granja Santa Catarina")
    st.dataframe(format_float_columns(historical_data), 
                 use_container_width=True,
                 hide_index=True)
    
    # Export option
    st.download_button(
        label="Download Historical Data",
        data=historical_data.to_csv(index=False).encode('utf-8'),
        file_name='historical_weather_data.csv',
        mime='text/csv'
    )

    df_mensal = df.copy()
    df_mensal['FORECAST_DATE'] = pd.to_datetime(df_mensal['FORECAST_DATE'])
    df_mensal['ano'] = df_mensal['FORECAST_DATE'].dt.year
    df_mensal['mes'] = df_mensal['FORECAST_DATE'].dt.month
    df_agg_prec_mensal = (
        df_mensal
        .groupby(['ano', 'mes'])['PRECIPITACAO']
        .sum()
        .reset_index()
    )
    st.title("Precipitação acumulada por mês")
    st.dataframe(df_agg_prec_mensal, hide_index=True)

