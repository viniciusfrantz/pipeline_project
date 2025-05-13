import streamlit as st
import altair as alt
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
object_key = 'weather_forecast/results_forecast_export'

# Load and prepare data
df = load_data_from_s3(bucket_name, object_key)

df = df.reset_index(drop=True)
df['DATE'] = pd.to_datetime(df['DATE']).dt.date

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
    future_data = df[df['DATE'] >= today].drop(columns=['PRECIPITACAO'])
    
    # Show the forecast data
    st.title("Previsão do tempo - Granja Santa Catarina (Próximos Dias)")
    st.dataframe(format_float_columns(future_data), use_container_width=True)

elif data_view == "Dados Históricos":
    # Filter data to show past dates and include all columns
    historical_data = df[df['DATE'] < today]
    historical_data['DATE'] = pd.to_datetime(historical_data['DATE'], errors='coerce')
    historical_data['DIA_PREVISTO'] = historical_data['DATE'].dt.strftime('%d-%m-%Y')
    historical_data.drop(columns=['DATE'], inplace=True)
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
    df_mensal['DATE'] = pd.to_datetime(df_mensal['DATE'])
    df_mensal['ano'] = df_mensal['DATE'].dt.year
    df_mensal['mes'] = df_mensal['DATE'].dt.month
    df_agg_prec_mensal = (
        df_mensal
        .groupby(['ano', 'mes'])['PRECIPITACAO']
        .sum()
        .reset_index()
    )

    df_agg_prec_mensal['mes_ano'] = df_agg_prec_mensal['mes'].astype(str).str.zfill(2) + '/' + df_agg_prec_mensal['ano'].astype(str)

    # Ordena por ano e mês
    df_agg_prec_mensal = df_agg_prec_mensal.sort_values(by=['ano', 'mes'])

    # Limites do eixo Y
    y_max = df_agg_prec_mensal['PRECIPITACAO'].max() + 20

    bars = alt.Chart(df_agg_prec_mensal).mark_bar(color='steelblue').encode(
        x=alt.X('mes_ano:N', title='Mês/Ano', sort=None),
        y=alt.Y('PRECIPITACAO:Q', title='Precipitação acumulada (mm)', scale=alt.Scale(domain=[0, y_max]))
    )

    # Rótulos acima das barras
    labels = alt.Chart(df_agg_prec_mensal).mark_text(
        align='center',
        baseline='bottom',
        dy=-2,  # distância do topo da barra
        fontSize=16,
        color='blue'
    ).encode(
        x='mes_ano:N',
        y='PRECIPITACAO:Q',
        text=alt.Text('PRECIPITACAO:Q', format='.1f')
    )

    # Combina os dois gráficos
    chart = (bars + labels).properties(
        width=700,
        height=400,
        title='Precipitação Acumulada por Mês'
    )

    # Exibe no Streamlit
    st.title("Precipitação acumulada por mês (mm) Granja Santa Catarina")
    st.altair_chart(chart, use_container_width=True)
