import streamlit as st
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection info
engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')
# Query data
query = "SELECT * FROM weather ORDER BY time ASC"
df = pd.read_sql(query, con=engine)

# UI Layout
st.title("🌤️ Weather Dashboard")
st.write(f"Total records: {len(df)}")

if not df.empty:
    latest = df.iloc[-1]
    st.metric("🌡️ Temperature", f"{latest['temperature']} °C", help=latest['time'])
    st.metric("💨 Windspeed", f"{latest['windspeed']} km/h", help=latest['time'])

    df['time'] = pd.to_datetime(df['time'])

    st.line_chart(df.set_index('time')[['temperature']], use_container_width=True)
    st.line_chart(df.set_index('time')[['windspeed']], use_container_width=True)
else:
    st.warning("No data yet. Trigger your Airflow DAG first!")

