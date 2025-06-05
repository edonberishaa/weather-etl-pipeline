
---

## ☁️ Project 2: `weather-etl-pipeline`
# 🌤️ Weather ETL Pipeline with Airflow & Streamlit

This project is a real-time ETL pipeline that extracts live weather data from the Open-Meteo API, processes it using **Apache Airflow**, and stores it in a PostgreSQL database. A **Streamlit dashboard** visualizes the latest weather conditions.

## 📖 Description

The pipeline is made up of three key stages:

1. **Extract**: Pulls live weather data (temperature, wind speed) from an external API.
2. **Transform**: Cleans the raw JSON and converts it to a CSV format.
3. **Load**: Loads the data into a PostgreSQL table with deduplication.

## 🧰 Tools & Technologies

- Apache Airflow
- Python 3
- PostgreSQL
- Streamlit
- Docker & Docker Compose

  
## Features
- Hourly scheduling with Airflow
- Real-time weather fetching
- Dynamic dashboard showing:
- Current temperature
- Wind speed
- Historical weather trends


## 🚀 Getting Started

### 1. Start the ETL pipeline
```bash
docker compose up -d
Access Airflow at: http://localhost:8080
Run the dashboard:
streamlit run dashboards/weather_dashboard.py


