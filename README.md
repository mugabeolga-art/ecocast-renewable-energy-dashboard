# EcoCast Renewable Energy Analytics Dashboard

## Overview

EcoCast is an end-to-end data engineering and analytics solution that extracts weather forecast data from the Open-Meteo API, transforms and validates the data through a Python ETL pipeline, stores the results in PostgreSQL, and presents actionable renewable energy insights through an interactive Dash dashboard.

## Key Features

* Automated ETL pipeline
* PostgreSQL database integration
* Interactive Dash analytics application
* Real-time weather and renewable energy insights
* Dynamic filtering by city
* KPI summary cards
* Renewable energy recommendation engine

## Business Insights

The dashboard helps decision-makers identify locations with the strongest renewable energy potential by analyzing:

* Temperature trends
* Wind speed forecasts
* Cloud cover patterns
* Solar energy potential
* Wind energy potential
* Renewable readiness scores

Users can compare multiple cities and evaluate renewable energy opportunities using interactive visualizations.

## Technology Stack

* Python
* PostgreSQL
* SQLAlchemy
* Pandas
* Dash
* Plotly
* Open-Meteo API

## Running the Application

1. Start PostgreSQL
2. Run the ETL pipeline

python ecocast_etl.py

3. Launch the dashboard

python app.py

4. Open

http://127.0.0.1:8051
