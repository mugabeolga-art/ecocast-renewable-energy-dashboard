import pandas as pd
from sqlalchemy import create_engine

from dash import Dash, dcc, html, Input, Output
import plotly.express as px


# =====================================================
# DATABASE CONNECTION
# =====================================================

DATABASE_URL = (
    "postgresql://postgres:Olalekan1996*@localhost:5433/ecocast_db"
)

engine = create_engine(DATABASE_URL)


# =====================================================
# LOAD DATA FROM POSTGRESQL
# =====================================================

def load_dashboard_data():
    query = """
        SELECT
            w.forecast_id,
            w.location_id,
            l.city_name,
            l.latitude,
            l.longitude,
            l.timezone,
            w.forecast_time,
            w.temperature,
            w.cloud_cover,
            w.wind_speed,
            w.wind_direction,
            s.solar_score,
            s.wind_score,
            s.recommendation
        FROM weather_forecasts w
        JOIN locations l
            ON w.location_id = l.location_id
        JOIN renewable_scores s
            ON w.forecast_id = s.forecast_id
        ORDER BY w.forecast_time;
    """

    df = pd.read_sql(query, engine)
    df["forecast_time"] = pd.to_datetime(df["forecast_time"])
    df["forecast_date"] = df["forecast_time"].dt.date
    df["forecast_hour"] = df["forecast_time"].dt.hour

    return df


df = load_dashboard_data()


# =====================================================
# DASH APP
# =====================================================

app = Dash(__name__)
app.title = "EcoCast Renewable Energy Dashboard"


# =====================================================
# STYLING
# =====================================================

CARD_STYLE = {
    "backgroundColor": "white",
    "padding": "20px",
    "borderRadius": "12px",
    "boxShadow": "0 4px 10px rgba(0,0,0,0.08)",
    "textAlign": "center",
    "width": "23%",
}

SECTION_STYLE = {
    "backgroundColor": "white",
    "padding": "25px",
    "borderRadius": "12px",
    "boxShadow": "0 4px 10px rgba(0,0,0,0.08)",
    "marginBottom": "25px",
}


# =====================================================
# APP LAYOUT
# =====================================================

app.layout = html.Div(
    style={
        "fontFamily": "Arial, sans-serif",
        "backgroundColor": "#f4f6f9",
        "padding": "30px",
    },
    children=[
        html.Div(
            style={
                "backgroundColor": "#1B2A41",
                "color": "white",
                "padding": "30px",
                "borderRadius": "14px",
                "marginBottom": "25px",
            },
            children=[
                html.H1(
                    "EcoCast Renewable Energy Analytics Dashboard",
                    style={"marginBottom": "5px"},
                ),
                html.P(
                    "Interactive dashboard connected to PostgreSQL for weather forecasting and renewable energy potential analysis.",
                    style={"fontSize": "17px"},
                ),
            ],
        ),

        html.Div(
            style=SECTION_STYLE,
            children=[
                html.H3("Filter Dashboard"),
                html.Label("Select City"),
                dcc.Dropdown(
                    id="city-filter",
                    options=[
                        {"label": "All Cities", "value": "ALL"}
                    ]
                    + [
                        {"label": city, "value": city}
                        for city in sorted(df["city_name"].unique())
                    ],
                    value="ALL",
                    clearable=False,
                    style={"width": "50%"},
                ),
            ],
        ),

        html.Div(
            id="kpi-cards",
            style={
                "display": "flex",
                "justifyContent": "space-between",
                "marginBottom": "25px",
            },
        ),

        html.Div(
            style=SECTION_STYLE,
            children=[
                html.H2("Weather Forecast Trends"),
                dcc.Graph(id="temperature-chart"),
                dcc.Graph(id="wind-chart"),
            ],
        ),

        html.Div(
            style=SECTION_STYLE,
            children=[
                html.H2("Renewable Energy Potential"),
                dcc.Graph(id="solar-wind-chart"),
                dcc.Graph(id="recommendation-chart"),
            ],
        ),

        html.Div(
            style=SECTION_STYLE,
            children=[
                html.H2("City Comparison"),
                dcc.Graph(id="city-comparison-chart"),
            ],
        ),

        html.Div(
            style={
                "textAlign": "center",
                "color": "#666",
                "marginTop": "30px",
            },
            children=[
                html.P(
                    "EcoCast MVP Dashboard | PostgreSQL + Dash + Plotly"
                )
            ],
        ),
    ],
)


# =====================================================
# CALLBACK
# =====================================================

@app.callback(
    [
        Output("kpi-cards", "children"),
        Output("temperature-chart", "figure"),
        Output("wind-chart", "figure"),
        Output("solar-wind-chart", "figure"),
        Output("recommendation-chart", "figure"),
        Output("city-comparison-chart", "figure"),
    ],
    Input("city-filter", "value"),
)
def update_dashboard(selected_city):

    if selected_city == "ALL":
        filtered_df = df.copy()
    else:
        filtered_df = df[df["city_name"] == selected_city].copy()

    # ============================
    # KPI CALCULATIONS
    # ============================

    avg_temp = round(filtered_df["temperature"].mean(), 2)
    avg_wind = round(filtered_df["wind_speed"].mean(), 2)
    avg_solar = round(filtered_df["solar_score"].mean(), 2)
    total_records = len(filtered_df)

    best_city_df = (
        filtered_df.groupby("city_name", as_index=False)
        .agg(avg_renewable_score=("solar_score", "mean"))
        .sort_values("avg_renewable_score", ascending=False)
    )

    best_city = (
        best_city_df.iloc[0]["city_name"]
        if not best_city_df.empty
        else "N/A"
    )

    kpi_cards = [
        html.Div(
            style=CARD_STYLE,
            children=[
                html.H4("Average Temperature"),
                html.H2(f"{avg_temp}°"),
                html.P("Forecast average"),
            ],
        ),
        html.Div(
            style=CARD_STYLE,
            children=[
                html.H4("Average Wind Speed"),
                html.H2(f"{avg_wind}"),
                html.P("Wind speed forecast"),
            ],
        ),
        html.Div(
            style=CARD_STYLE,
            children=[
                html.H4("Average Solar Score"),
                html.H2(f"{avg_solar}"),
                html.P("Solar potential score"),
            ],
        ),
        html.Div(
            style=CARD_STYLE,
            children=[
                html.H4("Forecast Records"),
                html.H2(f"{total_records}"),
                html.P(f"Best city: {best_city}"),
            ],
        ),
    ]

    # ============================
    # FIGURES
    # ============================

    temperature_fig = px.line(
        filtered_df,
        x="forecast_time",
        y="temperature",
        color="city_name",
        title="Temperature Forecast Over Time",
        labels={
            "forecast_time": "Forecast Time",
            "temperature": "Temperature",
            "city_name": "City",
        },
    )

    wind_fig = px.line(
        filtered_df,
        x="forecast_time",
        y="wind_speed",
        color="city_name",
        title="Wind Speed Forecast Over Time",
        labels={
            "forecast_time": "Forecast Time",
            "wind_speed": "Wind Speed",
            "city_name": "City",
        },
    )

    solar_wind_fig = px.scatter(
        filtered_df,
        x="solar_score",
        y="wind_score",
        color="city_name",
        size="wind_speed",
        hover_data=[
            "forecast_time",
            "temperature",
            "cloud_cover",
            "recommendation",
        ],
        title="Solar vs Wind Renewable Potential",
        labels={
            "solar_score": "Solar Score",
            "wind_score": "Wind Score",
            "city_name": "City",
        },
    )

    recommendation_fig = px.pie(
        filtered_df,
        names="recommendation",
        title="Renewable Energy Recommendation Distribution",
    )

    city_summary = (
        filtered_df.groupby("city_name", as_index=False)
        .agg(
            avg_temperature=("temperature", "mean"),
            avg_wind_speed=("wind_speed", "mean"),
            avg_solar_score=("solar_score", "mean"),
            avg_wind_score=("wind_score", "mean"),
        )
    )

    city_summary["avg_renewable_score"] = (
        city_summary["avg_solar_score"]
        + city_summary["avg_wind_score"]
    ) / 2

    city_comparison_fig = px.bar(
        city_summary,
        x="city_name",
        y="avg_renewable_score",
        title="Average Renewable Energy Score by City",
        labels={
            "city_name": "City",
            "avg_renewable_score": "Average Renewable Score",
        },
    )

    # Layout polish
    for fig in [
        temperature_fig,
        wind_fig,
        solar_wind_fig,
        recommendation_fig,
        city_comparison_fig,
    ]:
        fig.update_layout(
            template="plotly_white",
            title_x=0.02,
            margin=dict(l=40, r=40, t=70, b=40),
        )

    return (
        kpi_cards,
        temperature_fig,
        wind_fig,
        solar_wind_fig,
        recommendation_fig,
        city_comparison_fig,
    )


# =====================================================
# RUN SERVER
# =====================================================

if __name__ == "__main__":
    app.run(debug=True, port=8051)