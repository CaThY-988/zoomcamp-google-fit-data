import os
from pathlib import Path
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
from databricks import sql

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

st.title("🏃‍♂️ Health & Activity Dashboard")

if st.button("Refresh data"):
    st.cache_data.clear()

def load_data():
    conn = sql.connect(
        server_hostname=st.secrets["DATABRICKS_HOST"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"],
    )
    
    query = """
        SELECT *
        FROM workspace.analytics.mart_googlefit
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()

# --- Chart 1 ---
st.subheader("Activity vs Cardiometabolic Risk")
activity = df.groupby("activity_level")["cardiometabolic_risk_state"].mean()
st.bar_chart(activity)

# --- Chart 2 ---
st.subheader("Sleep vs Cardiometabolic Risk")
sleep = df.groupby("sleep_quality")["cardiometabolic_risk_state"].mean()
st.bar_chart(sleep)

# --- Chart 3 ---
st.subheader("Wellness Score Over Time")
df["date"] = pd.to_datetime(df["date"])
trend = df.groupby("date")["wellness_score"].mean()
st.line_chart(trend)