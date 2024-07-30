import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd

st.title("Food Consumption Metrics Dashboard")
session = get_active_session()


# This function queries data from Snowflake
def get_food_consumption_data():
    query = session.table("AIRLINE.INSIGHTS.FOOD_CONSUMPTION_METRICS")
    df = query.select(
        col("FLIGHTNUMBER"),
        col("FLIGHTDATE"),
        col("TOTALPASSENGERS"),
        col("TOTALSOLDITEMS"),
        col("TOTALSALES"),
        col("AVERAGESOLDITEMSPERPASSENGER"),
        col("AVERAGESALESPERPASSENGER")
    ).to_pandas()
    return df


# Get the food consumption data
food_consumption_data = get_food_consumption_data()

# Display the data in a table and create bar charts
st.subheader("Food Consumption Metrics")
st.dataframe(food_consumption_data, use_container_width=True)

st.subheader("Total Sold Items per Flight")
st.bar_chart(data=food_consumption_data.rename(columns={"FLIGHTNUMBER": "FlightNumber", "TOTALSOLDITEMS": "TotalSoldItems"}), x="FlightNumber", y="TotalSoldItems")

st.subheader("Total Sales per Flight")
st.bar_chart(data=food_consumption_data.rename(columns={"FLIGHTNUMBER": "FlightNumber", "TOTALSALES": "TotalSales"}), x="FlightNumber", y="TotalSales")

st.subheader("Average Sold Items per Passenger")
st.bar_chart(data=food_consumption_data.rename(columns={"FLIGHTNUMBER": "FlightNumber", "AVERAGESOLDITEMSPERPASSENGER": "AverageSoldItemsPerPassenger"}), x="FlightNumber", y="AverageSoldItemsPerPassenger")

st.subheader("Average Sales per Passenger")
st.bar_chart(data=food_consumption_data.rename(columns={"FLIGHTNUMBER": "FlightNumber", "AVERAGESALESPERPASSENGER": "AverageSalesPerPassenger"}), x="FlightNumber", y="AverageSalesPerPassenger")
