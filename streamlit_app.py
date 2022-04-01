import streamlit as st
import duckdb
import pyarrow.dataset as ds

@st.cache
def get_connection():

    # Gets Database Connection
    return duckdb.connect()

@st.cache
def get_dataset():
    return ds.dataset('s3://ursa-labs-taxi-data/', partitioning=["year", "month"])


nyc_dataset = get_dataset()
con = get_connection()
query = con.execute("SELECT * FROM nyc_dataset limit 10")

st.write(query.fetchdf())