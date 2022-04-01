from socket import create_connection
import streamlit as st
import duckdb
from urllib.request import urlretrieve
import pyarrow.dataset as ds
from time import time

@st.cache
def download_dataset():
    url = 'https://github.com/cwida/duckdb-data/releases/download/v1.0/lineitemsf1.snappy.parquet'
    dst = 'lineitemsf1.snappy.parquet'
    urlretrieve(url, dst)
    return dst

def run_timed_query(title, desc, query, connection):
    st.header(title)
    st.write(desc)
    st.code(query)
    start = time()
    results = connection.execute(query).fetchdf()
    end = time()
    st.write(f'Query took {end - start} seconds')
    st.write(results)

# First we'll download the dataset
download_dataset()

connection = duckdb.connect()

run_timed_query("Simple aggregation", "This query uses projection pushdown",
"""SELECT sum(l_extendedprice * l_discount) AS revenue
    FROM 'lineitemsf1.snappy.parquet';""", connection)