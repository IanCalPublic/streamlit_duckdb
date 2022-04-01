import streamlit as st
import duckdb
from urllib.request import urlretrieve
import pyarrow.dataset as ds


@st.cache
def download_dataset():
    url = 'https://github.com/cwida/duckdb-data/releases/download/v1.0/lineitemsf1.snappy.parquet'
    dst = 'lineitemsf1.snappy.parquet'
    urlretrieve(url, dst)
    return dst


download_dataset()
con = duckdb.connect()
query = con.execute("""SELECT sum(l_extendedprice * l_discount) AS revenue
                FROM
                'lineitemsf1.snappy.parquet';""")

st.write(query.fetchdf())