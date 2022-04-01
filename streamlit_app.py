import streamlit as st
import duckdb
import pyarrow
import pyarrow.dataset as ds


@st.cache(hash_funcs={pyarrow.lib.Buffer: lambda _: None})
def get_dataset():
    return ds.dataset('s3://ursa-labs-taxi-data/', partitioning=["year", "month"])

nyc_dataset = get_dataset()
con = duckdb.connect()
query = con.execute("SELECT * FROM nyc_dataset limit 10")

st.write(query.fetchdf())