import os
from datetime import datetime
from time import time
from urllib.request import urlretrieve

import altair as alt
import duckdb
import streamlit as st


@st.cache
def download_dataset():
    url = "https://github.com/cwida/duckdb-data/releases/download/v1.0/lineitemsf1.snappy.parquet"
    dst = "lineitemsf1.snappy.parquet"
    if not os.path.exists(dst):
        urlretrieve(url, dst)
    return dst


def run_timed_query(title, desc, query, connection):
    st.header(title)
    st.write(desc)
    st.code(query)
    start = time()
    results = connection.execute(query).fetchdf()
    end = time()
    st.write(f"Query took {end - start} seconds")
    st.write(results)


st.title("DuckDB Demo")

st.write(
    """
This is a simple demo showing off a few features of DuckDB (https://duckdb.org).

If the application is booting cold, it needs a few more seconds to download the dataset. 
After that, all queries are run directly on the parquet file downloaded.

The queries are taken from the following blogpost: https://duckdb.org/2021/12/03/duck-arrow.html
"""
)

# First we'll download the dataset
download_dataset()

connection = duckdb.connect()

run_timed_query(
    "Basic count",
    "This is just a count of the rows to show the size of the data",
    """SELECT count(*) FROM 'lineitemsf1.snappy.parquet';""",
    connection,
)

run_timed_query(
    "Simple aggregation",
    "This query uses projection pushdown",
    """SELECT sum(l_extendedprice * l_discount) AS revenue
    FROM 'lineitemsf1.snappy.parquet';""",
    connection,
)

run_timed_query(
    "Filter pushdown",
    "This query reads very little from the file as it filters on more columns",
    """SELECT sum(l_extendedprice * l_discount) AS revenue
    FROM
        'lineitemsf1.snappy.parquet'
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
        AND l_discount BETWEEN 0.05
        AND 0.07
        AND l_quantity < 24; """,
    connection,
)

st.header("Shipped totals")
st.write("A small example showing interactive filtering")

start_date, end_date = st.slider(
    "Date range for analysis",
    value=(datetime(1992, 1, 1, 9, 30), datetime(1999, 1, 1, 9, 30)),
    format="DD/MM/YY",
)

results = connection.execute(
    """SELECT l_shipdate, sum(l_extendedprice * l_discount) AS revenue
    FROM
        'lineitemsf1.snappy.parquet'
    WHERE
        l_shipdate >= CAST(? AS date)
        AND l_shipdate < CAST(? AS date)
    group by l_shipdate
    order by l_shipdate asc
    """,
    [start_date, end_date],
).fetchdf()

c = (
    alt.Chart(results)
    .mark_line()
    .encode(x=alt.X("l_shipdate:T", timeUnit="yearmonthdate"), y="revenue")
)

st.altair_chart(c, use_container_width=True)
