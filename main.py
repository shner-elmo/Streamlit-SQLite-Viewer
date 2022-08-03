import streamlit as st
import pandas as pd

import sqlite3
import time
from pathlib import Path
from uuid import uuid4


def sqlite_connect(db_bytes):
    fp = Path(str(uuid4()))
    fp.write_bytes(db_bytes.getvalue())
    conn = sqlite3.connect(str(fp))
    return conn


if 'data' not in st.session_state:
    file_path = st.file_uploader('Upload dataset:', type='.db', accept_multiple_files=False)
    while file_path is None:
        st.stop()
    else:
        st.session_state.conn = sqlite_connect(file_path)


with st.container():
    query = st.text_area('SQL Query', value='SELECT * FROM table', key='query')
    timer_start = time.perf_counter()

    if query:
        try:
            df = pd.read_sql_query(query, st.session_state.conn)
        except Exception as E:
            st.warning(E)
        else:
            ms_elapsed = int((time.perf_counter() - timer_start) * 1000)
            cols = st.columns(3)
            cols[0].text(f'Exec time: {ms_elapsed}ms')
            cols[1].text(f'Last Query: {time.strftime("%X")}')
            cols[2].text(f'Shape: {df.shape}')
            st.dataframe(df)


with st.sidebar:
    show_types = st.checkbox('Show types', value=True, help='Show data types for each column ?')
    schema_md = ''
    cursor = st.session_state.conn.cursor()

    for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        table = x[0]
        schema_md += f'\n  * {table}:'

        for row in cursor.execute(f"PRAGMA table_info('{table}')").fetchall():
            col_name = row[1]
            col_type = row[2] if show_types is True else ''
            schema_md += f'\n \t* {col_name} &ensp; {col_type}'  # &ensp; == md tab

    st.text('DataBase Schema:')
    st.markdown(schema_md)
