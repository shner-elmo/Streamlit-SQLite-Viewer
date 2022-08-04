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


upload_file = st.file_uploader('Upload dataset:', type=['.db', '.sqlite', '.sqlite3', '.db3'],
                               accept_multiple_files=False)

while upload_file is None:
    with open("parch-and-posey.db", "rb") as file:
        st.download_button(label="Download sample dataset", data=file, file_name=file.name)
    st.stop()
else:
    st.session_state.conn = sqlite_connect(upload_file)


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

            df_cols = df.columns.value_counts()
            duplicated_cols = df_cols[df_cols > 1].index.to_list()

            if len(duplicated_cols) > 0:
                st.warning(f'Cannot display a table with multiple same name columns ({duplicated_cols})')
            else:
                st.dataframe(df)


with st.sidebar:
    show_types = st.checkbox('Show types', value=True, help='Show data types for each column ?')
    schema = ''
    cursor = st.session_state.conn  # .cursor()

    for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        table = x[0]
        schema += f'\n\n * {table}:'

        for row in cursor.execute(f"PRAGMA table_info('{table}')"):
            col_name = row[1]
            col_type = row[2].upper() if show_types is True else ''
            schema += f'\n     - {col_name:<15} {col_type}'

    st.text('DataBase Schema:')
    st.text(schema)
