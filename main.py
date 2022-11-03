import streamlit as st
import pandas as pd

import sqlite3
import time
from pathlib import Path
from uuid import uuid4


def sqlite_connect(db_bytes):
    """
    Load Sqlite file

    :param db_bytes: file obj
    :return: sqlite connection
    """
    fp = Path(str(uuid4()))
    fp.write_bytes(db_bytes.getvalue())
    con = sqlite3.connect(str(fp))
    return con


def rename_duplicate_cols(data_frame):
    """
    for each duplicated column it will add a suffix with a number (col, col_2, col_3... )

    :param data_frame: DataFrame
    :return: None
    """
    new_cols = []
    prev_cols = []  # previously iterated columns in for loop

    for col in data_frame.columns:
        prev_cols.append(col)
        count = prev_cols.count(col)

        if count > 1:
            new_cols.append(f'{col}_{count}')
        else:
            new_cols.append(col)
    data_frame.columns = new_cols


@st.experimental_singleton
def users_dict() -> dict:
    """
    # Function that returns a list, the decorator will save the output
    # of the function, and therefore it will just mutate the output (list)
    # which is saved in cache.
    # (all this because we cannot store a dictionary directly in cache)
    """
    return {'users': []}


user_id = st.experimental_user.get('name') or st.experimental_user.get('email')
users = users_dict()
if user_id not in users:
    users[user_id] = []  # each user has a list of historical queries
    users['users'].append({**st.experimental_user})

queries = users[user_id]
tab1, tab2 = st.tabs(['Execute SQL', 'Query History'])

with tab2:
    # query history
    st.write(f'Total Queries: {len(queries)}')
    for dct in reversed(queries):  #
        st.markdown('---')
        cols = st.columns(3)
        cols[0].text(dct['time'])
        cols[1].text(f'Exec time: {dct["exec_time_ms"]}ms')
        cols[2].text(f'Shape: {dct["shape"]}')
        st.markdown(f'```sql \n{dct["query"]} \n```')

with tab1:
    # upload file and download sample
    upload_file = st.file_uploader('Upload dataset:', type=['.sql', '.db', '.sqlite', '.sqlite3', '.db3'],
                                   accept_multiple_files=False)
    while upload_file is None:
        with open("parch-and-posey.db", "rb") as file:
            st.download_button(label="Download sample dataset", data=file, file_name=file.name)
        st.stop()
    else:
        extension = upload_file.name.split('.')[-1]
        if extension == 'sql':
            conn = sqlite3.connect(':memory:')
            conn.executescript(upload_file.getvalue().decode('utf-8'))
            st.session_state.conn = conn
        else:
            st.session_state.conn = sqlite_connect(upload_file)

    # table and metrics
    with st.container():
        query = st.text_area(
            label='SQL Query',
            value='SELECT * FROM table',
            height=160,
            key='query',
            help='All queries are executed by the SQLite3 engine. Drag the bottom right corner to expand the window'
        )
        timer_start = time.perf_counter()

        if query:
            try:
                df = pd.read_sql_query(query, st.session_state.conn)
            except Exception as E:
                st.warning(E)
            else:
                # display dataframe and stats
                ms_elapsed = int((time.perf_counter() - timer_start) * 1000)
                cols = st.columns(3)
                cols[0].text(f'Exec time: {ms_elapsed}ms')
                cols[1].text(f'Last Query: {time.strftime("%X")}')
                cols[2].text(f'Shape: {df.shape}')

                if df.columns.has_duplicates:
                    rename_duplicate_cols(df)
                st.dataframe(df)

                # save query and stats for query-history tab
                queries.append(
                    {'time': time.strftime("%X"), 'query': query, 'exec_time_ms': ms_elapsed, 'shape': df.shape})

# sidebar/ schema
with st.sidebar:
    show_types = st.checkbox('Show types', value=True, help='Show data types for each column ?')
    schema = ''
    with st.session_state.conn as cursor:

        for x in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            table = x[0]
            schema += f'\n\n * {table}:'

            for row in cursor.execute(f"PRAGMA table_info('{table}')"):
                col_name = row[1]
                col_type = row[2].upper() if show_types is True else ''
                schema += f'\n     - {col_name:<15} {col_type}'

    st.text('DataBase Schema:')
    st.text(schema)

    st.markdown('---')
    cols = st.columns(4)
    cols[0].markdown('Shneor E.')
    cols[3].markdown('[Source](https://bit.ly/3zZwpim)')
