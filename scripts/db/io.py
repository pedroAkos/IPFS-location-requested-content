import pandas as pd
from sqlalchemy import create_engine

conn = None


def open_db(host, port):
    """ Opens a connection to the database.
    :param host: Hostname of the database
    :param port: Port of the database
    """
    global conn
    if conn is None:
        engine = create_engine(f'postgresql+psycopg2://postgres:@{host}:{port}/ipfs_content_location')
        conn = engine.connect()
    return conn


def close_db():
    """ Closes the connection to the database. """
    global conn
    if conn is not None:
        conn.close()


def execute_query(query):
    """ Executes a query on the database.
    :param query: The query to execute.
    :return: A pandas dataframe with the results.
    """
    global conn
    if conn is None:
        raise Exception('Database is not connected!')

    return pd.read_sql(query, conn)
