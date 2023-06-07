import logging
import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from airflow.models import Variable
from dotenv import load_dotenv

load_dotenv()

DB_NAME = Variable.get("DATABASE_NAME")
DB_USER = Variable.get("DATABASE_USER")
DB_PASS = Variable.get("DATABASE_PASS")
DB_PORT = Variable.get("DATABASE_PORT")
DB_HOST = Variable.get("DATABASE_HOST")


def song_table():
    """
    Creates the 'Song' table in the database using SQLAlchemy engine.
    This function executes a SQL query to create the 'Song' table in the
    specified database. The table schema includes columns for song details
    such as ID, name, album, artists, danceability, and duration.
    """
    # Create a SQLAlchemy engine with the connection string
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    print(connection)
    cursor = connection.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS Song_airflow(
            id_songs  VARCHAR(255) PRIMARY KEY,
            name_song VARCHAR(255),
            album VARCHAR(255),
            album_id VARCHAR(255),
            artists VARCHAR(255),
            artist_ids VARCHAR(255),
            danceability FLOAT NOT NULL,
            duration_ms BIGINT NOT NULL
        )
    """
    # Execute the create table query using the SQLAlchemy engine
    cursor.execute(create_table_query)
    connection.commit()
    print("Table Created")


def read_csv_data():
    """
    Reads the CSV file and returns a DataFrame.

    This function reads the CSV file containing the song dataset and returns
    a pandas DataFrame with the loaded data. The CSV file is assumed to be
    located in the same directory as this script.
    :return -> dataframe
    """
    print("-----------Read CSV Data function-----------")
    # Get the absolute path of the DAGs directory
    dir = os.path.abspath(os.path.dirname(__file__))

    # Construct the absolute path to the CSV file
    csv = os.path.join(dir, "Song-dataset.csv")

    # Read the CSV file
    df = pd.read_csv(csv)
    # Return the DataFrame
    return df


def insert_data_to_sql(**context):
    """
    Inserts the data from the DataFrame into the 'Song' table in the database.

    This function takes a pandas DataFrame as input and inserts the data into
    the 'Song' table in the specified database using raw SQL queries. The DataFrame
    columns must match the table columns for a successful insertion.
    """
    df_read = context["ti"].xcom_pull(task_ids="read_csv_data")

    # Create a SQLAlchemy engine with the connection string
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    curs = conn.cursor()

    # Generate the SQL query for bulk insertion
    query = """
            INSERT INTO Song_airflow (id_songs, name_song, album, album_id, artists, artist_ids, danceability, duration_ms) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """

    try:
        # Convert DataFrame to a list of tuples
        data = df_read.to_records(index=False).tolist()

        # Execute the bulk insertion
        curs.execute(query, data, multi=True)
        print("Data inserted")
    except Exception as e:
        logging.error("Error during insertion of data: %s", str(e))

    conn.commit()
    curs.close()
    conn.close()
    logging.info("Data Populated")


default_args = {
    "owner": "airflow_dag",
    "start_date": datetime(2023, 6, 5),
    "schedule_interval": "0 0 * * *",
    "catchup": False,
}
with DAG(
    "airflow_task", default_args=default_args, schedule_interval="@daily", catchup=False
) as dag:
    """
    This is the main dag which will start the airflow tasks
    """
    t_song_table = PythonOperator(task_id="create_table", python_callable=song_table)
    t_read_csv = PythonOperator(task_id="read_csv_data", python_callable=read_csv_data)
    # t_clean_data = PythonOperator(
    #     task_id="clean_data", python_callable=data_cleaning, provide_context=True
    # )

    t_insert_data = PythonOperator(
        task_id="data_into_db", python_callable=insert_data_to_sql, provide_context=True
    )
    # Set task dependencies e.g 1 -> 2 -> 3 task
    t_song_table >> t_read_csv >> t_insert_data


# DB_NAME = os.getenv("DATABASE_NAME")
# DB_USER = os.getenv("DATABASE_USER")
# DB_PASS = os.getenv("DATABASE_PASS")
# DB_PORT = os.getenv("DATABASE_PORT")
# DB_HOST = "cloudsql"

# db_connection = (
#     f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@cloudsql:{DB_PORT}/{DB_NAME}"
# )
# def insert_data_to_sql(**context):
#     """
#     Inserts the data from the DataFrame into the 'Song' table in the database.

#     This function takes a pandas DataFrame as input and inserts the data into
#     the 'Song' table in the specified database using raw SQL queries. The DataFrame
#     columns must match the table columns for a successful insertion.
#     """
#     df_read = context["ti"].xcom_pull(task_ids="read_csv_data")

#     # Create a SQLAlchemy engine with the connection string
#     conn = psycopg2.connect(
#         host=DB_HOST,
#         port=DB_PORT,
#         database=DB_NAME,
#         user=DB_USER,
#         password=DB_PASS,
#     )
#     curs = conn.cursor()
#     for index, row in df_read.iterrows():
#         try:
#             # Generate the SQL query for bulk insertion
#             print("data is inserted")
#             curs.execute(
#                 """
#                 INSERT INTO Song_airflow (id_songs, name_song, album, album_id, artists, artist_ids, danceability, duration_ms)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
#                 (
#                     row["id_songs"],
#                     row["name_song"],
#                     row["album"],
#                     row["album_id"],
#                     row["artists"],
#                     row["artist_ids"],
#                     row["danceability"],
#                     row["duration_ms"],
#                 ),
#             )

#         except Exception as e:
#             logging.error("Error during insertion of data: %s", str(e))

#     conn.commit()
#     curs.close()
#     conn.close()
#     logging.info("Data Populated")


# def insert_data_to_sql(**context):
#     """
#     Inserts the data from the DataFrame into the 'Song' table in the database.

#     This function takes a pandas DataFrame as input and inserts the data into
#     the 'Song' table in the specified database using raw SQL queries. The DataFrame
#     columns must match the table columns for a successful insertion.
#     """
#     df_read = context["ti"].xcom_pull(task_ids="read_csv_data")

#     # Create a SQLAlchemy engine with the connection string
#     # conn = psycopg2.connect(
#     #     host=DB_HOST,
#     #     port=DB_PORT,
#     #     database=DB_NAME,
#     #     user=DB_USER,
#     #     password=DB_PASS,
#     # )
#     # curs = conn.cursor()

#     # # Generate the SQL query for bulk insertion
#     # query = """
#     #         INSERT INTO Song_airflow (id_songs, name_song, album, album_id, artists, artist_ids, danceability, duration_ms)
#     #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
#     #         """

#     # Convert DataFrame to a list of tuples for bulk insertion
#     # records = df_read.to_records(index=False)
#     # data = list(records)
#     db_connection = (
#         f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
#     )
#     engine = create_engine(db_connection)
#     # Insert the data into the "Song" table
#     with engine.connect() as connection:
#         df_read.to_sql(
#             "Song_airflow", connection, if_exists="append", index=False, method="multi"
#         )
#     # Close the database connection
#     engine.dispose()
#     logging.info("Data Populated")
