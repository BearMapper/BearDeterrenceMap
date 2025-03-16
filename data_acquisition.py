#the movementbank api is described here: https://github.com/movebank/movebank-api-doc/blob/master/movebank-api.md
#it is used to automatically download and 
import psycopg2
import polars as pl
#text is needed to safely execute SQL queries with newer versions of SQLAlchemy
from sqlalchemy import create_engine, text

class DatabaseManager:
    def __init__(self, host, port, database, user, password):
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string)

    #for general SQL queries
    def execute_query(self, query):
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            return result.fetchall()
        
    #for getting query results as pandas DataFrames
    def load_dataframe(self, query):
        return pl.read_database(query, self.engine)
    
    #for writing DataFrame data to the database
    def save_dataframe(self, df, table_name, if_exists='replace'):
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)

# local Postgres, should eventually be packed together with the python app into a single docker container
db = DatabaseManager(
    host="localhost",
    port="5432",
    database="postgres",
    user="postgres",
    password="mysecretgeopassword"
)

# Get jaguar data
def get_jaguar_data():
    jaguar_data = db.load_dataframe("SELECT * FROM public.jaguar_rescue")
    return jaguar_data

