# Snowpark libs
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T
from snowflake.snowpark import version
import pandas as pd

#Snowflake connection info
from config import snowflake_conn_prop

session = Session.builder.configs(snowflake_conn_prop).create()

snowdf_pandas = pd.read_csv('Spotify Million Song Dataset_exported.csv', index_col=False) #Podemos elegir entre los dos csv que tenemos uno tiene mas data que el otro
results = session.write_pandas(snowdf_pandas,"Music", quote_identifiers=False, auto_create_table=True, overwrite=True)