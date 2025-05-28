import numpy as np
import pandas as pd
from config.db_config import get_target_mysql_engine

target_engine = get_target_mysql_engine()


def load_table(df, table_name, engine=target_engine):
    print(f"Loading '{table_name}' into database...")
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
    )
    print(f"'{table_name}' loaded successfully. Rows inserted: {len(df)}")

def load_dim_account(df):
    load_table(df, "dim_account")

def load_fact_entry(df):
    load_table(df, "fact_entry")
