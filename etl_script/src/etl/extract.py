import numpy as np
import pandas as pd
from config.db_config import get_source_mysql_engine

source_engine = get_source_mysql_engine()

def extract_table(table_name, engine=source_engine):
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)

def extract_dim_account():
    account_df = extract_table("accounting_account").add_prefix("account_")
    accountType_df = extract_table("account_type").add_prefix("type_")

    joined_df = pd.merge(
        account_df,
        accountType_df,
        how="inner",
        left_on="account_acctype_id",
        right_on="type_acctype_id"
    )

    joined_df.drop(['type_acctype_id'], axis=1, inplace=True)

    joined_df.rename(columns={
        'account_acc_id': 'account_id',
        'account_acctype_id': 'type_id',
    }, inplace=True)

    return joined_df


def extract_fact_entry():
    journalTransaction_df = extract_table("journal_transaction")
    journalEntry_df = extract_table("journal_entry")

    joined_df = pd.merge(
        journalEntry_df, 
        journalTransaction_df, 
        how="inner", 
        on="trans_id"
    )

    joined_df.drop(["journal_id", "period_id", "supplier_id", "customer_id"], axis=1, inplace=True)

    joined_df.rename(columns={
        'trans_id': 'transaction_id',
        'acc_id': 'account_id',
        'trans_date': 'transaction_date',
    }, inplace=True)

    return joined_df
