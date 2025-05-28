import numpy as np
import pandas as pd

# --- DIM_ACCOUNT TRANSFORMATIONS ---

def _change_dtype_da(dim_account):    
    object_cols = ['account_id', 'account_code', 'type_id']
    for col in object_cols:
        dim_account[col] = dim_account[col].astype(str).str.strip()
    return dim_account

def _remove_accounts_da(dim_account):
    codes_to_remove = ['621', '622', '627', '911']
    dim_account = dim_account[~dim_account['account_code'].isin(codes_to_remove)]
    return dim_account

def _remove_cols_da(dim_account):
    cols_to_remove = ['type_id']
    dim_account.drop(cols_to_remove, axis=1, inplace=True)
    return dim_account

def transform_dim_account(dim_account):
    dim_account = dim_account.copy()
    dim_account = _change_dtype_da(dim_account)
    dim_account = _remove_accounts_da(dim_account)
    dim_account = _remove_cols_da(dim_account)
    return dim_account


# --- FACT_ENTRY TRANSFORMATIONS ---

def _change_dtype_fe(fact_entry):
    object_cols = ['entry_id', 'transaction_id', 'account_id']
    for col in object_cols:
        fact_entry[col] = fact_entry[col].astype(str).str.strip()
    date_cols = ['transaction_date']
    for col in date_cols:
        fact_entry[col] = pd.to_datetime(fact_entry[col]).dt.date
    return fact_entry

def _fillna_amount_fe(fact_entry):
    cols_to_fillna = ['debit_amount', 'credit_amount']
    fact_entry.loc[:, cols_to_fillna] = fact_entry[cols_to_fillna].fillna(0)
    return fact_entry

def _join_dim_account(fact_entry, dim_account):
    cols_to_merge = ['account_id', 'account_code', 'type_name']
    joined_df = fact_entry.merge(dim_account[cols_to_merge], on='account_id', how='inner')
    return joined_df

def _remove_accounts_fe(fact_entry):
    codes_to_remove = ['621', '622', '627', '911']
    fact_entry = fact_entry[~fact_entry['account_code'].isin(codes_to_remove)]
    return fact_entry

def _add_is_closing_entry(fact_entry):
    condition_1 = (
        (fact_entry['type_name'] == 'Revenue') & 
        (fact_entry['account_code'] != '521') & 
        (fact_entry['debit_amount'] > 0) & 
        (fact_entry['credit_amount'] == 0)
    )
    condition_2 = (
        (fact_entry['type_name'] == 'Revenue') & 
        (fact_entry['account_code'] == '521') & 
        (fact_entry['debit_amount'] == 0) & 
        (fact_entry['credit_amount'] > 0)
    )
    condition_3 = (
        (fact_entry['type_name'] == 'Expenses') & 
        (fact_entry['debit_amount'] == 0) & 
        (fact_entry['credit_amount'] > 0)
    )
    fact_entry['is_closing_entry'] = (condition_1 | condition_2 | condition_3).astype(int)
    return fact_entry

def _remove_closing_entries(fact_entry):
    fact_entry = fact_entry[fact_entry['is_closing_entry'] == 0]
    return fact_entry

def _add_sign(fact_entry):
    condition_1 = (fact_entry['type_name'] == 'Expenses')
    condition_2 = (fact_entry['type_name'] == 'Revenue') & (fact_entry['account_code'] == '521')
    condition_3 = (fact_entry['type_name'] == 'Assets') & (fact_entry['debit_amount'] == 0) & (fact_entry['credit_amount'] > 0)
    condition_4 = (fact_entry['type_name'].isin(['Liabilities', 'Equity'])) & (fact_entry['debit_amount'] > 0) & (fact_entry['credit_amount'] == 0)
    
    fact_entry['sign'] = 1
    fact_entry.loc[(condition_1 | condition_2 | condition_3 | condition_4), 'sign'] = -1
    return fact_entry

def _add_signed_amount(fact_entry):
    fact_entry['signed_amount'] = (fact_entry['debit_amount'] + fact_entry['credit_amount']) * fact_entry['sign']
    fact_entry['signed_amount'] = fact_entry['signed_amount'].astype(np.int64)
    return fact_entry

def _remove_cols_fe(fact_entry):
    cols_to_drop = ['debit_amount', 'credit_amount', 'account_code', 'type_name', 'is_closing_entry', 'sign']
    fact_entry.drop(cols_to_drop, axis=1, inplace=True)
    return fact_entry

def transform_fact_entry(fact_entry, transformed_dim_account):
    fact_entry = fact_entry.copy()
    dim_account = transformed_dim_account.copy()
    
    fact_entry = _change_dtype_fe(fact_entry)
    fact_entry = _fillna_amount_fe(fact_entry)
    fact_entry = _join_dim_account(fact_entry, dim_account)
    fact_entry = _remove_accounts_fe(fact_entry)  # redundant because join with transformed_dim_account already excluded accounts
    fact_entry = _add_is_closing_entry(fact_entry)
    fact_entry = _remove_closing_entries(fact_entry)
    fact_entry = _add_sign(fact_entry)
    fact_entry = _add_signed_amount(fact_entry)
    fact_entry = _remove_cols_fe(fact_entry)
    
    return fact_entry
