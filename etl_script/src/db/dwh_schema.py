from sqlalchemy import Table, Column, ForeignKey, MetaData, String, Integer, Date, text
from config.db_config import get_target_mysql_engine

target_engine = get_target_mysql_engine()


# Define schema for database
def create_or_replace_schema(engine=target_engine):
    # metadata is a container object that holds information about tables and schema
    metadata = MetaData()
    # Define dim_account table
    dim_account = Table("dim_account", metadata,
        Column("account_id", String(4), primary_key=True),
        Column("account_code", String(3)),
        Column("account_name", String(128)),
        Column("account_normal_balance", String(32)),
        Column("account_subcategory", String(128)),
        Column("account_category", String(128)),
        Column("type_name", String(32)),
        Column("type_normal_balance", String(32)),
    )
    # Define fact_entry table
    fact_entry = Table("fact_entry", metadata,
        Column("entry_id", String(7), primary_key=True),
        Column("transaction_id", String(7)),
        Column("account_id", String(4), ForeignKey("dim_account.account_id")),
        Column("signed_amount", Integer),
        Column("transaction_date", Date),
        Column("description", String(512)),
    )
    # Create tables in the database
    metadata.drop_all(engine)  # if already exists
    metadata.create_all(engine)
    print ("Schema created successfully.")


# Create full view from both tables
def create_full_view(engine=target_engine):
    view_name = "full_view"
    create_view_statement = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT 
        fe.entry_id,
        fe.transaction_id,
        fe.account_id,
        fe.signed_amount,
        fe.transaction_date,
        fe.description,
        da.account_code,
        da.account_name,
        da.account_normal_balance,
        da.account_subcategory,
        da.account_category,
        da.type_name,
        da.type_normal_balance
    FROM fact_entry fe
    INNER JOIN dim_account da ON fe.account_id = da.account_id;
    """
    with engine.connect() as conn:
        conn.execute(text(create_view_statement))
        print(f"'{view_name}' created successfully.")
